"""Processing-status dashboard, deployed as a Vercel Python (Flask/WSGI)
serverless function. Read-only: shows how extraction is going -- which
models have run, how many pages each has attempted/succeeded/failed for
both the structured catalogue-entry extraction and the independent
full-page OCR pass (see scripts/extract_with_llm.py and
supabase/migrations/20260912160000_add_page_ocr_text.sql).

Talks to Supabase via SUPABASE_DB_URL, same env var name as the pipeline
scripts for consistency -- but this MUST be set in Vercel to the
Transaction-mode pooler connection string (port 6543 in Supabase's
connection-string picker), not the Session-mode one extract_with_llm.py
uses. A serverless function can have many concurrent invocations each
opening a connection; session-mode's small fixed client-slot count is
exactly what caused the EMAXCONNSESSION contention this project already
hit once with the extraction workers alone (see PERFORMANCE_NOTES.md /
git history) -- pointing this dashboard at the same session-mode URL would
reintroduce that same contention every time someone loads the page while
workers are running.

No RLS is configured on these tables yet, so this deliberately never
accepts a database credential from the browser and never queries Supabase
via its REST/anon-key path -- only this server-side code, with the
Transaction-pooler URL, ever touches the database.

Access is gated by a single shared password (see login()/login_required
below) rather than Vercel Deployment Protection, so it can't rely on
Vercel-account login. Two env vars are required for this:

  DASHBOARD_PASSWORD  -- the string a visitor must type in at /login.
                         Never sent back to the browser in any form; only
                         compared against, server-side, in constant time.
  SESSION_SECRET_KEY  -- an unrelated random secret Flask uses to sign the
                         session cookie (via itsdangerous). The cookie it
                         produces contains only the plaintext claim
                         {"authenticated": true} plus a signature; the
                         signature proves the claim wasn't forged without
                         revealing this key (HMAC is one-way), and the key
                         itself never appears in the cookie or in any
                         client-side code. Generate it once with, e.g.,
                         `python -c "import secrets; print(secrets.token_hex(32))"`
                         -- it must stay the same across deployments/cold
                         starts for logins to persist, so it has to come
                         from an env var rather than being generated at
                         import time.
"""
import functools
import os
import re
import sys
from datetime import timedelta

# Vercel's Python runtime imports this file directly by path (see
# vc_init.py in its traceback), which does not add this file's own
# directory to sys.path the way running `python index.py` normally would
# -- confirmed in production logs as `ModuleNotFoundError: No module named
# 'queries'` even though the exact same import works fine when run
# locally (where sys.path already includes the script's directory).
# Adding it explicitly makes the sibling import work regardless of how
# the entry file is loaded.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import hmac
import json
import math

import psycopg2
from flask import Flask, abort, redirect, render_template, request, session, url_for
from markupsafe import Markup

from b2 import load_b2_accounts, presigned_image_url
from queries import (
    CATALOGUE_ENTRY_BOOL_FIELDS,
    CATALOGUE_ENTRY_FIELDS,
    CATALOGUE_ENTRY_INT_FIELDS,
    CATALOGUE_ENTRY_JSON_FIELDS,
    apply_page_exclusion,
    apply_qc_verdict,
    fetch_corpus_stats,
    fetch_dashboard_data,
    fetch_qc_page,
    fetch_qc_position,
    fetch_table_page,
    list_columns,
    list_tables,
    save_human_edit,
)

# Short, human-authored description of each model's role, shown next to its
# tag on the Progress page. Purely cosmetic labelling -- keyed by model_tag
# (see scripts/extract_with_llm.py's slugifying of OLLAMA_MODEL) with a
# blank fallback below for any model_tag not yet listed here, so a newly
# introduced model still renders instead of raising a KeyError.
MODEL_ROLE_LABELS = {
    "glm-ocr": "purpose-built document OCR · default",
    "minicpm-v4.6": "general vision, 8B",
    "minicpm-v4.5": "general vision, 8B",
    "qwen3-vl-4b": "general vision, 4B",
    "qwen3-vl-2b": "general vision, 2B · smallest",
}

# pipeline/schema.md names this flag field "char", but the catalogue_entries
# column -- and CATALOGUE_ENTRY_FIELDS's own name for it -- is
# "char_qualifier" (see supabase/migrations). Without this alias, a
# {"field": "char"} flag from the extractor never matches any real field
# name, so it's silently dropped from both the QC status row's flagged-field
# count and the correction form's per-field highlighting. Keyed by the
# canonical CATALOGUE_ENTRY_FIELDS name -> the raw schema.md name, since
# that's the direction qc.html's per-field highlighting needs; FLAG_FIELD_ALIAS_TO_CANONICAL
# below is the same mapping in the other direction, for normalizing a raw
# flag's field name.
FLAG_FIELD_ALIASES = {"char_qualifier": "char"}
FLAG_FIELD_ALIAS_TO_CANONICAL = {alias: canonical for canonical, alias in FLAG_FIELD_ALIASES.items()}

# Labels for the Overview page's stats card, in display order -- each keyed
# to the matching total from fetch_corpus_stats() so overview() only has to
# zip values onto them, not repeat the label text at the call site.
CORPUS_STAT_LABELS = [
    ("total_entries", "catalogue entries"),
    ("total_copies", "registered copies"),
    ("total_printers", "printers"),
    ("total_publishers", "publishers"),
    ("total_quarters", "quarterly catalogues"),
    ("total_source_pdfs", "source PDF files"),
]

# Every source/method citation in the public site (Overview's pipeline
# stages, Method's per-section refs, Sources' working-documents list) links
# straight to this repo on GitHub rather than sitting as inert path text --
# a reader curious about "how, exactly" shouldn't have to go find the repo
# and navigate to the file themselves. Pinned to main (not a commit SHA):
# these are living documents a reader should see the current state of, not
# a snapshot frozen at whatever commit happened to be deployed.
GITHUB_REPO = "CreativeMaladjustment/Punjab-Data-Project"

# Every path passed to _repo_link_html() below is a hardcoded literal from
# the lists in this file, never anything request-derived -- but this still
# validates it against a strict allowlist before it's allowed anywhere near
# an href, rather than trusting "it's a literal today" to stay true forever.
_SAFE_REPO_PATH_RE = re.compile(r"^[A-Za-z0-9._/-]+$")


def _repo_link_html(path, text=None, kind="blob"):
    """Safe, pre-rendered HTML linking to `path` in this repo on main --
    plain escaped text instead of a link if path is None (a citation that
    isn't a repo path at all: a SQL view name, a function name). kind=
    "tree" for a directory, "blob" (default) for a file.

    Returns a markupsafe.Markup, built entirely server-side, rather than
    the more obvious `<a href="{{ url }}">` in the template with `url`
    computed here and handed over as a plain string: a generic template
    scanner flags any raw variable inside href="..." on sight (it can't
    see that these values only ever come from the hardcoded literals
    below), and Flask's own usual answer to that -- url_for() -- only
    builds links within this app, not to an external site like this one.
    Building the whole anchor tag here, through Markup.format() (which
    HTML-escapes every value it substitutes, exactly like Jinja's own
    autoescaping would), sidesteps that ambiguity instead of arguing with
    a purely syntactic check -- and the path validation below means this
    is actually safe, not just quiet about it.
    """
    label = text if text is not None else path
    if path is None:
        return Markup("{}").format(label)
    if not _SAFE_REPO_PATH_RE.match(path):
        raise ValueError(f"unsafe repo path for a GitHub link: {path!r}")
    url = f"https://github.com/{GITHUB_REPO}/{kind}/main/{path}"
    return Markup('<a href="{}" target="_blank" rel="noopener">{}</a>').format(url, label)


PIPELINE_STAGES = [
    {
        "num": "01", "title": "Source volumes",
        "body": "Bound India Office PDFs — roughly 25 GB — held on pCloud, never committed to the repository.",
        "file": "scripts/process_pcloud.py",
    },
    {
        "num": "02", "title": "Page render",
        "body": "Each page rendered at 200 DPI as a WebP for vision input and uploaded to Backblaze B2.",
        "file": ".github/workflows/process-pdfs.yml",
    },
    {
        "num": "03", "title": "Vision extraction",
        "body": "A local vision model on the runner transcribes catalogue entries into the per-entry schema.",
        "file": "scripts/extract_with_llm.py",
    },
    {
        "num": "04", "title": "Quality control",
        "body": "A person compares the extraction against the scan, approves it, or sends it back to the queue.",
        "file": "api/templates/qc.html",
    },
    {
        "num": "05", "title": "Normalisation",
        "body": "Aliases folded, sequences validated, uncertain readings pushed into an adjudication queue.",
        "file": "pipeline/postprocess.py",
    },
]
for _stage in PIPELINE_STAGES:
    _stage["link"] = _repo_link_html(_stage["file"])

# Each ref is a list of pre-rendered Markup fragments (see
# _repo_link_html()) -- a real repo path renders as a link, a citation
# that isn't one at all (a SQL view name, a function name) as plain escaped
# text. "supabase/migrations" is a directory, not a file, hence kind="tree"
# -- every other ref here is a real file, linked as a blob.
METHOD_SECTIONS = [
    {
        "heading": "Verbatim first",
        "body": "The extractor transcribes what is printed. It does not correct, complete, or infer beyond a stated set of rules. Misprints stay, the annotator's editorialising stays, and a reading the model is unsure of is flagged rather than smoothed over.",
        "body2": "A separate normalised layer resolves Ditto, folds printer and publisher aliases, and types the numeric fields. The verbatim layer is never rewritten by it.",
        "ref": [
            _repo_link_html("pipeline/schema.md"),
            _repo_link_html("OCR_RESEARCH_AGENDA.md"),
        ],
    },
    {
        "heading": "Provenance on every entry",
        "body": "Each entry records the page number printed on the page and the PDF page index it was read from, so any row can be traced back to the pixels it came from.",
        "body2": "A database view joins each entry all the way back to its B2 image key and its original pCloud file, in one query.",
        "ref": [
            _repo_link_html("supabase/migrations", kind="tree"),
            _repo_link_html(None, text="catalogue_entries_full"),
        ],
    },
    {
        "heading": "Native-script titles",
        "body": "Where the register prints a vernacular title alongside a printed romanization, the localization workstream finds the native-script title within the entry and pairs it with its romanization.",
        "body2": "Legibility of the native script varies sharply across the volumes. A 21-page re-imaging pilot tests whether buying better scans is worth it.",
        "ref": [
            _repo_link_html("pipeline/localize.py"),
            _repo_link_html("analysis/ocr_lab/REIMAGING_PILOT.md"),
        ],
    },
    {
        "heading": "The model is not the record",
        "body": "A human correction is stored under its own reserved tag rather than edited into a model's output, so a person's judgement is always distinguishable from what a model actually produced.",
        "body2": "Several models run against the same backlog and their output is namespaced separately, so they can be compared page by page rather than merged.",
        "ref": [
            _repo_link_html("api/queries.py"),
            _repo_link_html(None, text="save_human_edit()"),
        ],
    },
]

VALIDATION_CHECKS = [
    {"name": "Registration sequence", "body": "One annual run of registration numbers. A gap or a repeat marks a page worth re-reading."},
    {"name": "Serial chaining", "body": "Serial numbers chain across quarters within each language–topic section."},
    {"name": "Integrity sweep", "body": "The stored record is swept against the extractor's own flags; it holds with five identified exceptions."},
]

SOURCE_LICENCES = [
    {"kind": "Code", "licence": "GPL-3.0-or-later", "why": "Pipeline, analysis and site-build scripts."},
    {"kind": "Data", "licence": "CC0", "why": "A transcription of a public-domain government record, and mostly not ours to license."},
    {"kind": "Prose", "licence": "CC BY 4.0", "why": "Method notes, decision log, and the dialectic documents."},
]

SOURCE_DOCS = [
    {"path": "README.md", "what": "Project overview and the live explorer link."},
    {"path": "PLAN.md", "what": "Governing document for project direction and scope."},
    {"path": "OCR_RESEARCH_AGENDA.md", "what": "Governing document for transcription and extraction."},
    {"path": "DECISIONS.md", "what": "Numbered decision log governing every normalisation fold and method choice."},
    {"path": "ARCHITECTURE.md", "what": "Infrastructure/engineering decision record for the CI/CD-as-compute pipeline itself."},
    {"path": "analysis/integrity/INTEGRITY_SWEEP.md", "what": "Does the stored record match its own specification?"},
    {"path": "analysis/ocr_lab/E0B_RESULTS.md", "what": "Legibility measurements across the volumes, by language and script."},
    {"path": "analysis/ocr_lab/REIMAGING_PILOT.md", "what": "The 21-page experiment deciding whether to buy re-imaged volumes."},
    {"path": "dialectic/dead_ends.md", "what": "What was tried and abandoned. Read this one first."},
    {"path": "PERFORMANCE_NOTES.md", "what": "Point-in-time extraction throughput measurements, by runner type."},
    {"path": "NEXT_STEPS.md", "what": "Personal to-do list for adding more free-tier inference capacity."},
    {"path": "LICENSING.md", "what": "Authoritative statement of which licence covers code, data, and prose."},
]
for _doc in SOURCE_DOCS:
    _doc["link"] = _repo_link_html(_doc["path"])

TABLE_PAGE_SIZES = (20, 50, 100)
MAX_TABLE_PAGE = 1_000_000  # request.args["page"] is only ever clamped to
# >= 1 otherwise; an absurd value (or one crafted to overflow) would still
# reach fetch_table_page() and drive a huge OFFSET -- at best a wasted
# full-table scan for a request that can only return zero rows, at worst
# an overflow. A million pages is already far beyond anything this tool
# would ever legitimately need to page through.
EXTRA_BLANK_EDIT_ROWS = 3  # empty rows offered in the QC edit form for adding
# entries the model missed entirely, on top of however many already exist.
MAX_EDIT_ROWS = 500  # total_rows arrives as a hidden form field a caller
# fully controls; without a cap, a huge submitted value would drive an
# unbounded loop in qc_save_edit() and could tie up a serverless
# invocation until it times out. Far more than any real page's worth of
# catalogue entries plus blank rows.
POSTGRES_INT4_MIN = -2_147_483_648
POSTGRES_INT4_MAX = 2_147_483_647


def _reject_json_constant(constant):
    raise ValueError(f"non-standard JSON constant {constant!r} is not allowed")


def _reject_non_finite_numbers(value):
    # parse_constant (see _reject_json_constant above) only catches the
    # literal tokens NaN/Infinity/-Infinity appearing in the JSON text --
    # it does nothing for an ordinary-looking number that merely overflows
    # float range, like 1e400. Python's json module parses that to
    # float('inf') without complaint, and psycopg2.extras.Json would then
    # serialize it as the bare (invalid-JSON) token `Infinity`, which
    # Postgres's jsonb column rejects at write time -- the same "500 well
    # after this 400 should have caught it" problem parse_constant alone
    # doesn't fully close. Walks the parsed structure recursively since
    # the offending number could be nested inside a list/object.
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"non-finite number {value!r} is not allowed")
    if isinstance(value, dict):
        for v in value.values():
            _reject_non_finite_numbers(v)
    elif isinstance(value, list):
        for v in value:
            _reject_non_finite_numbers(v)


# template_folder is given as an absolute path rather than left to Flask's
# default __name__-based resolution: that default depends on this module
# being registered in sys.modules under a normal dotted name, which
# Vercel's importlib-by-path loading (see the sys.path comment above)
# doesn't guarantee -- confirmed locally by reproducing that exact load
# path, which raised jinja2.exceptions.TemplateNotFound even after fixing
# the `queries` import.
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"),
)

# app.secret_key is what Flask/itsdangerous signs the session cookie with.
# Read (not generated) so it's stable across cold starts -- a randomly
# generated key would invalidate every logged-in session the moment
# Vercel spins up a fresh instance. Left None if unset rather than raising
# here: Flask only raises (its own clear "no secret key was set" error)
# once something actually tries to open/save a session, which keeps
# /healthz working even if this is misconfigured.
app.secret_key = os.environ.get("SESSION_SECRET_KEY")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,  # Vercel is HTTPS-only; never send over plain HTTP
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
)

DB_CONNECT_MAX_ATTEMPTS = 3  # short retry, not the pipeline's 5 -- a
# dashboard request should fail fast and let the user reload, not hold a
# serverless invocation open for a long backoff.


def _dashboard_password():
    try:
        return os.environ["DASHBOARD_PASSWORD"]
    except KeyError:
        raise RuntimeError(
            "DASHBOARD_PASSWORD is not set for this deployment -- add it "
            "in Vercel's project environment variables."
        ) from None


def login_required(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


@app.route("/login", methods=["GET", "POST"])
def login():
    # Deliberately always redirects to a hardcoded endpoint on success
    # rather than honoring a caller-supplied "return to this page"
    # parameter: passing any request-controlled value to redirect() is an
    # open redirect (an attacker's /login?next=https://evil.example link
    # would send a visitor on to it right after they type their real
    # password in), and there's currently only one page behind
    # login_required anyway, so there's nothing real to return to.
    error = None
    if request.method == "POST":
        submitted = request.form.get("password", "")
        # compare_digest for constant-time comparison -- a plain `==`
        # leaks how many leading characters matched via response timing.
        if submitted and hmac.compare_digest(submitted, _dashboard_password()):
            session.clear()
            session["authenticated"] = True
            session.permanent = True
            return redirect(url_for("progress"))
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


def db_connect():
    # Read at call time, not at module import: Vercel's Python builder
    # imports this file to find the WSGI `app` object, and raising here
    # (as a bare os.environ[...] at module level would, if the env var
    # isn't set for this deployment's environment -- e.g. added for
    # Production but not Preview) would abort the whole build with an
    # opaque "Error" status instead of a clear message on the one request
    # that actually needs it.
    try:
        db_url = os.environ["SUPABASE_DB_URL"]
    except KeyError:
        raise RuntimeError(
            "SUPABASE_DB_URL is not set for this deployment -- add it in "
            "Vercel's project environment variables (Supabase's "
            "Transaction-mode pooler connection string, port 6543), making "
            "sure it's enabled for this deployment's environment "
            "(Production/Preview/Development)."
        ) from None

    import time

    last_exc = None
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(db_url)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if attempt < DB_CONNECT_MAX_ATTEMPTS:
                time.sleep(1)
    raise last_exc


@app.route("/")
def overview():
    conn = db_connect()
    try:
        stats = fetch_corpus_stats(conn)
    finally:
        conn.close()
    corpus_stats = [{"value": "{:,}".format(stats[key]), "label": label} for key, label in CORPUS_STAT_LABELS]

    return render_template(
        "overview.html",
        active="overview",
        corpus_stats=corpus_stats,
        stages=PIPELINE_STAGES,
    )


@app.route("/method")
def method():
    return render_template(
        "method.html",
        active="method",
        method_sections=METHOD_SECTIONS,
        checks=VALIDATION_CHECKS,
    )


@app.route("/sources")
def sources():
    return render_template(
        "sources.html",
        active="sources",
        licences=SOURCE_LICENCES,
        docs=SOURCE_DOCS,
    )


@app.route("/progress")
@login_required
def progress():
    conn = db_connect()
    try:
        data = fetch_dashboard_data(conn)
    finally:
        conn.close()

    total_pages = data["total_pages"]
    models = []
    for m in data["models"]:
        m = dict(m)
        if m["model_tag"] in MODEL_ROLE_LABELS:
            m["role"] = MODEL_ROLE_LABELS[m["model_tag"]]
        elif m["model_tag"].startswith("textparse:"):
            # scripts/parse_ocr_text.py's namespace -- a specific parser
            # model (e.g. "textparse:llama3.1-8b") rather than a fixed
            # tag, so this can't be a MODEL_ROLE_LABELS entry the way a
            # real vision model's fixed tag is.
            m["role"] = "text-parsed from full-page OCR, not the image"
        else:
            m["role"] = ""
        # Same formula the design mockup used: each bar segment is sized
        # against the *total* uploaded pages (not this model's own attempted
        # count), so every model's bar is directly comparable at a glance.
        # Keyed off m["approved"], not m["extraction_success"] -- this page's
        # own lede says a page is never done until a person has looked at
        # it, so a successful-but-unreviewed extraction isn't "done" here.
        m["done_pct"] = (m["approved"] / total_pages * 100) if total_pages else 0
        m["fail_pct"] = (m["content_failed_capped"] / total_pages * 100) if total_pages else 0
        models.append(m)

    any_extracted_pages = data["any_extracted_pages"]
    any_extracted_pct = (any_extracted_pages / total_pages * 100) if total_pages else 0

    return render_template(
        "progress.html",
        active="progress",
        total_pages=total_pages,
        any_extracted_pages=any_extracted_pages,
        any_extracted_pct=any_extracted_pct,
        models=models,
    )


def _table_or_404(conn, table_name):
    tables = list_tables(conn)
    table = next((t for t in tables if t["name"] == table_name), None)
    if table is None:
        abort(404)
    columns = list_columns(conn, table_name)
    return tables, table, columns


@app.route("/tables")
@login_required
def tables_index():
    conn = db_connect()
    try:
        tables = list_tables(conn)
    finally:
        conn.close()
    return render_template("tables_list.html", active="tables", tables=tables)


@app.route("/tables/<table_name>")
@login_required
def table_view(table_name):
    try:
        per_page = int(request.args.get("per_page", 50))
    except ValueError:
        per_page = 50
    if per_page not in TABLE_PAGE_SIZES:
        per_page = 50

    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1
    page = min(max(page, 1), MAX_TABLE_PAGE)

    conn = db_connect()
    try:
        tables, table, columns = _table_or_404(conn, table_name)

        # Only columns that actually exist on this table are ever looked up
        # in request.args -- a filter_<col>/sort for anything else is
        # silently ignored rather than reaching fetch_table_page(), which is
        # what keeps its psycopg2.sql.Identifier() calls safe despite
        # table_name/columns ultimately coming from the URL.
        filters = {}
        for col in columns:
            value = request.args.get(f"filter_{col}", "").strip()
            if value:
                filters[col] = value

        sort_col = request.args.get("sort")
        if sort_col not in columns:
            sort_col = None
        sort_dir = request.args.get("dir")
        if sort_dir not in ("asc", "desc"):
            sort_dir = "asc"

        rows, total = fetch_table_page(conn, table_name, columns, filters, sort_col, sort_dir, page, per_page)
    finally:
        conn.close()

    total_pages = max(math.ceil(total / per_page), 1)
    # Filters only -- sort/dir/page/per_page are passed explicitly wherever
    # a link is built, since Jinja's default globals don't include dict()
    # to merge an override in inline.
    link_params = {f"filter_{c}": v for c, v in filters.items()}

    return render_template(
        "table_view.html",
        active="tables",
        tables=tables,
        table=table,
        table_name=table_name,
        columns=columns,
        rows=rows,
        filters=filters,
        sort_col=sort_col,
        sort_dir=sort_dir,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        page_sizes=TABLE_PAGE_SIZES,
        link_params=link_params,
    )


@app.route("/qc")
@login_required
def qc_index():
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            # image_uploaded_at IS NOT NULL: a placeholder page with no
            # image yet has nothing for a reviewer to look at (same
            # predicate fetch_qc_page()'s own lookup and prev/next use).
            # excluded_at IS NULL: skip straight past a page someone's
            # already pulled out of processing, same as prev/next do.
            cur.execute("SELECT min(id) FROM pages WHERE image_uploaded_at IS NOT NULL AND excluded_at IS NULL")
            (first_id,) = cur.fetchone()
    finally:
        conn.close()
    if first_id is None:
        abort(404)
    return redirect(url_for("qc_page", page_id=first_id))


def _entries_for_display(entries):
    """Reduce full catalogue_entries rows to what the QC page's summary
    table shows, plus a "⚑ field, field" label built from each entry's own
    flags (a jsonb array of {"field", "issue"} -- see pipeline/schema.md).
    Returns (rows, total_flagged_fields) -- the latter backs the status
    row's "N fields flagged" note."""
    rows = []
    total_flagged = 0
    for e in entries:
        flags = e.get("flags") or []
        if not isinstance(flags, list):
            flags = []
        # str() coerces a malformed flag (e.g. {"field": 1}, which the
        # correction form's own flags textarea doesn't stop a reviewer from
        # saving) instead of raising here; sorted(set(...)) both dedupes
        # repeated flags and canonicalizes the "char"/"char_qualifier" alias
        # (see FLAG_FIELD_ALIASES) so it isn't double-counted or double-listed.
        field_names = sorted(
            {
                FLAG_FIELD_ALIAS_TO_CANONICAL.get(str(f["field"]), str(f["field"]))
                for f in flags
                if isinstance(f, dict) and f.get("field")
            }
        )
        total_flagged += len(field_names)
        rows.append(
            {
                "serial": e.get("serial"),
                "title": e.get("title"),
                "author": e.get("author"),
                "copies": e.get("copies"),
                "printer": e.get("printer"),
                "date": e.get("date"),
                "flag_label": ("⚑ " + ", ".join(field_names)) if field_names else "",
            }
        )
    return rows, total_flagged


@app.route("/qc/<int:page_id>")
@login_required
def qc_page(page_id):
    model_tag = request.args.get("model_tag") or None
    conn = db_connect()
    try:
        data = fetch_qc_page(conn, page_id, model_tag)
        if data is not None:
            page_rank, total_pages_available = fetch_qc_position(conn, page_id)
    finally:
        conn.close()
    if data is None:
        abort(404)

    display_entries, flagged_entry_count = _entries_for_display(data["entries"])

    # Only checked here to decide whether to render the <img> tag at all --
    # the tag itself points at /image/<page_id> (see qc_image() below), not
    # at this URL directly, so the page's HTML never embeds a signed B2
    # URL: every image load goes through the login-gated proxy and gets a
    # freshly generated signature, rather than the one computed at the
    # moment this page happened to render.
    image_available = (
        presigned_image_url(
            load_b2_accounts(), data["page"]["b2_account"], data["page"]["b2_bucket"], data["page"]["image_key"]
        )
        is not None
    )
    # Continuing an existing correction re-opens exactly what's already
    # saved for it; starting a fresh one seeds the form from whichever
    # model's entries are currently selected, so a reviewer edits instead
    # of retyping a whole page from scratch. Branches on whether a human
    # row exists at all, not on human_entries being non-empty -- a
    # correction that was deliberately edited down to zero entries must
    # stay empty on reload, not silently resurrect the model's entries.
    prefill_entries = data["human_entries"] if data["human_extraction"] else data["entries"]

    return render_template(
        "qc.html",
        active="qc",
        data=data,
        image_available=image_available,
        prefill_entries=prefill_entries,
        display_entries=display_entries,
        flagged_entry_count=flagged_entry_count,
        page_rank=page_rank,
        total_pages_available=total_pages_available,
        catalogue_fields=CATALOGUE_ENTRY_FIELDS,
        bool_fields=CATALOGUE_ENTRY_BOOL_FIELDS,
        json_fields=CATALOGUE_ENTRY_JSON_FIELDS,
        flag_field_aliases=FLAG_FIELD_ALIASES,
        extra_blank_rows=EXTRA_BLANK_EDIT_ROWS,
    )


@app.route("/image/<int:page_id>")
@login_required
def qc_image(page_id):
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT b2_account, b2_bucket, image_key FROM pages WHERE id = %(page_id)s",
                {"page_id": page_id},
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        abort(404)
    b2_account, b2_bucket, image_key = row
    url = presigned_image_url(load_b2_accounts(), b2_account, b2_bucket, image_key)
    if url is None:
        abort(502)
    return redirect(url)


@app.route("/qc/verdict", methods=["POST"])
@login_required
def qc_verdict():
    extraction_id = request.form.get("extraction_id", type=int)
    verdict = request.form.get("verdict")
    note = request.form.get("note", "").strip()
    if extraction_id is None or verdict not in ("approved", "needs_reprocessing"):
        abort(400)

    conn = db_connect()
    try:
        # apply_qc_verdict() is also the source of the redirect's page_id
        # and model_tag -- read back from the row itself, rather than
        # whatever the form happened to submit alongside it, both because
        # the form's copies were only ever for display (trusting them
        # instead could send a reviewer to the wrong page/tab if they
        # disagreed) and because a value read straight from request.form
        # still gets flagged reaching redirect() via url_for() even though
        # url_for() can only ever build a same-origin URL (see PR history
        # for next=, which was dropped outright rather than validated in
        # place) -- sourcing it from a DB row instead avoids relying on a
        # scanner-specific sanitizer it may not recognize.
        result, page_id, model_tag = apply_qc_verdict(conn, extraction_id, verdict, note)
    finally:
        conn.close()
    if result == "not_found":
        abort(404)
    if result == "claimed":
        # A worker might be mid-page on this right now (or claimed it
        # long enough ago that it *looks* stale, without proof it's
        # actually dead) -- either way its own eventual success/failure
        # write is unconditional and would overwrite whatever this
        # verdict sets, silently discarding the reviewer's "needs
        # reprocessing" reset. There's also nothing meaningful to approve
        # yet. Reject rather than race it; a genuinely stale claim doesn't
        # need QC's help anyway -- claim_next_page() reclaims it on its
        # own regardless.
        abort(409)
    if result == "not_success":
        # Only a completed, successful extraction has output worth
        # signing off on -- a failed or (stale-)claimed row has nothing
        # to approve.
        abort(400)
    return redirect(url_for("qc_page", page_id=page_id, model_tag=model_tag))


@app.route("/qc/exclude", methods=["POST"])
@login_required
def qc_exclude():
    submitted_page_id = request.form.get("page_id", type=int)
    submitted_model_tag = request.form.get("model_tag") or None
    action = request.form.get("action")
    note = request.form.get("note", "").strip()
    if submitted_page_id is None or action not in ("exclude", "include"):
        abort(400)

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM pages WHERE id = %(page_id)s", {"page_id": submitted_page_id})
            row = cur.fetchone()
        if row is None:
            abort(404)
        # Same "read the id back from the row, don't trust the submitted
        # value" treatment as qc_verdict()/qc_save_edit() use for their own
        # redirects -- avoids the same open-redirect scanner finding even
        # though url_for() can only ever build a same-origin URL.
        (page_id,) = row

        model_tag = None
        if submitted_model_tag:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT model_tag FROM llm_extractions WHERE page_id = %(page_id)s AND model_tag = %(model_tag)s",
                    {"page_id": page_id, "model_tag": submitted_model_tag},
                )
                tag_row = cur.fetchone()
            if tag_row:
                (model_tag,) = tag_row

        result = apply_page_exclusion(conn, page_id, action == "exclude", note)
    finally:
        conn.close()
    if result == "not_found":
        # Shouldn't happen given the existence check above, but a page
        # deleted between that SELECT and here (pcloud_files cascades)
        # would otherwise fall through to a misleading redirect.
        abort(404)
    if result == "claimed":
        # A worker may be mid-attempt on this page right now -- see
        # apply_page_exclusion()'s own docstring for why exclusion is
        # rejected outright rather than raced against it. The QC page
        # itself avoids offering the Exclude button in this state (see
        # data.any_claimed in qc.html), so reaching this is a narrow
        # timing window, not the expected path.
        abort(409)
    return redirect(url_for("qc_page", page_id=page_id, model_tag=model_tag))


@app.route("/qc/save_edit", methods=["POST"])
@login_required
def qc_save_edit():
    submitted_page_id = request.form.get("page_id", type=int)
    submitted_model_tag = request.form.get("model_tag") or None
    # `... or 0` would treat a missing or malformed total_rows the same as
    # an explicit, legitimate 0 (which does mean something real: "save
    # this correction with every entry deleted") -- silently running the
    # loop zero times either way and committing an empty correction, even
    # for a request that should have been rejected outright. type=int
    # already returns None for both "absent" and "not a valid int", so
    # None is checked explicitly instead of coalescing it away.
    total_rows = request.form.get("total_rows", type=int)
    if submitted_page_id is None or total_rows is None:
        abort(400)
    if total_rows < 0 or total_rows > MAX_EDIT_ROWS:
        abort(400)

    entries = []
    for i in range(total_rows):
        prefix = f"row_{i}_"
        if request.form.get(prefix + "delete"):
            continue

        entry = {}
        # True for a checked boolean field too, not just a non-blank text
        # value -- otherwise a row whose only content is a checked
        # title_native (every text field left blank) reads as "empty" and
        # gets silently dropped below.
        has_content = False
        for field in CATALOGUE_ENTRY_FIELDS:
            if field in CATALOGUE_ENTRY_BOOL_FIELDS:
                value = bool(request.form.get(prefix + field))
                entry[field] = value
                has_content = has_content or value
                continue

            raw = request.form.get(prefix + field, "").strip()
            if raw == "":
                entry[field] = None
                continue
            has_content = True
            if field in CATALOGUE_ENTRY_INT_FIELDS:
                try:
                    value = int(raw)
                except ValueError:
                    # Reject rather than silently save NULL: a typo while
                    # correcting e.g. `serial` would otherwise erase the
                    # value with no feedback that anything went wrong.
                    abort(400, description=f"Entry {i + 1}: '{field}' must be a whole number, got {raw!r}.")
                # Python's int() has no size limit, but pdf_page/
                # printed_page/serial are Postgres `integer` (32-bit)
                # columns -- an in-range-for-Python value like
                # 2147483648 would otherwise reach the INSERT and raise
                # a raw "integer out of range" error (500) instead of
                # this same 400.
                if not (POSTGRES_INT4_MIN <= value <= POSTGRES_INT4_MAX):
                    abort(400, description=f"Entry {i + 1}: '{field}' is out of range, got {raw!r}.")
                entry[field] = value
            elif field in CATALOGUE_ENTRY_JSON_FIELDS:
                try:
                    # parse_constant rejects Python's json module's
                    # non-standard NaN/Infinity/-Infinity extension: those
                    # parse successfully here but psycopg2.extras.Json
                    # then serializes them as bare (invalid-JSON) tokens
                    # Postgres's jsonb column rejects at write time -- a
                    # 500 well after this 400 was supposed to have already
                    # caught anything unparseable.
                    parsed = json.loads(raw, parse_constant=_reject_json_constant)
                    _reject_non_finite_numbers(parsed)
                    entry[field] = parsed
                except ValueError:
                    abort(400, description=f"Entry {i + 1}: '{field}' must be valid JSON, got {raw!r}.")
            else:
                entry[field] = raw

        if has_content:
            entries.append(entry)

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM pages WHERE id = %(page_id)s", {"page_id": submitted_page_id})
            row = cur.fetchone()
        if row is None:
            abort(404)
        # As in qc_verdict(): the id used for the redirect (and for the
        # actual write) is read back from the pages row itself, not the
        # raw submitted value -- closes the same Semgrep open-redirect
        # finding, and turns what would otherwise be an unhandled foreign-
        # key IntegrityError on a bogus page_id into a clean 404.
        (page_id,) = row

        # Same treatment for model_tag, to preserve which tab the reviewer
        # was on across the redirect without reintroducing the open-
        # redirect finding: only kept if it actually names a real
        # extraction on this page, and even then the value used below is
        # what came back from the query, not the submitted string.
        model_tag = None
        if submitted_model_tag:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT model_tag FROM llm_extractions WHERE page_id = %(page_id)s AND model_tag = %(model_tag)s",
                    {"page_id": page_id, "model_tag": submitted_model_tag},
                )
                tag_row = cur.fetchone()
            if tag_row:
                (model_tag,) = tag_row

        save_human_edit(conn, page_id, entries)
    finally:
        conn.close()
    return redirect(url_for("qc_page", page_id=page_id, model_tag=model_tag))


@app.route("/healthz")
def healthz():
    return {"ok": True}
