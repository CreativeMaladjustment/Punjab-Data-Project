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

from b2 import load_b2_accounts, presigned_image_url
from queries import (
    CATALOGUE_ENTRY_BOOL_FIELDS,
    CATALOGUE_ENTRY_FIELDS,
    CATALOGUE_ENTRY_INT_FIELDS,
    CATALOGUE_ENTRY_JSON_FIELDS,
    fetch_dashboard_data,
    fetch_qc_page,
    fetch_table_page,
    list_columns,
    list_tables,
    save_human_edit,
    save_qc_verdict,
)

TABLE_PAGE_SIZES = (20, 50, 100)
EXTRA_BLANK_EDIT_ROWS = 3  # empty rows offered in the QC edit form for adding
# entries the model missed entirely, on top of however many already exist.

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
            return redirect(url_for("dashboard"))
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
@login_required
def dashboard():
    conn = db_connect()
    try:
        data = fetch_dashboard_data(conn)
    finally:
        conn.close()
    return render_template("dashboard.html", **data)


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
    return render_template("tables_list.html", tables=tables)


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
    page = max(page, 1)

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
            cur.execute("SELECT min(id) FROM pages")
            (first_id,) = cur.fetchone()
    finally:
        conn.close()
    if first_id is None:
        abort(404)
    return redirect(url_for("qc_page", page_id=first_id))


@app.route("/qc/<int:page_id>")
@login_required
def qc_page(page_id):
    model_tag = request.args.get("model_tag") or None
    conn = db_connect()
    try:
        data = fetch_qc_page(conn, page_id, model_tag)
    finally:
        conn.close()
    if data is None:
        abort(404)

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
    # of retyping a whole page from scratch.
    prefill_entries = data["human_entries"] or data["entries"]

    return render_template(
        "qc.html",
        data=data,
        image_available=image_available,
        prefill_entries=prefill_entries,
        catalogue_fields=CATALOGUE_ENTRY_FIELDS,
        bool_fields=CATALOGUE_ENTRY_BOOL_FIELDS,
        json_fields=CATALOGUE_ENTRY_JSON_FIELDS,
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
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_id FROM llm_extractions WHERE id = %(extraction_id)s",
                {"extraction_id": extraction_id},
            )
            row = cur.fetchone()
        if row is None:
            abort(404)
        # The redirect target is this extraction's *actual* page_id, read
        # back from the row itself, rather than whatever page_id the form
        # happened to submit alongside it -- the form's copy was only ever
        # for display, and trusting it instead could send a reviewer to
        # the wrong page if the two ever disagreed. This also closes a
        # Semgrep open-redirect finding: url_for() can only ever build a
        # same-origin URL regardless of this value, but a value read
        # straight from request.form still gets flagged reaching
        # redirect() through it -- sourcing it from a DB row instead
        # avoids relying on a scanner-specific sanitizer it may not
        # recognize (see PR history for next= and model_tag, both of
        # which were dropped outright rather than validated in place;
        # page_id can't be dropped the same way since it's the redirect's
        # whole purpose).
        (page_id,) = row
        save_qc_verdict(conn, extraction_id, verdict, note)
    finally:
        conn.close()
    return redirect(url_for("qc_page", page_id=page_id))


@app.route("/qc/save_edit", methods=["POST"])
@login_required
def qc_save_edit():
    submitted_page_id = request.form.get("page_id", type=int)
    total_rows = request.form.get("total_rows", type=int) or 0
    if submitted_page_id is None:
        abort(400)

    entries = []
    for i in range(total_rows):
        prefix = f"row_{i}_"
        if request.form.get(prefix + "delete"):
            continue

        entry = {}
        has_content = False
        for field in CATALOGUE_ENTRY_FIELDS:
            if field in CATALOGUE_ENTRY_BOOL_FIELDS:
                entry[field] = bool(request.form.get(prefix + field))
                continue

            raw = request.form.get(prefix + field, "").strip()
            if raw == "":
                entry[field] = None
                continue
            has_content = True
            if field in CATALOGUE_ENTRY_INT_FIELDS:
                try:
                    entry[field] = int(raw)
                except ValueError:
                    entry[field] = None
            elif field in CATALOGUE_ENTRY_JSON_FIELDS:
                try:
                    entry[field] = json.loads(raw)
                except ValueError:
                    entry[field] = None
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
        save_human_edit(conn, page_id, entries)
    finally:
        conn.close()
    return redirect(url_for("qc_page", page_id=page_id))


@app.route("/healthz")
def healthz():
    return {"ok": True}
