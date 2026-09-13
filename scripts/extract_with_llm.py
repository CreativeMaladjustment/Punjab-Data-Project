"""Extract catalogue entries from rendered page images using a local vision
LLM served by Ollama, running entirely on the GitHub Actions runner — no
external API calls, no API key. Reads the page images process_pcloud.py
already uploaded to B2 (looked up via Postgres, not by listing B2) and
writes the result — one row per extracted catalogue entry, following
pipeline/schema.md's field shape — into Postgres (Supabase). See
supabase/migrations/20260909140000_init_processing_schema.sql.

No B2 writes happen here at all: B2 is read-only from this script's point
of view (fetching page image bytes to send to Ollama). Postgres is the sole
source of truth for both "which pages have images ready" (the `pages` table,
populated by process_pcloud.py) and "which pages have already been
extracted by which model" (`llm_extractions`, unique on (page_id,
model_tag), so re-running the same model against the same page is a no-op).
Results are namespaced by model (OLLAMA_MODEL, slugified into MODEL_TAG) so
multiple models can be tried against the same page images without
clobbering each other's rows — run the workflow once per model to bake
them off against each other.

main() runs a claim loop rather than fetching every outstanding page up
front: each iteration calls claim_next_page() to atomically pick one
random page and mark it 'claimed' in llm_extractions (see
supabase/migrations/20260910040000_claim_pages_for_extraction.sql),
processes it, then claims the next one. Multiple instances of this script
can run concurrently against the same model (see .github/workflows/
extract-pages.yml's `worker` matrix) without ever claiming the same page:
claim_next_page() locks candidate rows with FOR UPDATE SKIP LOCKED, so a
row already under consideration by one worker simply isn't visible as a
candidate to another. A worker that dies mid-page leaves its claim behind;
it's treated as available again once older than CLAIM_TIMEOUT_SECONDS,
picked up by whichever worker gets there next -- no separate cleanup step.

Supports up to two B2 accounts (see load_b2_accounts), same as
process_pcloud.py, since a page's image may live in either one depending on
which was active when it was uploaded — `pages.b2_account`/`b2_bucket`
records exactly which, so this script always fetches from the right place.

Requires an Ollama server already running and reachable at OLLAMA_HOST (see
.github/workflows/extract-pages.yml) with OLLAMA_MODEL already pulled.
"""
import base64
import contextlib
import json
import os
import pathlib
import re
import sys
import time

import boto3
import psycopg2
import requests
from psycopg2.extras import Json

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ["OLLAMA_MODEL"]
MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", OLLAMA_MODEL)

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]


def load_b2_accounts():
    """Return {"1": {"endpoint", "key_id", "app_key", "bucket"}, "2": {...}}.
    Account "1" (B2_ENDPOINT/B2_KEY_ID/B2_APPLICATION_KEY/B2_BUCKET_NAME) is
    required. Account "2" (the same names suffixed _2) is included only if
    all four of its variables are set — a second account is optional.
    """
    def _account(suffix):
        endpoint = os.environ.get(f"B2_ENDPOINT{suffix}", "")
        key_id = os.environ.get(f"B2_KEY_ID{suffix}", "")
        app_key = os.environ.get(f"B2_APPLICATION_KEY{suffix}", "")
        bucket = os.environ.get(f"B2_BUCKET_NAME{suffix}", "")
        if not (endpoint and key_id and app_key and bucket):
            return None
        if not endpoint.startswith(("http://", "https://")):
            # The B2 console's bucket details page shows the endpoint
            # without a scheme; boto3 requires a full URL.
            endpoint = f"https://{endpoint}"
        return {"endpoint": endpoint, "key_id": key_id, "app_key": app_key, "bucket": bucket}

    accounts = {}
    primary = _account("")
    if primary is None:
        raise RuntimeError(
            "B2 account 1 is not fully configured (need B2_ENDPOINT, B2_KEY_ID, "
            "B2_APPLICATION_KEY, B2_BUCKET_NAME)"
        )
    accounts["1"] = primary
    secondary = _account("_2")
    if secondary is not None:
        accounts["2"] = secondary
    return accounts


B2_ACCOUNTS = load_b2_accounts()

MAX_RUNTIME_SECONDS = 18000  # 5 hours; runner guard, exit 42 to hand off to a fresh run
RUNTIME_GUARD_EXIT_CODE = 42

# Started as a temporary smoke-test cap (2026-09-11) while diagnosing a 400
# every Ollama /api/generate call was throwing across all 9 workers; kept on
# afterwards as a standing per-worker safety ceiling (extract-pages.yml sets
# it well above what one worker will realistically reach in a run -- see the
# comment there) rather than removed, so one worker can't loop through an
# unboundedly huge backlog. 0 (or unset) means unlimited -- the default here
# matters for local/manual runs that don't set the env var; extract-pages.yml
# always sets an explicit value.
MAX_PAGES_PER_WORKER = int(os.environ.get("MAX_PAGES_PER_WORKER", "0"))

# A dense page (a full multi-column table with many entries) can take an
# 8B CPU-only vision model well past 10 minutes; 600s was cutting those off
# before Ollama ever responded. Still well inside MAX_RUNTIME_SECONDS's
# budget for a single page.
#
# Split into separate connect/read values rather than one shared timeout:
# wait_for_ollama() already confirms OLLAMA_HOST is up before any of this
# runs, but if the server dies mid-run, a single 1800s timeout would let
# requests hang that long just trying to connect, not only while waiting
# on a slow model response.
OLLAMA_CONNECT_TIMEOUT_SECONDS = 10
OLLAMA_READ_TIMEOUT_SECONDS = 1800

# glm-ocr's Ollama default (4096) is too small for system prompt + schema +
# image tokens (observed 5140-5178 for a typical page); 8192 is double the
# default, comfortable headroom on a CPU runner without meaningfully
# increasing memory/compute cost at these request sizes.
OLLAMA_NUM_CTX = int(os.environ.get("OLLAMA_NUM_CTX", "8192"))
START_TIME = time.time()

SCHEMA_PATH = pathlib.Path(__file__).resolve().parent.parent / "pipeline" / "schema.md"

SYSTEM_PROMPT = """You transcribe entries from a scanned page of a British \
colonial-era "Catalogue of Books registered" print register. Output ONLY a \
JSON array of entry objects following the schema below. Transcribe verbatim; \
never correct or invent. Flag every uncertain reading in `flags`. If the page \
has no catalog entries (cover, blank, title page, index), output [].

Read the printed page number directly off the page image if one is visible \
(printed in a margin, header, or footer) and put it in `printed_page` as an \
integer. If none is visible, use 0 and add a `flags` entry noting the printed \
page number wasn't visible. Leave `quarter` as "" — it isn't known for this \
source. Output the JSON array only, no commentary.

SCHEMA:
""" + SCHEMA_PATH.read_text(encoding="utf-8")


def elapsed():
    return time.time() - START_TIME


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
    )


def b2_get_bytes(client, bucket, key):
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


DB_CONNECT_MAX_ATTEMPTS = 5  # retried with backoff (2/4/8/16s): connecting can
# transiently fail under pool pressure -- Supabase's session-mode pooler has a
# small fixed client-slot count, and with each page now opening a fresh
# connection only for its brief claim/save moments (see claim_next_page(),
# db_save_extraction_success(), db_save_extraction_failure() below) rather
# than holding one open for the page's entire multi-minute Ollama call, a
# transient "pool momentarily full" on connect is expected occasionally
# under concurrent runs, not a real outage worth failing the whole worker
# over immediately.


def db_connect():
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError as exc:
            if attempt == DB_CONNECT_MAX_ATTEMPTS:
                raise
            print(
                f"DB connect attempt {attempt}/{DB_CONNECT_MAX_ATTEMPTS} failed "
                f"({exc}); retrying",
                file=sys.stderr,
            )
            time.sleep(2**attempt)


@contextlib.contextmanager
def db_connection():
    """Open a connection for exactly the duration of one `with` block, then
    close it -- not just commit/rollback, which `with conn:` alone would do
    while leaving the socket open. Used for every DB touchpoint (claiming,
    saving a result, counting capped failures) so a worker only ever holds
    a pooler slot for the brief moment it's actually running a query, not
    for the page's entire multi-minute Ollama call in between -- see
    process_page(), which does that call with no open connection at all."""
    conn = db_connect()
    try:
        yield conn
    finally:
        conn.close()


CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60  # 3 hours; a worker that dies mid-page leaves
# its claim behind for this long before another worker (or the same one, next
# run) is allowed to pick the page back up.

CLAIM_MAX_ATTEMPTS = 5  # see the "lost the race" note in claim_next_page()

MAX_ATTEMPTS_PER_PAGE = 2  # total tries allowed per (page, model_tag) -- one
# retry after an initial *content* failure -- before claim_next_page() stops
# reclaiming it. A page whose content fails the same way every time (a bad
# JSON shape the model keeps producing) was otherwise reclaimed forever,
# every CLAIM_TIMEOUT_SECONDS, burning worker time on a page that was never
# going to succeed with this model. Once a row hits this cap it stays
# visible (status='failed' AND content_failure AND attempt_count >=
# MAX_ATTEMPTS_PER_PAGE) for manual review or a different model, instead of
# burning worker time on a page that keeps failing the same way.
#
# Scoped to content failures only (content_failure = true -- see
# is_content_failure() and CLAIM_NEXT_PAGE_SQL). process_page() calls
# db_save_extraction_failure() for every kind of failure alike: a
# transient one (a B2/network error, a non-ok Ollama HTTP response, a bad
# catalogue_entries insert) is not the model producing bad output, and
# capping it the same way would be actively harmful -- exactly the
# multi-hour B2 quota outage this project hit on 2026-09-10 (DECISIONS.md
# D-021) would, under an undiscriminating cap, have permanently excluded
# every page it touched even after B2 recovered. Transient failures keep
# retrying indefinitely, exactly like before this cap existed.
#
# Also exempt: a stale 'claimed' row (a worker that died before ever
# recording *any* outcome, content or transient) never got a real attempt
# at extraction, so it must stay reclaimable regardless of attempt_count;
# capping it too would let a worker crash on a page's final permitted try
# leave that row stuck at status='claimed' forever -- looking perpetually
# "in progress" while actually abandoned, and visible nowhere for review.

# candidate CTE narrows to one page: uploaded, and either never attempted
# under model_tag, or claimed/failed but stale. claimed_at is set once, when
# a page is claimed, and left untouched by a failure -- so it still reads
# as "last touched" for a failed row, just from the claim that led to it.
# A fresh failure is deliberately NOT immediately reclaimable -- without the
# same staleness gate a permanently-failing page (e.g. a corrupt image) would
# get claimed, fail, and be claimed right back in the same tight loop for as
# long as it's the only page left, burning the runtime budget on one page
# instead of exiting cleanly. FOR UPDATE OF p SKIP LOCKED makes two
# concurrent claims very unlikely to even consider the same pages row -- but
# it locks `pages`, not `llm_extractions`, so it can't fully prevent two
# transactions from both treating a page with *no existing llm_extractions
# row* as available and both attempting to insert one: with ON CONFLICT DO
# UPDATE unconditional, the second writer would silently overwrite the
# first's fresh claim and *both* callers would come away believing they'd
# claimed the same page. The WHERE on DO UPDATE is what actually closes
# that: it only re-claims a conflicting row that's still claim/fail-stale,
# so the loser of a real race updates zero rows and RETURNING gives it
# nothing back, instead of clobbering the winner.
#
# The final SELECT has no FROM clause, so it always returns exactly one row
# regardless of whether `inserted` produced a row -- an inner join on
# `inserted` would silently vanish (zero rows) on a lost race, which would
# be indistinguishable from a genuinely empty candidate pick.
#
MAX_B2_FAILURES_PER_WORKER = int(os.environ.get("MAX_B2_FAILURES_PER_WORKER", "10"))
if MAX_B2_FAILURES_PER_WORKER < 0:
    raise ValueError(
        "MAX_B2_FAILURES_PER_WORKER must be >= 0 (0 disables the breaker), "
        f"got {MAX_B2_FAILURES_PER_WORKER}"
    )
# 0 disables the breaker entirely (same convention as MAX_PAGES_PER_WORKER
# above) -- main()'s check is `if MAX_B2_FAILURES_PER_WORKER and b2_failures
# >= MAX_B2_FAILURES_PER_WORKER`, not a bare `>=`, specifically so 0 can't
# trip it after the very first page: b2_failures starts at 0, so an
# unguarded `0 >= 0` would fire immediately, on any page, B2 failure or not.
#
# If this worker can't fetch a page's image from B2 this many times in one
# run, stop claiming further pages instead of grinding through the rest of
# the backlog against a B2 that's probably broken for everyone right now --
# bad credentials, a bucket problem, or a quota/outage like the one in
# DECISIONS.md D-021 -- rather than a run of unlucky individual files. This
# is a within-run circuit breaker, separate from MAX_ATTEMPTS_PER_PAGE: it
# doesn't change what's retryable across runs (a page counted here is still
# just a transient failure, reclaimable next run exactly as before), it
# only stops *this* worker from burning its whole runtime budget
# re-downloading against a B2 that isn't going to start working mid-run.
# Deliberately scoped to the B2 fetch specifically (see process_page()'s
# b2_download_failure tagging) -- an LLM/content failure never counts
# toward this, and the loop keeps moving to the next page for those
# exactly as before; only a genuine failure to download counts.

# The failed-row condition below only applies MAX_ATTEMPTS_PER_PAGE to a
# row whose last failure was content_failure -- a transient one (B2/
# network, non-ok Ollama HTTP, a bad insert) must keep retrying regardless
# of attempt_count, exactly like before this cap existed (see
# MAX_ATTEMPTS_PER_PAGE's comment). attempt_count itself only increments
# when reclaiming a stale row whose *last* recorded outcome was a content
# failure -- not for a stale 'claimed' row (never got a real outcome) and
# not for a stale transient failure (not the kind this cap counts) -- so a
# run of crashes or transient errors never eats into the two real content
# attempts the cap promises.
CLAIM_NEXT_PAGE_SQL = """
    WITH candidate AS (
        SELECT p.id
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND (
            le.id IS NULL
            OR (le.status = 'claimed'
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second')
            OR (le.status = 'failed'
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
        ORDER BY random()
        LIMIT 1
        FOR UPDATE OF p SKIP LOCKED
    ),
    inserted AS (
        INSERT INTO llm_extractions (page_id, model, model_tag, status, claimed_at, attempt_count)
        SELECT candidate.id, %(model)s, %(model_tag)s, 'claimed', now(), 1
        FROM candidate
        ON CONFLICT (page_id, model_tag) DO UPDATE SET
            status = 'claimed', claimed_at = now(), error_message = NULL,
            raw_response = NULL, raw_text = NULL,
            attempt_count = CASE
                WHEN llm_extractions.status = 'failed' AND llm_extractions.content_failure
                    THEN llm_extractions.attempt_count + 1
                ELSE llm_extractions.attempt_count
            END
        WHERE (
            (llm_extractions.status = 'claimed'
             AND llm_extractions.claimed_at < now() - %(claim_timeout)s * interval '1 second')
            OR (llm_extractions.status = 'failed'
                AND llm_extractions.claimed_at < now() - %(claim_timeout)s * interval '1 second'
                AND (NOT llm_extractions.content_failure OR llm_extractions.attempt_count < %(max_attempts)s))
        )
        RETURNING page_id
    )
    SELECT (SELECT page_id FROM inserted) AS page_id
"""

# A separate, lock-free EXISTS check -- deliberately not folded into
# CLAIM_NEXT_PAGE_SQL above (a previous version did, and ran it on every
# attempt including a successful claim's, for no benefit: it's only ever
# needed once, after every attempt has failed to claim anything). Also
# deliberately separate from `candidate` there (which takes FOR UPDATE ...
# SKIP LOCKED and reflects only what one attempt's random pick happened to
# see): a page currently claimed by another worker, or one temporarily
# skipped because its `pages` row is momentarily locked, both make
# `candidate` come back empty without meaning the backlog is exhausted --
# this still finds them, so claim_next_page() can tell "genuinely nothing
# left" apart from "nothing grabbable by *this* call right now".
# Deliberately excludes a content-failed row once it's past
# MAX_ATTEMPTS_PER_PAGE -- otherwise a single permanently capped-out page
# would make this true forever and claim_next_page() would never report
# exhaustion again, even once every other page has resolved. A transient
# failure or a stale 'claimed' row has no attempt_count condition here,
# matching CLAIM_NEXT_PAGE_SQL.
PENDING_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND (
            le.id IS NULL
            OR le.status = 'claimed'
            OR (le.status = 'failed'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
    )
"""


CLAIM_CONTENDED = object()  # sentinel: every attempt found a candidate but
# lost the race for it -- distinct from None (every attempt confirmed there
# was truly no candidate). Other workers, or a later run, may still find
# pages even though this call didn't -- callers must not treat this the
# same as genuine exhaustion.


def claim_next_page(model, model_tag, claim_timeout_seconds):
    """Atomically claim one page still needing extraction under model_tag.
    Returns {"page_id", "page_no", "account", "bucket", "image_key",
    "folder", "stem"}; None if a PENDING_EXISTS_SQL check confirmed there
    was truly nothing left; or CLAIM_CONTENDED if that check says otherwise
    (see below) -- callers must treat CLAIM_CONTENDED as "unknown, not
    exhausted", not as equivalent to None.

    Opens its own connection for just this call (see db_connection()) --
    the whole thing (up to CLAIM_MAX_ATTEMPTS claim attempts, plus one
    page-detail lookup) is fast, nowhere near process_page()'s
    multi-minute Ollama call that happens after this returns.

    A single attempt can come back empty even when pages *are* still
    available: it narrows to exactly one random candidate up front under
    FOR UPDATE ... SKIP LOCKED, so it can miss a page that's only
    momentarily locked or claimed by another worker, and if another
    worker's concurrent claim wins the race for the one candidate it did
    pick (see the module-level SQL comment), this attempt's write affects
    zero rows too -- indistinguishable, from the affected-row count alone,
    from "nothing left at all". PENDING_EXISTS_SQL is a separate, lock-free
    existence check that isn't fooled by either case: it resolves the
    ambiguity by asking directly whether any not-yet-resolved row exists,
    regardless of what this attempt's random pick happened to find. Only
    run once every CLAIM_MAX_ATTEMPTS claim attempt has failed -- there's
    no reason to pay for it on the (overwhelmingly common) path where an
    early attempt just succeeds. Retrying the claim itself a few times
    (each picks a fresh random candidate) resolves things in practice for
    a single call when the contention is just a lost race; only genuine
    exhaustion -- confirmed by the existence check, not just an empty pick
    -- survives every attempt. A worker that gives up after
    CLAIM_MAX_ATTEMPTS real races in a row just exits a little early -- a
    later run mops up whatever's left, never a double-claim or a
    permanently skipped page.
    """
    params = {
        "model_tag": model_tag,
        "claim_timeout": claim_timeout_seconds,
        "model": model,
        "max_attempts": MAX_ATTEMPTS_PER_PAGE,
    }
    with db_connection() as conn:
        for _ in range(CLAIM_MAX_ATTEMPTS):
            with conn.cursor() as cur:
                cur.execute(CLAIM_NEXT_PAGE_SQL, params)
                (page_id,) = cur.fetchone()
            conn.commit()  # releases the row lock candidate took, whether or not it matched
            if page_id is not None:
                break
        else:
            with conn.cursor() as cur:
                cur.execute(PENDING_EXISTS_SQL, params)
                (pending_exists,) = cur.fetchone()
            conn.commit()
            return CLAIM_CONTENDED if pending_exists else None

        with conn.cursor() as cur:
            cur.execute(
                "SELECT p.page_no, p.b2_account, p.b2_bucket, p.image_key, pf.folder, pf.name "
                "FROM pages p JOIN pcloud_files pf ON pf.pcloud_fileid = p.pcloud_fileid "
                "WHERE p.id = %s",
                (page_id,),
            )
            page_no, account, bucket, image_key, folder, name = cur.fetchone()
        conn.rollback()  # read-only; drop the implicit transaction

    return {
        "page_id": page_id,
        "page_no": page_no,
        "account": account,
        "bucket": bucket,
        "image_key": image_key,
        "folder": folder,
        "stem": pathlib.Path(name).stem,
    }


def wait_for_ollama(timeout=120):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            resp = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
            if resp.ok:
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(2)
    raise RuntimeError(f"Ollama server did not become ready in time: {last_error}")


def flag_if_printed_page_missing(entry):
    """Note when printed_page couldn't be read, rather than silently storing
    NULL with no explanation. Unlike the old JSON-file version of this
    script, we don't force a 0 sentinel here — NULL in a proper relational
    column already means "unknown" without overloading a real page number.
    """
    pp = entry.get("printed_page")
    if isinstance(pp, str):
        pp = pp.strip()
    looks_valid = (isinstance(pp, int) and not isinstance(pp, bool)) or (
        isinstance(pp, str) and pp.isdigit()
    )
    if not looks_valid:
        flags = entry.get("flags")
        if not isinstance(flags, list):
            flags = []
        flags.append({"field": "printed_page", "issue": "not visible or unparsable on page"})
        entry["flags"] = flags
    return entry


# pipeline/schema.md's entry field names exactly -- not source_folder/
# source_pdf, which process_pdf() sets on each entry itself *after*
# extract_page() returns, so the model never sees or emits them; including
# them here would recognize a shape the model can't actually produce,
# weakening the disambiguation this set exists for. `flags` is the
# schema's only array-valued field -- every other field is a scalar -- so
# a single entry emitted flat (not wrapped in a list) that fills in
# `flags` (the system prompt says to use it "aggressively") is otherwise
# indistinguishable by value shape alone from a dict with one list-valued
# envelope key. Checking against these known field names resolves that
# ambiguity instead of guessing from types.
ENTRY_FIELD_NAMES = {
    "quarter", "pdf_page", "printed_page", "section", "lang", "char", "topic",
    "serial", "reg", "copies", "printer_verbatim", "printer", "pcity", "author",
    "title", "title_native", "gloss", "pp_verbatim", "publisher", "pubcity",
    "date", "price", "edition", "format", "method", "educ", "copyright",
    "notes", "marks", "flags",
}


def _looks_like_entry_list(value):
    """True if value is a non-empty list where every element is a dict
    containing at least one known schema entry field -- i.e. a real list
    of catalogue entries, as opposed to (for example) a `flags` list whose
    elements have keys like "field"/"issue", none of which are schema
    entry fields. Only meaningful for a non-empty list: an empty list's
    contents can't tell you anything, so _coerce_to_entry_list decides
    that case by the list's key name instead (see its comments)."""
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(e, dict) and (e.keys() & ENTRY_FIELD_NAMES) for e in value)
    )


def _coerce_to_entry_list(parsed):
    """Some local models wrap the requested JSON array in a dict, or emit a
    single entry object instead of a one-entry array, even when told to
    output the array only -- observed from minicpm-v4.5 in practice. Recover
    the actual list in these common shapes rather than failing the whole
    page over the model not nesting things exactly as asked:

      - a dict with exactly one list-valued key and no dict-valued keys,
        where that list either (a) is non-empty and its elements each look
        like entry dicts (see _looks_like_entry_list), or (b) is empty and
        its key isn't itself a known schema field -> unwrap to that list,
        e.g. {"printed_page": 1, "entries": [{...one real entry...}]}
        unwraps to the entry inside, *not* the whole wrapper as one bogus
        entry -- observed for real: minicpm-v4.5 hoisting a page-level
        `printed_page` alongside the actual `entries` array trips the
        "known schema key present" signal below if checked first. Any
        other top-level key that's also a known schema field (like that
        `printed_page`) is backfilled onto each entry that doesn't already
        have it, so page-level metadata the model chose to hoist isn't
        lost -- schema.md defines printed_page/quarter etc. per entry, so
        applying the wrapper's value to every entry on the page is exactly
        the intended shape, just written once instead of repeated.
        The empty-list case is its own branch because content can't
        disambiguate an empty list -- {"printed_page": 12, "entries": []}
        (a legitimate zero-entries page, explicitly allowed by the system
        prompt) must still unwrap to [], while {"title": "x", "flags": []}
        (a single entry with nothing flagged) must not: checking the key's
        *name* against ENTRY_FIELD_NAMES tells them apart where the
        (necessarily vacuous) contents check can't.
      - otherwise, a single entry emitted flat -> [parsed], recognized by
        at least one key being a known schema entry field (see
        ENTRY_FIELD_NAMES; without this an unrelated dict, e.g. an error
        payload, would pass as a bogus "entry" just because it has no
        dict values) and no dict-valued keys at all -- the schema has no
        dict-valued fields, so one present means something is genuinely
        off, not a real entry. Deliberately *not* conditioned on how many
        list-valued keys it has or what they're named otherwise: `flags`
        is the schema's one array field, but a real entry hallucinating
        some other list field alongside known fields (e.g. {"title": "x",
        "tags": [...]}) is still a single entry, not an envelope --
        _looks_like_entry_list already rules out matching on `flags`
        itself here, since flag objects (`{"field": ..., "issue": ...}`)
        aren't entry-shaped.
      - otherwise (no known schema keys at all), a dict with exactly one
        list-valued key and no dict-valued keys -- e.g. {"entries": [...]},
        or {"entries": [...], "count": 3} -- is an envelope -> unwrap to
        that list. A dict-valued key alongside it (e.g. {"entries": [...],
        "meta": {...}}) is exactly the "genuinely unrecognized shape" this
        function is meant to still raise on, not metadata to discard.

    Anything else still raises, with the dict's keys included so a real
    unrecognized shape is diagnosable from the error message alone.

    Every element of the returned list is checked to be an entry dict
    (not e.g. a bare string from {"entries": ["oops"]}) before returning,
    on every path -- including the plain-list pass-through -- so a
    genuinely malformed element shape raises a clear error here instead
    of an opaque AttributeError from process_pdf()'s entry.setdefault(...)
    calls three frames away.
    """
    if isinstance(parsed, list):
        result = parsed
    elif not isinstance(parsed, dict):
        raise ValueError(f"expected a JSON array, got {type(parsed).__name__}")
    else:
        list_items = [(k, v) for k, v in parsed.items() if isinstance(v, list)]
        has_dict_value = any(isinstance(v, dict) for v in parsed.values())

        is_entries_envelope = False
        if len(list_items) == 1 and not has_dict_value:
            sole_key, sole_value = list_items[0]
            if len(sole_value) == 0:
                is_entries_envelope = sole_key not in ENTRY_FIELD_NAMES
            else:
                is_entries_envelope = _looks_like_entry_list(sole_value)

        if is_entries_envelope:
            list_key, entries = list_items[0]
            backfill = {k: v for k, v in parsed.items() if k != list_key and k in ENTRY_FIELD_NAMES}
            for entry in entries:
                for k, v in backfill.items():
                    entry.setdefault(k, v)
            result = entries
        else:
            known_keys = parsed.keys() & ENTRY_FIELD_NAMES
            if known_keys and not has_dict_value:
                result = [parsed]
            elif not known_keys and not has_dict_value:
                if len(list_items) != 1:
                    raise ValueError(f"expected a JSON array, got dict with keys {sorted(parsed.keys())}")
                result = list_items[0][1]
            else:
                raise ValueError(f"expected a JSON array, got dict with keys {sorted(parsed.keys())}")

    bad_types = sorted({type(e).__name__ for e in result if not isinstance(e, dict)})
    if bad_types:
        raise ValueError(f"expected a list of entry objects, got element type(s) {bad_types}")
    return result


FULL_PAGE_OCR_PROMPT = """Transcribe every word of text visible on this page \
image, exactly as printed, verbatim, preserving line breaks and reading \
order top to bottom. Include running headers, page numbers, and any text \
outside the catalogue entries -- not just the entries themselves. Output \
plain text only: no JSON, no commentary, no markdown formatting."""


def ocr_full_page(image_bytes, context=""):
    """Independent of extract_page(): a verbatim transcription of everything
    on the page, not just the catalogue entries the schema-constrained
    prompt asks for. Deliberately a separate Ollama call rather than folded
    into extract_page()'s prompt/response -- that prompt and its parsing
    (_coerce_to_entry_list and friends) are already tuned against real
    model quirks (see their docstrings); asking one call to do both jobs at
    once risks degrading the structured-extraction quality this pipeline
    already depends on. Costs one extra model call per page; process_page()
    treats its outcome as fully independent of extract_page()'s -- see
    there for why."""
    b64 = base64.b64encode(image_bytes).decode()
    started = time.time()
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": FULL_PAGE_OCR_PROMPT,
            "images": [b64],
            "stream": False,
            "options": {"num_ctx": OLLAMA_NUM_CTX},
        },
        timeout=(OLLAMA_CONNECT_TIMEOUT_SECONDS, OLLAMA_READ_TIMEOUT_SECONDS),
    )
    if not resp.ok:
        raise RuntimeError(
            f"Ollama /api/generate (full-page OCR) returned {resp.status_code}: {resp.text[:2000]}"
        )
    raw_text = resp.json()["response"]
    print(f"    full-page OCR for {context} in {time.time() - started:.1f}s ({len(raw_text)} chars)")
    return raw_text


def db_save_page_ocr_text(page_id, model, model_tag, raw_text):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO page_ocr_text (page_id, model, model_tag, status, raw_text) "
            "VALUES (%s, %s, %s, 'success', %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'success', raw_text = EXCLUDED.raw_text, error_message = NULL",
            (page_id, model, model_tag, raw_text),
        )
        conn.commit()


def db_save_page_ocr_failure(page_id, model, model_tag, error_message):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO page_ocr_text (page_id, model, model_tag, status, error_message) "
            "VALUES (%s, %s, %s, 'failed', %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'failed', error_message = EXCLUDED.error_message",
            (page_id, model, model_tag, error_message),
        )
        conn.commit()


def extract_page(image_bytes, context=""):
    b64 = base64.b64encode(image_bytes).decode()
    started = time.time()
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "system": SYSTEM_PROMPT,
            "prompt": "Output the JSON array for this page only.",
            "images": [b64],
            "stream": False,
            "format": "json",
            # glm-ocr's default context window (4096) is smaller than
            # system prompt + schema + image tokens for a typical page
            # (observed 5140-5178 tokens on run 34595756362, confirmed via
            # Ollama's own "exceeds the available context size" error once
            # the response body started getting logged) -- override it
            # explicitly rather than relying on the model's default, which
            # can also change out from under us on a fresh `ollama pull`.
            "options": {"num_ctx": OLLAMA_NUM_CTX},
        },
        timeout=(OLLAMA_CONNECT_TIMEOUT_SECONDS, OLLAMA_READ_TIMEOUT_SECONDS),
    )
    if not resp.ok:
        # resp.raise_for_status() only gives the status line ("400 Client
        # Error: Bad Request for url: ..."), not Ollama's actual reason --
        # include the response body so a rejected request is diagnosable
        # from this error_message alone, without re-fetching Actions logs.
        raise RuntimeError(
            f"Ollama /api/generate returned {resp.status_code}: {resp.text[:2000]}"
        )
    raw_text = resp.json()["response"]
    text = raw_text.strip()
    text = re.sub(r"^```(json)?|```$", "", text, flags=re.M).strip()
    print(
        f"    ollama response for {context} in {time.time() - started:.1f}s "
        f"({len(text)} chars): {text[:300]!r}"
    )
    try:
        parsed = json.loads(text)  # fail loudly; the page can be retried next run
        entries = _coerce_to_entry_list(parsed)
    except Exception as exc:
        # Attach raw_text so process_page() can still persist what the model
        # actually said even though extraction failed -- letting this raise
        # unadorned would lose it, since the caller only sees the exception.
        exc.raw_text = raw_text
        raise
    return entries, raw_text


def _text(entry, key):
    v = entry.get(key)
    return None if v is None else str(v)


def _int_or_none(entry, key):
    v = entry.get(key)
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    return None


def _bool_or_none(entry, key):
    v = entry.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        low = v.strip().lower()
        if low in ("true", "yes", "1"):
            return True
        if low in ("false", "no", "0", ""):
            return False
    return None


def build_entry_row(extraction_id, entry_index, entry):
    return (
        extraction_id,
        entry_index,
        _text(entry, "quarter"),
        _int_or_none(entry, "pdf_page"),
        _int_or_none(entry, "printed_page"),
        _text(entry, "section"),
        _text(entry, "lang"),
        _text(entry, "char"),
        _text(entry, "topic"),
        _int_or_none(entry, "serial"),
        _text(entry, "reg"),
        _text(entry, "copies"),
        _text(entry, "printer_verbatim"),
        _text(entry, "printer"),
        _text(entry, "pcity"),
        _text(entry, "author"),
        _text(entry, "title"),
        _bool_or_none(entry, "title_native"),
        _text(entry, "gloss"),
        _text(entry, "pp_verbatim"),
        _text(entry, "publisher"),
        _text(entry, "pubcity"),
        _text(entry, "date"),
        _text(entry, "price"),
        _text(entry, "edition"),
        _text(entry, "format"),
        _text(entry, "method"),
        _text(entry, "educ"),
        _text(entry, "copyright"),
        _text(entry, "notes"),
        _text(entry, "marks"),
        Json(entry.get("flags") or []),
        _text(entry, "source_folder"),
        _text(entry, "source_pdf"),
    )


# One fully static literal (no string building/concatenation) so this can't
# be mistaken for a SQL-injection-shaped pattern — every value still goes
# through a parameterized %s via build_entry_row(); this is just the column
# list. psycopg2 raises clearly on any column/placeholder count mismatch.
INSERT_ENTRY_SQL = """
    INSERT INTO catalogue_entries (
        extraction_id, entry_index, quarter, pdf_page, printed_page, section, lang,
        char_qualifier, topic, serial, reg, copies, printer_verbatim, printer, pcity,
        author, title, title_native, gloss, pp_verbatim, publisher, pubcity, date,
        price, edition, format, method, educ, copyright, notes, marks, flags,
        source_folder, source_pdf
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""


def db_save_extraction_success(page_id, model, model_tag, entries, raw_text):
    # Its own connection, opened just for this call (see db_connection()) --
    # by the time this runs, extract_page()'s multi-minute Ollama call has
    # already finished, so nothing here needs a connection held open any
    # longer than this one write actually takes.
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions (page_id, model, model_tag, status, raw_response, raw_text) "
            "VALUES (%s, %s, %s, 'success', %s, %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'success', raw_response = EXCLUDED.raw_response, "
            "raw_text = EXCLUDED.raw_text, error_message = NULL "
            "RETURNING id",
            (page_id, model, model_tag, Json(entries), raw_text),
        )
        extraction_id = cur.fetchone()[0]
        cur.execute("DELETE FROM catalogue_entries WHERE extraction_id = %s", (extraction_id,))
        for idx, entry in enumerate(entries):
            cur.execute(INSERT_ENTRY_SQL, build_entry_row(extraction_id, idx, entry))
        conn.commit()


def is_content_failure(exc):
    """True if this failure means the model responded but its output was
    unusable (bad JSON/shape) -- deterministic, will very likely fail the
    same way again with this model, so it's the kind MAX_ATTEMPTS_PER_PAGE
    is meant to bound. False for anything upstream of getting a response
    (a B2/network error, a non-ok Ollama HTTP status, an unconfigured
    account) or downstream of it (a bad catalogue_entries insert) -- those
    are transient/infra and must keep retrying indefinitely, exactly like
    before this cap existed. extract_page() only attaches raw_text to
    exceptions it raises itself, after successfully parsing a response
    body -- i.e. content/shape failures specifically -- so its presence is
    exactly this distinction."""
    return getattr(exc, "raw_text", None) is not None


def db_save_extraction_failure(page_id, model, model_tag, error_message, raw_text, content_failure):
    # Its own connection, opened just for this call (see db_connection()).
    # Previously this reused a long-lived connection shared with
    # db_save_extraction_success(), so a rollback was needed here first in
    # case that call had left the connection mid-aborted-transaction (e.g.
    # a bad catalogue_entries insert); now every call gets a brand-new
    # connection that was never touched by anything else, so there's
    # nothing to roll back.
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions "
            "(page_id, model, model_tag, status, error_message, raw_text, content_failure) "
            "VALUES (%s, %s, %s, 'failed', %s, %s, %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'failed', error_message = EXCLUDED.error_message, "
            "raw_text = EXCLUDED.raw_text, content_failure = EXCLUDED.content_failure",
            (page_id, model, model_tag, error_message, raw_text, content_failure),
        )
        conn.commit()


def process_page(clients, claim):
    """Extract one already-claimed page and record success or failure.
    claim: the dict returned by claim_next_page(). Returns
    (success, b2_download_failure): success is True on success, False if
    it needs a retry; b2_download_failure is True only when this specific
    failure happened while fetching the page's image from B2 -- see
    MAX_B2_FAILURES_PER_WORKER, which counts exactly this and nothing
    else (not an LLM/content failure, not an unconfigured-account guard,
    not a bad DB insert).

    Holds no database connection at all during extract_page()'s call to
    Ollama (the slow part, often 5+ minutes) -- db_save_extraction_success()
    and db_save_extraction_failure() each open their own short-lived
    connection only once there's an actual result to write."""
    page_id = claim["page_id"]
    page_no = claim["page_no"]
    account = claim["account"]
    bucket = claim["bucket"]
    image_key = claim["image_key"]
    folder = claim["folder"]
    stem = claim["stem"]
    context = f"{folder}/{stem} page {page_no}"

    print(f"  page {page_no}: b2 account={account} bucket={bucket} key={image_key}")
    raw_text = None  # set once extract_page() returns; stays None if the
    # failure happens before that (a non-ok HTTP response, a B2/network
    # error, an unconfigured account) -- there's no model output yet then.
    try:
        try:
            client = clients[account]
        except KeyError:
            raise RuntimeError(
                f"page {folder}/{stem} page_no={page_no} is recorded in account "
                f"{account!r}, but that account isn't configured in this run's secrets"
            )
        try:
            image_bytes = b2_get_bytes(client, bucket, image_key)
        except Exception as exc:
            # Tagged here, at the one call site that actually fetches the
            # image, rather than inferred later from the exception's type --
            # so MAX_B2_FAILURES_PER_WORKER counts exactly "couldn't
            # download this page's image from B2", not anything else that
            # happens to also raise an Exception subclass.
            exc.b2_download_failure = True
            raise
        # Full-page OCR: independent of the structured extraction below --
        # best-effort, and deliberately not allowed to affect this page's
        # success/failure/retry accounting (MAX_ATTEMPTS_PER_PAGE,
        # content_failure, MAX_B2_FAILURES_PER_WORKER all stay scoped to
        # extract_page()'s outcome only, exactly as before this existed).
        try:
            page_text = ocr_full_page(image_bytes, context=context)
            db_save_page_ocr_text(page_id, OLLAMA_MODEL, MODEL_TAG, page_text)
        except Exception as exc:
            print(f"WARNING: full-page OCR failed for {context}: {exc}")
            db_save_page_ocr_failure(page_id, OLLAMA_MODEL, MODEL_TAG, str(exc))
        entries, raw_text = extract_page(image_bytes, context=context)
        for entry in entries:
            entry.setdefault("source_folder", folder)
            entry.setdefault("source_pdf", stem)
            entry["pdf_page"] = page_no - 1  # known exactly; don't trust the model's guess
            flag_if_printed_page_missing(entry)
        entries_json = json.dumps(entries)
        print(f"    saving {len(entries)} entries for {context}: {entries_json[:500]!r}")
        db_save_extraction_success(page_id, OLLAMA_MODEL, MODEL_TAG, entries, raw_text)
        print(f"done: {context}")
        return True, False
    except Exception as exc:
        # Classified from the exception itself, before raw_text below might
        # fall back to a pre-existing local value from an *earlier*,
        # already-successful extract_page() call -- content_failure must
        # reflect only whether *this* exception is the model-produced-bad-
        # output kind, not whatever raw_text happens to end up holding.
        content_failure = is_content_failure(exc)
        b2_download_failure = getattr(exc, "b2_download_failure", False)
        # extract_page() attaches raw_text to exceptions it raises itself
        # (bad JSON/shape) -- prefer that when present. Otherwise, fall
        # back to the local raw_text above rather than clobbering it with
        # None: an exception raised *after* extract_page() already
        # returned (e.g. while enriching an entry, or a bad
        # catalogue_entries insert inside db_save_extraction_success())
        # doesn't carry its own raw_text, but the model output it already
        # returned is still sitting in the local variable and shouldn't be
        # thrown away just because something later in the pipeline failed.
        raw_text = getattr(exc, "raw_text", raw_text)
        print(f"WARNING: page {page_no} of {folder}/{stem} failed: {exc}; will retry next run")
        db_save_extraction_failure(
            page_id, OLLAMA_MODEL, MODEL_TAG, str(exc), raw_text, content_failure
        )
        return False, b2_download_failure


def count_capped_failures(model_tag):
    """How many pages under model_tag are permanently done retrying --
    status='failed', content_failure (not a transient error -- see
    is_content_failure()), and attempt_count at or past
    MAX_ATTEMPTS_PER_PAGE, so claim_next_page() will never reclaim them
    again. Queried once at the end of a run so a genuinely-exhausted
    backlog that still has pages stuck at the cap gets reported as needing
    review, not silently folded into "all pages processed"."""
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM llm_extractions "
            "WHERE model_tag = %s AND status = 'failed' AND content_failure "
            "AND attempt_count >= %s",
            (model_tag, MAX_ATTEMPTS_PER_PAGE),
        )
        return cur.fetchone()[0]


def main():
    wait_for_ollama()
    clients = {aid: b2_client(acct) for aid, acct in B2_ACCOUNTS.items()}

    print(f"model: {OLLAMA_MODEL} (tag: {MODEL_TAG})")
    if MAX_PAGES_PER_WORKER:
        print(f"MAX_PAGES_PER_WORKER set: stopping after {MAX_PAGES_PER_WORKER} page(s)")
    processed = 0
    any_incomplete = False
    limited = False
    contended = False
    b2_capped = False
    b2_failures = 0
    while True:
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before claiming another page"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        if MAX_PAGES_PER_WORKER and processed >= MAX_PAGES_PER_WORKER:
            print(f"MAX_PAGES_PER_WORKER limit ({MAX_PAGES_PER_WORKER}) reached; stopping")
            limited = True
            break

        claim = claim_next_page(OLLAMA_MODEL, MODEL_TAG, CLAIM_TIMEOUT_SECONDS)
        if claim is None:
            break
        if claim is CLAIM_CONTENDED:
            print(
                "gave up after repeated claim contention, not confirmed exhaustion; "
                "other workers or a later run may still find pages"
            )
            contended = True
            break
        ok, b2_download_failure = process_page(clients, claim)
        if not ok:
            any_incomplete = True
            # A genuine LLM/content failure never touches b2_failures -- the
            # loop just moves on to the next page exactly as before. Only a
            # run of actual B2 download failures can trip this breaker.
            if b2_download_failure:
                b2_failures += 1
        processed += 1  # counts every page process_page() was called on,
        # same as every other stop path below -- incremented before the
        # b2_capped break too, so "processed N page(s)" always reflects
        # what actually got attempted, including the page that tripped it.
        print(f"count of pages processed so far: {processed}")
        if MAX_B2_FAILURES_PER_WORKER and b2_failures >= MAX_B2_FAILURES_PER_WORKER:
            # The `MAX_B2_FAILURES_PER_WORKER and` guard is load-bearing, not
            # redundant: without it, MAX_B2_FAILURES_PER_WORKER=0 (meant to
            # disable the breaker) would instead make `0 >= 0` true right
            # after this very first page -- success or failure, B2 or not --
            # since b2_failures starts at 0. Deliberately not gated by
            # MAX_PAGES_PER_WORKER's cap -- a broken B2 is worth noticing
            # regardless of how many pages a worker is allowed to reach.
            print(
                f"{b2_failures} B2 download failures this run; stopping before "
                "claiming another page -- this many failures to fetch images "
                "likely means B2 itself is broken right now (bad credentials, a "
                "bucket problem, or a quota/outage like DECISIONS.md D-021), not "
                "a run of unlucky individual files"
            )
            b2_capped = True
            break

    print(f"processed {processed} page(s) total; stopped for the reason logged above")

    # Printed before any sys.exit(1) below -- a run with any_incomplete set
    # is exactly when knowing *why* the loop stopped (hit the limiter,
    # contended, hit the B2 circuit breaker, or genuinely exhausted)
    # matters most; exiting non-zero first would silently drop that
    # context from the log.
    if limited:
        # MAX_PAGES_PER_WORKER stopped this worker before it ever asked
        # whether more pages exist -- don't claim to know either way.
        print(
            f"limited run: stopped at MAX_PAGES_PER_WORKER ({MAX_PAGES_PER_WORKER}); "
            "whether more pages remain is unknown"
        )
    elif contended:
        print("stopped on claim contention; backlog status unknown, not confirmed exhausted")
    elif b2_capped:
        print(
            f"stopped after {b2_failures} B2 download failures (MAX_B2_FAILURES_PER_WORKER="
            f"{MAX_B2_FAILURES_PER_WORKER}); whether more pages remain is unknown -- "
            "confirm B2 access is actually working before re-running"
        )
    else:
        capped = count_capped_failures(MODEL_TAG)
        if capped:
            print(
                f"claimable backlog exhausted, but {capped} page(s) had content the model "
                f"couldn't produce a usable response for after {MAX_ATTEMPTS_PER_PAGE} attempts "
                f"and need manual review or a different model (status='failed' AND "
                f"content_failure AND attempt_count >= {MAX_ATTEMPTS_PER_PAGE})"
            )
        else:
            print("all pages processed")

    if any_incomplete:
        print("one or more pages failed extraction; exiting non-zero so this is visible")
        sys.exit(1)


if __name__ == "__main__":
    main()
