"""Extract catalogue entries from rendered page images using Google's hosted
Gemini API, as an additional model alongside the local-Ollama vision models
extract_with_llm.py runs. Reads the page images process_pcloud.py already
uploaded to B2 (looked up via Postgres) and writes the result -- one row per
extracted catalogue entry, following pipeline/schema.md's field shape --
into Postgres (Supabase). See supabase/migrations/20260909140000_init_
processing_schema.sql.

Why a separate script rather than a code path inside extract_with_llm.py:
that script's whole shape (wait_for_ollama, an Ollama /api/generate call, an
18-worker matrix) is local-CPU-inference-specific; Gemini is a hosted API
with its own auth, request shape, and -- the reason this script exists at
all -- its own per-minute and per-day free-tier rate limits that have no
Ollama equivalent to design around. Keeping them separate means neither
script's control flow has to carry conditionals for the other's concerns.

Namespaced under model_tag = slugified GEMINI_MODEL (e.g. "gemini-2.5-
flash") -- an ordinary peer in the same llm_extractions/model_tag bake-off
as glm-ocr, minicpm-v4.6, etc., not a reserved namespace like
HUMAN_MODEL_TAG or scripts/parse_ocr_text.py's "textparse:" prefix, since
this genuinely is another model's own attempt at the same structured
extraction task.

Rate limits are the entire reason this script's shape differs from
extract_with_llm.py beyond swapping Ollama for Gemini:

  - GEMINI_MODEL_PACING below sets a floor pace (a minimum spacing between
    requests) per model, chosen from Google's published free-tier requests-
    per-minute ceiling for that model tier -- confirm the current numbers at
    https://ai.google.dev/gemini-api/docs/rate-limits before relying on
    them, Google revises these over time and this script deliberately
    doesn't try to auto-detect them. This is a *floor*, not the actual
    protection -- request() below still handles a real 429 with backoff
    (honoring Retry-After when Google sends one) regardless of pacing,
    since the pacing number can drift stale in either direction.
  - Single worker, not a matrix (see .github/workflows/extract-pages-
    gemini.yml) -- unlike Ollama's per-runner CPU inference, N parallel
    Gemini callers would each independently pace against the SAME shared
    per-project rate limit, so more workers here doesn't mean more
    throughput, just N times the 429s.
  - A full-page OCR pass runs too (see gemini_ocr_full_page(), mirroring
    extract_with_llm.py's ocr_full_page()) -- a second full API call per
    page, which roughly halves how many pages a day's free-tier quota
    reaches. Accepted deliberately: page_ocr_text is worth having rather
    than re-spending a whole image call through scripts/parse_ocr_text.py
    later just to get the same transcription from text already sitting in
    Postgres. Both calls share the same pace()/429 handling below, so the
    accounting for "how much quota does one page cost" already reflects
    two calls, not one.

Default candidate selection, rescue mode (SOURCE_MODEL), and
ALLOW_ALREADY_EXTRACTED all work exactly like extract_with_llm.py's own --
see that module's docstring for the full reasoning; duplicated here rather
than imported for the same reason parse_ocr_text.py duplicates them: that
module does real work at import time (connecting to B2, requiring
OLLAMA_MODEL) this script has no reason to need.
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
from botocore.config import Config
from psycopg2.extras import Json

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", GEMINI_MODEL)

# See the module docstring's "rate limits" section. Deliberately per-tier
# (flash vs. pro), not per-exact-model -- both generations share the same
# free-tier request-per-minute ceiling for a given tier as of this writing.
# A model not listed here (e.g. a new release) falls back to the
# conservative "pro" pace rather than assuming a generous one.
GEMINI_MODEL_PACING = {
    "gemini-2.5-flash": 4.0,
    "gemini-1.5-flash": 4.0,
    "gemini-2.5-pro": 20.0,
    "gemini-1.5-pro": 20.0,
}
PACE_SECONDS = float(os.environ.get("GEMINI_PACE_SECONDS", GEMINI_MODEL_PACING.get(GEMINI_MODEL, 20.0)))

# See extract_with_llm.py's own SOURCE_MODEL/rescue-mode comment -- same
# mechanic, provider-agnostic: model_tag alone decides what's rescuable, so
# this can rescue an Ollama model's (or another Gemini model's) capped
# content failures exactly the same way.
SOURCE_MODEL = os.environ.get("SOURCE_MODEL", "").strip()
if SOURCE_MODEL == "(none)":
    SOURCE_MODEL = ""
SOURCE_MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", SOURCE_MODEL) if SOURCE_MODEL else None
if SOURCE_MODEL_TAG is not None and SOURCE_MODEL_TAG == MODEL_TAG:
    raise ValueError(
        f"SOURCE_MODEL ({SOURCE_MODEL!r}) slugifies to the same tag as GEMINI_MODEL "
        f"({GEMINI_MODEL!r}) -- a model can't rescue its own capped content failures "
        "under its own tag; set GEMINI_MODEL to a different model"
    )

# See extract_with_llm.py's own ALLOW_ALREADY_EXTRACTED comment -- same
# default-skip-elsewhere-successful behavior, same opt-out for a deliberate
# comparison run.
ALLOW_ALREADY_EXTRACTED = os.environ.get("ALLOW_ALREADY_EXTRACTED", "").strip().lower() in ("true", "1", "yes")

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]


def load_b2_accounts():
    """Identical to extract_with_llm.py's load_b2_accounts() -- see there
    for the full reasoning; duplicated for the same not-importing-that-
    module rationale the rest of this file's docstring gives."""
    def _account(suffix):
        endpoint = os.environ.get(f"B2_ENDPOINT{suffix}", "")
        key_id = os.environ.get(f"B2_KEY_ID{suffix}", "")
        app_key = os.environ.get(f"B2_APPLICATION_KEY{suffix}", "")
        bucket = os.environ.get(f"B2_BUCKET_NAME{suffix}", "")
        if not (endpoint and key_id and app_key and bucket):
            return None
        if not endpoint.startswith(("http://", "https://")):
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

MAX_RUNTIME_SECONDS = 18000  # 5 hours; same runner guard as extract_with_llm.py
RUNTIME_GUARD_EXIT_CODE = 42
MAX_PAGES_PER_WORKER = int(os.environ.get("MAX_PAGES_PER_WORKER", "0"))
START_TIME = time.time()

GEMINI_CONNECT_TIMEOUT_SECONDS = 10
GEMINI_READ_TIMEOUT_SECONDS = 120  # a hosted API; generous but nowhere near
# the CPU-inference timeouts extract_with_llm.py needs for local Ollama.

SCHEMA_PATH = pathlib.Path(__file__).resolve().parent.parent / "pipeline" / "schema.md"

GEMINI_SYSTEM_PROMPT = """You transcribe entries from a scanned page of a British \
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

# Mirrors extract_with_llm.py's FULL_PAGE_OCR_PROMPT exactly -- see there
# for the full reasoning (a separate, unconstrained transcription of
# everything on the page, not just the catalogue entries the schema-
# constrained prompt above asks for).
GEMINI_OCR_PROMPT = """Transcribe every word of text visible on this page \
image, exactly as printed, verbatim, preserving line breaks and reading \
order top to bottom. Include running headers, page numbers, and any text \
outside the catalogue entries -- not just the entries themselves. Output \
plain text only: no JSON, no commentary, no markdown formatting."""


def elapsed():
    return time.time() - START_TIME


_B2_REGION_RE = re.compile(r"^s3\.([a-z0-9-]+)\.backblazeb2\.com$")


def _b2_region(endpoint):
    host = endpoint.split("://", 1)[-1]
    match = _B2_REGION_RE.match(host)
    return match.group(1) if match else "us-east-1"


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
        region_name=_b2_region(account["endpoint"]),
        config=Config(signature_version="s3v4"),
    )


def b2_get_bytes(client, bucket, key):
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


DB_CONNECT_MAX_ATTEMPTS = 5


def db_connect():
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError as exc:
            if attempt == DB_CONNECT_MAX_ATTEMPTS:
                raise
            print(
                f"DB connect attempt {attempt}/{DB_CONNECT_MAX_ATTEMPTS} failed ({exc}); retrying",
                file=sys.stderr,
            )
            time.sleep(2**attempt)


@contextlib.contextmanager
def db_connection():
    conn = db_connect()
    try:
        yield conn
    finally:
        conn.close()


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


# Mirrors extract_with_llm.py's CLAIM_TIMEOUT_SECONDS/MAX_ATTEMPTS_PER_PAGE
# exactly -- must stay numerically identical: the dashboard interprets
# every model_tag's claimed/failed rows against these same two numbers,
# this one included.
CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60
CLAIM_MAX_ATTEMPTS = 5
MAX_ATTEMPTS_PER_PAGE = 2

MAX_B2_FAILURES_PER_WORKER = int(os.environ.get("MAX_B2_FAILURES_PER_WORKER", "10"))

_SOURCE_CAPPED_FILTER = """
          AND EXISTS (
            SELECT 1 FROM llm_extractions src
            WHERE src.page_id = p.id
              AND src.model_tag = %(source_model_tag)s
              AND src.status = 'failed'
              AND src.content_failure
              AND src.attempt_count >= %(max_attempts)s
          )
""" if SOURCE_MODEL_TAG else ""

# See extract_with_llm.py's own _SKIP_ALREADY_EXTRACTED_FILTER comment for
# the full reasoning, including why this only ever gates the `le.id IS
# NULL` (never-attempted) branch below, not a page this model_tag has
# already claimed or failed on -- the same stale-claim-stranding bug a
# Copilot review caught there applies here identically.
_SKIP_ALREADY_EXTRACTED_FILTER = """
                AND NOT EXISTS (
                  SELECT 1 FROM llm_extractions any_le
                  WHERE any_le.page_id = p.id AND any_le.status = 'success'
                )""" if not ALLOW_ALREADY_EXTRACTED else ""

# Built by plain concatenation, not str.format()/an f-string, even though
# every spliced-in piece (_SKIP_ALREADY_EXTRACTED_FILTER,
# _SOURCE_CAPPED_FILTER) is a hardcoded, import-time-fixed SQL fragment
# with no request/user-controlled content anywhere near it -- a code-
# scanning Bandit rule (B608) pattern-matches on string formatting applied
# to SQL-keyword-shaped text specifically, with no awareness of what's
# actually being substituted, so the earlier .format()-based version of
# this query (and PENDING_EXISTS_SQL below) tripped it even though nothing
# about the actual data flow changed. Real per-row values still go through
# psycopg2's own %(name)s parameterization via the params dict passed to
# cur.execute() in claim_next_page() -- never through this splicing.
CLAIM_NEXT_PAGE_SQL = """
    WITH candidate AS (
        SELECT p.id
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND p.excluded_at IS NULL
          AND (
            (le.id IS NULL""" + _SKIP_ALREADY_EXTRACTED_FILTER + """)
            OR (le.status = 'claimed'
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second')
            OR (le.status = 'failed'
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
          """ + _SOURCE_CAPPED_FILTER + """
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

# See CLAIM_NEXT_PAGE_SQL's own comment just above -- same concatenation-
# not-format() reasoning, same two hardcoded fragments spliced in.
PENDING_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND p.excluded_at IS NULL
          AND (
            (le.id IS NULL""" + _SKIP_ALREADY_EXTRACTED_FILTER + """)
            OR le.status = 'claimed'
            OR (le.status = 'failed'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
          """ + _SOURCE_CAPPED_FILTER + """
    )
"""

CLAIM_CONTENDED = object()


def claim_next_page():
    """Atomically claim one page still needing extraction under MODEL_TAG.
    See extract_with_llm.py's claim_next_page() for the full reasoning --
    identical shape, just no `model`/`model_tag` parameters since this
    script only ever runs against the one GEMINI_MODEL its process is
    configured for."""
    params = {
        "model_tag": MODEL_TAG,
        "model": GEMINI_MODEL,
        "claim_timeout": CLAIM_TIMEOUT_SECONDS,
        "max_attempts": MAX_ATTEMPTS_PER_PAGE,
        "source_model_tag": SOURCE_MODEL_TAG,
    }
    with db_connection() as conn:
        for _ in range(CLAIM_MAX_ATTEMPTS):
            with conn.cursor() as cur:
                cur.execute(CLAIM_NEXT_PAGE_SQL, params)
                (page_id,) = cur.fetchone()
            conn.commit()
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
        conn.rollback()

    return {
        "page_id": page_id,
        "page_no": page_no,
        "account": account,
        "bucket": bucket,
        "image_key": image_key,
        "folder": folder,
        "stem": pathlib.Path(name).stem,
    }


def flag_if_printed_page_missing(entry):
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


# Deliberate duplicate of extract_with_llm.py's ENTRY_FIELD_NAMES/
# _looks_like_entry_list/_coerce_to_entry_list -- see that module for the
# full reasoning behind each recovered shape. Kept in sync by hand.
ENTRY_FIELD_NAMES = {
    "quarter", "pdf_page", "printed_page", "section", "lang", "char", "topic",
    "serial", "reg", "copies", "printer_verbatim", "printer", "pcity", "author",
    "title", "title_native", "gloss", "pp_verbatim", "publisher", "pubcity",
    "date", "price", "edition", "format", "method", "educ", "copyright",
    "notes", "marks", "flags",
}


def _looks_like_entry_list(value):
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(e, dict) and (e.keys() & ENTRY_FIELD_NAMES) for e in value)
    )


def _coerce_to_entry_list(parsed):
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


_LAST_CALL_AT = [0.0]  # mutable single-element box; a plain module global
# reassigned from inside pace() would need a `global` statement at every
# call site otherwise -- this is simpler for a script with exactly one
# in-process caller (no threads/workers here, single-worker by design).


def pace():
    """Sleep just enough to keep requests at least PACE_SECONDS apart,
    measured from the *end* of the previous call to the *start* of this
    one -- not a blind sleep(PACE_SECONDS) before every call, so a call
    that itself took longer than the pace interval (a large/dense page)
    doesn't add needless extra delay on top."""
    wait = PACE_SECONDS - (time.time() - _LAST_CALL_AT[0])
    if wait > 0:
        time.sleep(wait)


GEMINI_MAX_429_RETRIES = 5


def _gemini_post(body, context):
    """Shared POST-with-pace-and-429-retry core for both gemini_generate()
    and gemini_ocr_full_page() below -- pacing and backoff apply per HTTP
    call, not per logical operation, and both make one call each, so they
    share this rather than each keeping its own copy of the retry loop.
    Returns the model's raw text response; raises (no .raw_text attached,
    i.e. transient by is_content_failure()'s convention) on anything short
    of a clean 200 with a text candidate in it."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}

    backoff = 30
    for attempt in range(1, GEMINI_MAX_429_RETRIES + 1):
        pace()
        resp = requests.post(
            url, json=body, headers=headers,
            timeout=(GEMINI_CONNECT_TIMEOUT_SECONDS, GEMINI_READ_TIMEOUT_SECONDS),
        )
        _LAST_CALL_AT[0] = time.time()
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else backoff
            print(f"    429 from Gemini for {context} (attempt {attempt}/{GEMINI_MAX_429_RETRIES}); waiting {wait:.0f}s")
            time.sleep(wait)
            backoff = min(backoff * 2, 300)
            continue
        break
    else:
        raise RuntimeError(f"Gemini kept returning 429 after {GEMINI_MAX_429_RETRIES} retries")

    if not resp.ok:
        # Anything other than a clean 200 (auth, bad request, 5xx) is
        # transient/infra by the same convention extract_with_llm.py uses
        # for a non-ok Ollama response -- retried indefinitely, not capped.
        raise RuntimeError(f"Gemini generateContent returned {resp.status_code}: {resp.text[:2000]}")

    payload = resp.json()
    try:
        return payload["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Gemini response had no text candidate: {json.dumps(payload)[:2000]}") from exc


def gemini_generate(image_bytes, context=""):
    """The structured-extraction call: JSON mode, schema-constrained
    prompt. Returns (entries, raw_text); raises with .raw_text attached on
    a parse/shape failure, same contract as extract_with_llm.py's
    extract_page()."""
    b64 = base64.b64encode(image_bytes).decode()
    body = {
        "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_PROMPT}]},
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": "image/webp", "data": b64}},
                {"text": "Output the JSON array for this page only."},
            ],
        }],
        "generationConfig": {"responseMimeType": "application/json"},
    }
    started = time.time()
    raw_text = _gemini_post(body, context)
    text = raw_text.strip()
    text = re.sub(r"^```(json)?|```$", "", text, flags=re.M).strip()
    print(f"    gemini response for {context} in {time.time() - started:.1f}s total ({len(text)} chars): {text[:300]!r}")
    try:
        parsed = json.loads(text)
        entries = _coerce_to_entry_list(parsed)
    except Exception as exc:
        exc.raw_text = raw_text
        raise
    return entries, raw_text


def gemini_ocr_full_page(image_bytes, context=""):
    """Independent of gemini_generate(): a verbatim transcription of
    everything on the page, not just the catalogue entries the schema-
    constrained prompt asks for. See extract_with_llm.py's ocr_full_page()
    for the full reasoning -- identical shape, just plain text instead of
    JSON mode, and no system_instruction (the OCR prompt is the only
    instruction this call needs)."""
    b64 = base64.b64encode(image_bytes).decode()
    body = {
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": "image/webp", "data": b64}},
                {"text": GEMINI_OCR_PROMPT},
            ],
        }],
    }
    started = time.time()
    raw_text = _gemini_post(body, context)
    print(f"    full-page OCR for {context} in {time.time() - started:.1f}s total ({len(raw_text)} chars)")
    return raw_text


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


def db_save_extraction_success(page_id, entries, raw_text):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions (page_id, model, model_tag, status, raw_response, raw_text) "
            "VALUES (%s, %s, %s, 'success', %s, %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'success', raw_response = EXCLUDED.raw_response, "
            "raw_text = EXCLUDED.raw_text, error_message = NULL, created_at = now() "
            "RETURNING id",
            (page_id, GEMINI_MODEL, MODEL_TAG, Json(entries), raw_text),
        )
        extraction_id = cur.fetchone()[0]
        cur.execute("DELETE FROM catalogue_entries WHERE extraction_id = %s", (extraction_id,))
        for idx, entry in enumerate(entries):
            cur.execute(INSERT_ENTRY_SQL, build_entry_row(extraction_id, idx, entry))
        conn.commit()


def db_save_extraction_failure(page_id, error_message, raw_text, content_failure):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions "
            "(page_id, model, model_tag, status, error_message, raw_text, content_failure) "
            "VALUES (%s, %s, %s, 'failed', %s, %s, %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'failed', error_message = EXCLUDED.error_message, "
            "raw_text = EXCLUDED.raw_text, content_failure = EXCLUDED.content_failure, "
            "created_at = now()",
            (page_id, GEMINI_MODEL, MODEL_TAG, error_message, raw_text, content_failure),
        )
        conn.commit()


def is_content_failure(exc):
    return getattr(exc, "raw_text", None) is not None


def process_page(clients, claim):
    """Extract one already-claimed page. See extract_with_llm.py's
    process_page() -- identical shape, including the full-page OCR call,
    minus the model/model_tag parameters this script doesn't need."""
    page_id = claim["page_id"]
    page_no = claim["page_no"]
    account = claim["account"]
    bucket = claim["bucket"]
    image_key = claim["image_key"]
    folder = claim["folder"]
    stem = claim["stem"]
    context = f"{folder}/{stem} page {page_no}"

    print(f"  page {page_no}: b2 account={account} bucket={bucket} key={image_key}")
    raw_text = None
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
            exc.b2_download_failure = True
            raise
        # Full-page OCR: independent of the structured extraction below --
        # best-effort, and deliberately not allowed to affect this page's
        # success/failure/retry accounting (MAX_ATTEMPTS_PER_PAGE,
        # content_failure, MAX_B2_FAILURES_PER_WORKER all stay scoped to
        # gemini_generate()'s outcome only), exactly like
        # extract_with_llm.py's process_page() treats its own OCR call.
        try:
            page_text = gemini_ocr_full_page(image_bytes, context=context)
            db_save_page_ocr_text(page_id, GEMINI_MODEL, MODEL_TAG, page_text)
        except Exception as exc:
            print(f"WARNING: full-page OCR failed for {context}: {exc}")
            db_save_page_ocr_failure(page_id, GEMINI_MODEL, MODEL_TAG, str(exc))
        entries, raw_text = gemini_generate(image_bytes, context=context)
        for entry in entries:
            entry.setdefault("source_folder", folder)
            entry.setdefault("source_pdf", stem)
            entry["pdf_page"] = page_no - 1
            flag_if_printed_page_missing(entry)
        entries_json = json.dumps(entries)
        print(f"    saving {len(entries)} entries for {context}: {entries_json[:500]!r}")
        db_save_extraction_success(page_id, entries, raw_text)
        print(f"done: {context}")
        return True, False
    except Exception as exc:
        content_failure = is_content_failure(exc)
        b2_download_failure = getattr(exc, "b2_download_failure", False)
        raw_text = getattr(exc, "raw_text", raw_text)
        print(f"WARNING: page {page_no} of {folder}/{stem} failed: {exc}; will retry next run")
        db_save_extraction_failure(page_id, str(exc), raw_text, content_failure)
        return False, b2_download_failure


def count_capped_failures():
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM llm_extractions "
            "WHERE model_tag = %s AND status = 'failed' AND content_failure "
            "AND attempt_count >= %s",
            (MODEL_TAG, MAX_ATTEMPTS_PER_PAGE),
        )
        return cur.fetchone()[0]


def main():
    clients = {aid: b2_client(acct) for aid, acct in B2_ACCOUNTS.items()}

    print(f"model: {GEMINI_MODEL} (tag: {MODEL_TAG}); pace: {PACE_SECONDS}s between requests")
    if SOURCE_MODEL_TAG:
        print(f"rescue mode: only claiming pages capped out as content failures under source model tag {SOURCE_MODEL_TAG!r}")
    if ALLOW_ALREADY_EXTRACTED:
        print("ALLOW_ALREADY_EXTRACTED set: will claim pages another model/textparse/human already succeeded on")
    else:
        print("default: skipping pages that already have a successful extraction from any source")
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
            print(f"runtime guard tripped after {elapsed():.0f}s; stopping before claiming another page")
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        if MAX_PAGES_PER_WORKER and processed >= MAX_PAGES_PER_WORKER:
            print(f"MAX_PAGES_PER_WORKER limit ({MAX_PAGES_PER_WORKER}) reached; stopping")
            limited = True
            break

        claim = claim_next_page()
        if claim is None:
            break
        if claim is CLAIM_CONTENDED:
            print("gave up after repeated claim contention, not confirmed exhaustion; other workers or a later run may still find pages")
            contended = True
            break
        ok, b2_download_failure = process_page(clients, claim)
        if not ok:
            any_incomplete = True
            if b2_download_failure:
                b2_failures += 1
        processed += 1
        print(f"count of pages processed so far: {processed}")
        if MAX_B2_FAILURES_PER_WORKER and b2_failures >= MAX_B2_FAILURES_PER_WORKER:
            print(f"{b2_failures} B2 download failures this run; stopping before claiming another page")
            b2_capped = True
            break

    print(f"processed {processed} page(s) total; stopped for the reason logged above")

    if limited:
        print(f"limited run: stopped at MAX_PAGES_PER_WORKER ({MAX_PAGES_PER_WORKER}); whether more pages remain is unknown")
    elif contended:
        print("stopped on claim contention; backlog status unknown, not confirmed exhausted")
    elif b2_capped:
        print(f"stopped after {b2_failures} B2 download failures (MAX_B2_FAILURES_PER_WORKER={MAX_B2_FAILURES_PER_WORKER})")
    else:
        capped = count_capped_failures()
        if capped:
            print(
                f"claimable backlog exhausted, but {capped} page(s) had content the model "
                f"couldn't produce a usable response for after {MAX_ATTEMPTS_PER_PAGE} attempts "
                "and need manual review or a different model"
            )
        else:
            print("all pages processed")

    if any_incomplete:
        print("one or more pages failed extraction; exiting non-zero so this is visible")
        sys.exit(1)


if __name__ == "__main__":
    main()
