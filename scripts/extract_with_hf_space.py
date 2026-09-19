"""Extract catalogue entries from rendered page images by calling the
Hugging Face Space at floutenvy/Punjab-Data-Project (hf_space/app.py),
running on that Space's own free ZeroGPU hardware -- a fourth extraction
peer alongside the local Ollama models (extract_with_llm.py), Gemini
(extract_with_gemini.py), and HF Inference Providers
(extract_with_hf.py). See hf_space/app.py's module docstring for what
the Space itself does; this script is the GitHub Actions side that
claims a page, calls the Space, and writes the result to Postgres --
the Space has no B2 or Postgres credentials of its own by design.

Namespaced under a fixed MODEL_TAG ("hf-space-zerogpu"), not one derived
from a model name the way the other three scripts' tags are: the model
actually running is a property of the Space's own code
(hf_space/app.py), not something this script chooses per-call, so
there's nothing here to slugify. If the Space's model changes, this tag
stays the same -- it identifies "this pipeline", not "this specific
model".

Why this differs in shape from the other three extraction scripts:

  - One call per page, not two: hf_space/app.py's extract() endpoint
    already bundles the full-page OCR pass and the structured extraction
    into a single Gradio call (two GPU-metered calls happen inside the
    Space, but this script only makes one outbound request). Returns
    both entries and ocr_text/ocr_error in one response.
  - No rate-limit or dollar-credit handling here the way Gemini/HF
    Inference Providers need: ZeroGPU's constraint is a daily GPU-second
    quota enforced *inside* the Space itself, not something this script
    can see or react to specially -- if the quota is exhausted, the
    Space call will simply fail or queue, and that failure is treated
    the same as any other transient error (retried next run), same
    convention as everything else. This hasn't been observed in
    practice yet, so the exact failure shape (timeout? a specific
    Gradio/queue error?) isn't confirmed -- see MAX_PAGES_PER_WORKER's
    default below.
  - Single worker, deliberately, same reasoning as the other two hosted
    pipelines: parallel callers would all draw against the SAME Space's
    shared daily ZeroGPU quota, so more workers doesn't mean more pages,
    just faster quota exhaustion.
  - MAX_PAGES_PER_WORKER defaults to 1 (not the other scripts' higher
    caps or unlimited): real GPU-seconds-per-call isn't measured yet, so
    this starts deliberately small -- literally "submit one image" --
    rather than assuming a throughput number and being wrong about it
    the way this project's model picks already have been twice this
    session (Gemini's retired default, HF Inference Providers' unhosted
    default). Raise it once a real run shows how many pages a day's
    quota actually supports.

Default candidate selection, rescue mode (SOURCE_MODEL), and
ALLOW_ALREADY_EXTRACTED all work exactly like the other three scripts'
own -- see their docstrings for the full reasoning; duplicated here
rather than imported for the same reason they duplicate each other's
copies.

UNVERIFIED as of this writing: this hasn't made a real call against the
live Space yet, so the exact gradio_client API usage below (how an
Image-typed input is actually passed, whether token= is the right
Client() kwarg for a private Space, what a ZeroGPU-quota-exhausted
response actually looks like) are all things to confirm on first run.
"""
import contextlib
import json
import os
import pathlib
import re
import sys
import tempfile
import time

import boto3
import psycopg2
from botocore.config import Config
from gradio_client import Client, handle_file
from psycopg2.extras import Json

HUGGING_FACE_API_KEY = os.environ["HUGGING_FACE_API_KEY"]
HF_SPACE_ID = os.environ.get("HF_SPACE_ID", "floutenvy/Punjab-Data-Project")

MODEL = "HF Space (ZeroGPU): floutenvy/Punjab-Data-Project"
MODEL_TAG = "hf-space-zerogpu"

# See extract_with_llm.py's own SOURCE_MODEL/rescue-mode comment -- same
# mechanic, provider-agnostic: model_tag alone decides what's rescuable, so
# this can rescue an Ollama/Gemini/HF-Inference-Providers model's capped
# content failures exactly the same way.
SOURCE_MODEL = os.environ.get("SOURCE_MODEL", "").strip()
if SOURCE_MODEL == "(none)":
    SOURCE_MODEL = ""
SOURCE_MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", SOURCE_MODEL) if SOURCE_MODEL else None
if SOURCE_MODEL_TAG is not None and SOURCE_MODEL_TAG == MODEL_TAG:
    raise ValueError(
        f"SOURCE_MODEL ({SOURCE_MODEL!r}) slugifies to the same tag as MODEL_TAG "
        f"({MODEL_TAG!r}) -- this pipeline can't rescue its own capped content "
        "failures under its own tag"
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

MAX_RUNTIME_SECONDS = 7200  # 2 hours -- same reasoning as extract_with_hf.py's
# own guard: no evidence yet that a longer window buys anything here, and
# MAX_PAGES_PER_WORKER=1 by default means most runs finish in one call anyway.
RUNTIME_GUARD_EXIT_CODE = 42
# Deliberately 1, not a larger cap -- see the module docstring's "why this
# differs" section. Override via MAX_PAGES_PER_WORKER once a real run has
# shown how many pages a day's ZeroGPU quota actually supports.
MAX_PAGES_PER_WORKER = int(os.environ.get("MAX_PAGES_PER_WORKER", "1"))
START_TIME = time.time()

HF_SPACE_CALL_TIMEOUT_SECONDS = int(os.environ.get("HF_SPACE_CALL_TIMEOUT_SECONDS", "600"))
# Generous -- a cold Space call can include ZeroGPU queueing time on top of
# two model forward passes (OCR + extraction), neither of which has been
# timed yet against the live Space.


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


def db_save_page_ocr_text(page_id, raw_text):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO page_ocr_text (page_id, model, model_tag, status, raw_text) "
            "VALUES (%s, %s, %s, 'success', %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'success', raw_text = EXCLUDED.raw_text, error_message = NULL",
            (page_id, MODEL, MODEL_TAG, raw_text),
        )
        conn.commit()


def db_save_page_ocr_failure(page_id, error_message):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO page_ocr_text (page_id, model, model_tag, status, error_message) "
            "VALUES (%s, %s, %s, 'failed', %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'failed', error_message = EXCLUDED.error_message",
            (page_id, MODEL, MODEL_TAG, error_message),
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

# See extract_with_gemini.py's CLAIM_NEXT_PAGE_SQL/PENDING_EXISTS_SQL
# comment for why these are fully static string literals with the
# conditionals baked in as bind-parameter short-circuits rather than any
# dynamic string construction (Bandit B608).
CLAIM_NEXT_PAGE_SQL = """
    WITH candidate AS (
        SELECT p.id
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND p.excluded_at IS NULL
          AND (
            (le.id IS NULL
             AND (
               %(allow_already_extracted)s
               OR NOT EXISTS (
                 SELECT 1 FROM llm_extractions any_le
                 WHERE any_le.page_id = p.id AND any_le.status = 'success'
               )
             ))
            OR (le.status = 'claimed'
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second')
            OR (le.status = 'failed'
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
          AND (
            %(source_model_tag)s IS NULL
            OR EXISTS (
              SELECT 1 FROM llm_extractions src
              WHERE src.page_id = p.id
                AND src.model_tag = %(source_model_tag)s
                AND src.status = 'failed'
                AND src.content_failure
                AND src.attempt_count >= %(max_attempts)s
            )
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

# See CLAIM_NEXT_PAGE_SQL's own comment just above -- same fully-static,
# bind-parameter-only construction, same two conditionals baked in the
# same way.
PENDING_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND p.excluded_at IS NULL
          AND (
            (le.id IS NULL
             AND (
               %(allow_already_extracted)s
               OR NOT EXISTS (
                 SELECT 1 FROM llm_extractions any_le
                 WHERE any_le.page_id = p.id AND any_le.status = 'success'
               )
             ))
            OR le.status = 'claimed'
            OR (le.status = 'failed'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
          AND (
            %(source_model_tag)s IS NULL
            OR EXISTS (
              SELECT 1 FROM llm_extractions src
              WHERE src.page_id = p.id
                AND src.model_tag = %(source_model_tag)s
                AND src.status = 'failed'
                AND src.content_failure
                AND src.attempt_count >= %(max_attempts)s
            )
          )
    )
"""

CLAIM_CONTENDED = object()


def claim_next_page():
    """Atomically claim one page still needing extraction under MODEL_TAG.
    See extract_with_llm.py's claim_next_page() for the full reasoning --
    identical shape, just no `model`/`model_tag` parameters since this
    script only ever runs against the one fixed MODEL_TAG above."""
    params = {
        "model_tag": MODEL_TAG,
        "model": MODEL,
        "claim_timeout": CLAIM_TIMEOUT_SECONDS,
        "max_attempts": MAX_ATTEMPTS_PER_PAGE,
        "source_model_tag": SOURCE_MODEL_TAG,
        "allow_already_extracted": ALLOW_ALREADY_EXTRACTED,
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


_hf_space_client = None


def hf_space_client():
    """Lazily construct the gradio_client.Client -- module-level so it's
    reused across pages in the same run (avoids re-handshaking with the
    Space on every single call) rather than rebuilt inside process_page().
    token= is this account's HF token, needed because the Space is
    private (see hf_space/README.md)."""
    global _hf_space_client
    if _hf_space_client is None:
        _hf_space_client = Client(HF_SPACE_ID, token=HUGGING_FACE_API_KEY)
    return _hf_space_client


def call_hf_space(image_bytes, context=""):
    """Calls the Space's extract() endpoint with one page image. Returns
    the parsed response dict directly (the Space already returns JSON --
    gradio_client deserializes it to a native Python dict) -- either
    {"entries": [...], "raw_text": ..., "ocr_text": ..., "ocr_error": ...}
    or {"error": ..., "raw_text": ..., "ocr_text": ..., "ocr_error": ...}.
    Writes the image to a temp file first: gradio_client's Image-input
    convention (handle_file()) expects a path or URL, not raw bytes.

    Uses .submit() + Job.result(timeout=...) rather than the simpler
    .predict() -- .predict() blocks with no way to bound how long it
    waits, so a queued/hung Space call (a Copilot review finding on this
    PR) would hang the whole worker instead of the failure being recorded
    and the page retried next run. Job.result()'s timeout raises
    concurrent.futures.TimeoutError, which process_page()'s generic
    except Exception below catches the same as any other failure."""
    with tempfile.NamedTemporaryFile(suffix=".webp") as tmp:
        tmp.write(image_bytes)
        tmp.flush()
        started = time.time()
        job = hf_space_client().submit(
            handle_file(tmp.name),
            api_name="/predict",
        )
        result = job.result(timeout=HF_SPACE_CALL_TIMEOUT_SECONDS)
        print(f"    HF Space response for {context} in {time.time() - started:.1f}s total")
    return result


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
            (page_id, MODEL, MODEL_TAG, Json(entries), raw_text),
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
            (page_id, MODEL, MODEL_TAG, error_message, raw_text, content_failure),
        )
        conn.commit()


def process_page(clients, claim):
    """Extract one already-claimed page. Unlike the other three scripts'
    process_page(), only ONE outbound call happens here (call_hf_space())
    -- the Space itself makes the two GPU-metered model calls internally
    and returns both results in one response. OCR is still independent
    and best-effort: an ocr_error in the response is saved as a
    page_ocr_text failure but never affects the returned entries or this
    page's overall success/failure, same convention as the other three
    scripts' own OCR call."""
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

        result = call_hf_space(image_bytes, context=context)
        if not isinstance(result, dict):
            raise RuntimeError(f"HF Space returned an unexpected response shape: {result!r}")

        ocr_text = result.get("ocr_text")
        ocr_error = result.get("ocr_error")
        if ocr_text:
            db_save_page_ocr_text(page_id, ocr_text)
        elif ocr_error:
            print(f"WARNING: full-page OCR failed for {context}: {ocr_error}")
            db_save_page_ocr_failure(page_id, ocr_error)

        raw_text = result.get("raw_text")
        if "error" in result:
            exc = RuntimeError(f"HF Space extraction failed: {result['error']}")
            exc.raw_text = raw_text
            raise exc

        entries = result.get("entries")
        if not isinstance(entries, list):
            raise RuntimeError(f"HF Space response had no usable 'entries' list: {result!r}")

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
        content_failure = getattr(exc, "raw_text", None) is not None
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

    print(f"HF Space: {HF_SPACE_ID} (tag: {MODEL_TAG})")
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
