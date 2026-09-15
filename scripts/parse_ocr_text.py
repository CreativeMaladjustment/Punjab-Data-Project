"""Backfill structured catalogue entries from an already-successful full-page
OCR transcription (page_ocr_text — see scripts/extract_with_llm.py's
ocr_full_page() and supabase/migrations/20260912160000_add_page_ocr_text.sql),
for pages that don't yet have a successful *structured* extraction under
PRIMARY_MODEL_TAG. A text-only LLM call (no image, no B2 access at all) reads
the OCR transcription and parses it into pipeline/schema.md-shaped entries,
written into the same llm_extractions/catalogue_entries tables the vision
pipeline uses — see supabase/migrations/20260909140000_init_processing_schema.sql.

Why this exists: extract_page() in extract_with_llm.py sometimes fails on
content it never even gets a clean shot at (a bad JSON envelope, a shape the
coercion logic doesn't recognize -- see _coerce_to_entry_list there), while
the *independent* full-page OCR call for the same page succeeds and holds a
perfectly good verbatim transcription. Most of pipeline/schema.md's fields
(title, gloss, author, publisher, date, price, edition, format, method,
pp_verbatim, section/lang/topic, serial, copies, printer/pcity, educ) are
prose the transcription actually contains and a second, cheaper (text-only,
no image tokens) LLM pass can parse out -- this script is that pass, run
against whatever's missing a real structured extraction rather than only
capped content failures, so it also picks up pages that were simply never
attempted yet.

What this can NEVER recover, and is told explicitly not to guess: `reg`
(registration number) and `copyright` (column 6) aren't present in a
full-page *text* transcription at all -- they're columnar data the OCR pass
apparently doesn't preserve inline. `marks` (pencil crosses, stamps) are
physical annotations on the scan, not printed text. `title_native` can't be
judged reliably unless the transcription itself still carries native-script
characters for the title. `pdf_page` and `source_folder`/`source_pdf` are
filled in by this script from the `pages`/`pcloud_files` rows themselves
(same as extract_with_llm.py's process_page() does), not asked of the model
at all -- they're known exactly, no reason to make the model guess.

Namespaced under model_tag "textparse:<slugified TEXT_MODEL>" -- the colon is
load-bearing, exactly like api/queries.py's HUMAN_MODEL_TAG ("human:review"):
extract_with_llm.py's slugify (`re.sub(r"[^A-Za-z0-9._-]", "-", ...)`) can
never produce a colon, so no real vision-model run can ever collide with this
prefix. The `model` column (free text, not part of the unique key) records
which OCR source this run actually parsed, e.g. "textparse:llama3.1:8b <-
page_ocr_text[glm-ocr]" -- both "how it was processed" and "sourced from"
readable directly off one row, no schema change needed for that.

Runs entirely against Postgres + a local Ollama text model -- no B2 access,
no image bytes, so no B2 credentials/client at all (unlike extract_with_llm.py).
Deliberately does NOT import anything from extract_with_llm.py: that module
does real work at import time (connecting to B2, requiring OLLAMA_MODEL) this
script has no reason to need -- same rationale api/queries.py and
scripts/extraction_status.py already give for redeclaring shared constants
instead of importing them. The JSON-coercion helpers below are a deliberate,
intentional duplicate of extract_with_llm.py's _coerce_to_entry_list() and
friends for the same reason -- keep the two in sync if either changes.
"""
import contextlib
import json
import os
import pathlib
import re
import sys
import time

import psycopg2
import requests
from psycopg2.extras import Json

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
TEXT_MODEL = os.environ["TEXT_MODEL"]
TEXT_MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", TEXT_MODEL)
MODEL_TAG = f"textparse:{TEXT_MODEL_TAG}"

# Mirrors api/queries.py's PRIMARY_MODEL_TAG -- "doesn't have extract" means
# no successful row under this tag specifically, not "no successful row
# under any model". Overridable via env instead of hardcoded in case the
# primary structured-extraction model changes later without a code edit here.
PRIMARY_MODEL_TAG = os.environ.get("PRIMARY_MODEL_TAG", "glm-ocr")

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

MAX_RUNTIME_SECONDS = 18000  # 5 hours; same runner guard as extract_with_llm.py
RUNTIME_GUARD_EXIT_CODE = 42
MAX_PAGES_PER_WORKER = int(os.environ.get("MAX_PAGES_PER_WORKER", "0"))
START_TIME = time.time()

# Text-only calls (no image tokens) are far cheaper than the vision pass, so
# shorter timeouts than extract_with_llm.py's -- still generous for a CPU
# runner under load.
OLLAMA_CONNECT_TIMEOUT_SECONDS = 10
OLLAMA_READ_TIMEOUT_SECONDS = 600
# A dense page's OCR transcription + schema + system prompt can still run
# long in tokens even without image tokens -- same headroom rationale as
# extract_with_llm.py's OLLAMA_NUM_CTX.
OLLAMA_NUM_CTX = int(os.environ.get("OLLAMA_NUM_CTX", "8192"))

SCHEMA_PATH = pathlib.Path(__file__).resolve().parent.parent / "pipeline" / "schema.md"

TEXT_PARSE_SYSTEM_PROMPT = """You parse structured catalogue entries out of a \
verbatim OCR transcription of one scanned page from a British colonial-era \
"Catalogue of Books registered" print register. The transcription below is \
plain text, not the image itself -- parse only what is literally present in \
it; never infer, correct, or complete missing information. Output ONLY a \
JSON array of entry objects following the schema below, one object per \
catalogue entry on the page. If the transcription has no catalog entries \
(cover, blank, title page, index), output [].

Several schema fields describe things a plain-text transcription cannot \
show: `reg` (registration number, a tabular column this transcription does \
not preserve), `copyright` (same column 6), and `marks` (physical pencil/ \
stamp marks on the scan, not printed text). Leave these empty/null on every \
entry -- do not guess or invent a value for any of them. Do not set \
`title_native` to true unless the transcription itself contains native-script \
characters for that title; if you can't tell, leave it null and add a \
`flags` entry noting title_native is unknown from this source. Leave \
`pdf_page` as 0 -- it will be overwritten from known data, not read from you. \
Leave `quarter` as "" unless the transcription's own text states which \
quarter this page belongs to.

Read the printed page number directly from the transcription if it's there \
(a running header/footer number) and put it in `printed_page` as an integer; \
if it isn't in the text, use 0 and add a `flags` entry noting the printed \
page number wasn't present in the transcription. Flag every uncertain \
reading in `flags`. Output the JSON array only, no commentary.

SCHEMA:
""" + SCHEMA_PATH.read_text(encoding="utf-8")


def elapsed():
    return time.time() - START_TIME


DB_CONNECT_MAX_ATTEMPTS = 5  # see extract_with_llm.py's own copy of this retry


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


# Mirrors extract_with_llm.py's CLAIM_TIMEOUT_SECONDS/MAX_ATTEMPTS_PER_PAGE
# exactly -- must stay numerically identical, not just similarly named: the
# dashboard (api/queries.py's EXTRACTION_SUMMARY_SQL, claimed_active/
# claimed_stale split) interprets *every* model_tag's claimed/failed rows
# against these same two numbers, this one included.
CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60
CLAIM_MAX_ATTEMPTS = 5
MAX_ATTEMPTS_PER_PAGE = 2

# Candidate: has a successful page_ocr_text row, has no successful
# PRIMARY_MODEL_TAG structured extraction, isn't excluded (pages.excluded_at
# -- see api/queries.py's apply_page_exclusion()), and this script's own
# model_tag isn't already claimed/succeeded/under-cap-failed on it. Only
# selects a page id here (not which OCR row to use) -- claim_next_ocr_page()
# fetches that separately, deterministically, once the claim itself has
# already committed; see its own docstring for why.
CLAIM_NEXT_OCR_PAGE_SQL = """
    WITH candidate AS (
        SELECT p.id
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND p.excluded_at IS NULL
          AND EXISTS (
              SELECT 1 FROM page_ocr_text pot
              WHERE pot.page_id = p.id AND pot.status = 'success'
          )
          AND NOT EXISTS (
              SELECT 1 FROM llm_extractions primary_le
              WHERE primary_le.page_id = p.id
                AND primary_le.model_tag = %(primary_model_tag)s
                AND primary_le.status = 'success'
          )
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

# Lock-free existence check, same role as extract_with_llm.py's
# PENDING_EXISTS_SQL -- see claim_next_ocr_page() for why a single claim
# attempt coming back empty doesn't yet mean the backlog is exhausted.
PENDING_EXISTS_OCR_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND p.excluded_at IS NULL
          AND EXISTS (
              SELECT 1 FROM page_ocr_text pot
              WHERE pot.page_id = p.id AND pot.status = 'success'
          )
          AND NOT EXISTS (
              SELECT 1 FROM llm_extractions primary_le
              WHERE primary_le.page_id = p.id
                AND primary_le.model_tag = %(primary_model_tag)s
                AND primary_le.status = 'success'
          )
          AND (
            le.id IS NULL
            OR le.status = 'claimed'
            OR (le.status = 'failed'
                AND (NOT le.content_failure OR le.attempt_count < %(max_attempts)s))
          )
    )
"""

CLAIM_CONTENDED = object()  # see extract_with_llm.py's own sentinel of the same name


def claim_next_ocr_page():
    """Atomically claim one page needing a text-parsed structured extraction.
    Returns {"page_id", "page_no", "folder", "stem", "ocr_text",
    "ocr_source_tag"}; None if genuinely exhausted (confirmed by
    PENDING_EXISTS_OCR_SQL); or CLAIM_CONTENDED if that's unconfirmed (a
    lost race, or a momentarily locked row) -- see extract_with_llm.py's
    claim_next_page(), which this mirrors closely.

    The OCR text to actually use is fetched in a second, separate query
    *after* the claim commits -- deliberately not joined into the claim
    query itself, which would let a page with more than one successful
    page_ocr_text row (different OCR models run at different times)
    multiply into more than one CTE candidate row for the same page id,
    making "LIMIT 1" ambiguous about *which* row's text and page id both
    came along together. Preferring PRIMARY_MODEL_TAG's own OCR text when
    it exists, falling back to the most recent successful one otherwise, is
    a deterministic tie-break that has nothing to do with which page got
    picked -- keeping the two concerns (which page; which OCR text for it)
    in separate queries avoids coupling them.
    """
    params = {
        "model_tag": MODEL_TAG,
        "model": f"textparse:{TEXT_MODEL}",
        "primary_model_tag": PRIMARY_MODEL_TAG,
        "claim_timeout": CLAIM_TIMEOUT_SECONDS,
        "max_attempts": MAX_ATTEMPTS_PER_PAGE,
    }
    with db_connection() as conn:
        for _ in range(CLAIM_MAX_ATTEMPTS):
            with conn.cursor() as cur:
                cur.execute(CLAIM_NEXT_OCR_PAGE_SQL, params)
                (page_id,) = cur.fetchone()
            conn.commit()
            if page_id is not None:
                break
        else:
            with conn.cursor() as cur:
                cur.execute(PENDING_EXISTS_OCR_SQL, params)
                (pending_exists,) = cur.fetchone()
            conn.commit()
            return CLAIM_CONTENDED if pending_exists else None

        with conn.cursor() as cur:
            cur.execute(
                "SELECT p.page_no, pf.folder, pf.name FROM pages p "
                "JOIN pcloud_files pf ON pf.pcloud_fileid = p.pcloud_fileid "
                "WHERE p.id = %s",
                (page_id,),
            )
            page_no, folder, name = cur.fetchone()

            cur.execute(
                "SELECT raw_text, model_tag FROM page_ocr_text "
                "WHERE page_id = %(page_id)s AND status = 'success' "
                "ORDER BY (model_tag = %(primary_model_tag)s) DESC, created_at DESC "
                "LIMIT 1",
                {"page_id": page_id, "primary_model_tag": PRIMARY_MODEL_TAG},
            )
            row = cur.fetchone()
        conn.rollback()  # read-only from here; drop the implicit transaction

    if row is None:
        # Genuinely shouldn't happen -- CLAIM_NEXT_OCR_PAGE_SQL only selects
        # pages with a successful page_ocr_text row -- but if the claim and
        # this lookup somehow straddle a concurrent change, treat it as
        # contended rather than crashing the worker on one page.
        return CLAIM_CONTENDED
    ocr_text, ocr_source_tag = row

    return {
        "page_id": page_id,
        "page_no": page_no,
        "folder": folder,
        "stem": pathlib.Path(name).stem,
        "ocr_text": ocr_text,
        "ocr_source_tag": ocr_source_tag,
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
        flags.append({"field": "printed_page", "issue": "not present in the OCR transcription"})
        entry["flags"] = flags
    return entry


# Deliberate duplicate of extract_with_llm.py's ENTRY_FIELD_NAMES -- see this
# module's own docstring for why importing that module isn't done instead.
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
    """Deliberate duplicate of extract_with_llm.py's _coerce_to_entry_list()
    -- see that function's own docstring for the full reasoning behind each
    shape it recovers. Kept in sync by hand; a genuinely new malformed shape
    (like the section-keyed dict this text-parse pass exists partly to work
    around) still raises here with the dict's keys in the message, exactly
    as there."""
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


def parse_page_text(ocr_text, context=""):
    """The text-only analog of extract_with_llm.py's extract_page(): same
    JSON-array-of-entries contract, same _coerce_to_entry_list recovery, but
    the model reads a transcription instead of an image -- no `images` key,
    no vision tokens."""
    started = time.time()
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={
            "model": TEXT_MODEL,
            "system": TEXT_PARSE_SYSTEM_PROMPT,
            "prompt": f"OCR transcription of this page:\n\n{ocr_text}\n\n"
                      "Output the JSON array for this page only.",
            "stream": False,
            "format": "json",
            "options": {"num_ctx": OLLAMA_NUM_CTX},
        },
        timeout=(OLLAMA_CONNECT_TIMEOUT_SECONDS, OLLAMA_READ_TIMEOUT_SECONDS),
    )
    if not resp.ok:
        raise RuntimeError(f"Ollama /api/generate returned {resp.status_code}: {resp.text[:2000]}")
    raw_text = resp.json()["response"]
    text = raw_text.strip()
    text = re.sub(r"^```(json)?|```$", "", text, flags=re.M).strip()
    print(f"    ollama response for {context} in {time.time() - started:.1f}s ({len(text)} chars): {text[:300]!r}")
    try:
        parsed = json.loads(text)
        entries = _coerce_to_entry_list(parsed)
    except Exception as exc:
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
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions (page_id, model, model_tag, status, raw_response, raw_text) "
            "VALUES (%s, %s, %s, 'success', %s, %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'success', raw_response = EXCLUDED.raw_response, "
            "raw_text = EXCLUDED.raw_text, error_message = NULL, created_at = now() "
            "RETURNING id",
            (page_id, model, model_tag, Json(entries), raw_text),
        )
        extraction_id = cur.fetchone()[0]
        cur.execute("DELETE FROM catalogue_entries WHERE extraction_id = %s", (extraction_id,))
        for idx, entry in enumerate(entries):
            cur.execute(INSERT_ENTRY_SQL, build_entry_row(extraction_id, idx, entry))
        conn.commit()


def db_save_extraction_failure(page_id, model, model_tag, error_message, raw_text, content_failure):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions "
            "(page_id, model, model_tag, status, error_message, raw_text, content_failure) "
            "VALUES (%s, %s, %s, 'failed', %s, %s, %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'failed', error_message = EXCLUDED.error_message, "
            "raw_text = EXCLUDED.raw_text, content_failure = EXCLUDED.content_failure, "
            "created_at = now()",
            (page_id, model, model_tag, error_message, raw_text, content_failure),
        )
        conn.commit()


def is_content_failure(exc):
    """See extract_with_llm.py's is_content_failure() -- same distinction,
    same reasoning: parse_page_text() only attaches raw_text to exceptions
    it raises itself, after a real model response came back."""
    return getattr(exc, "raw_text", None) is not None


def process_ocr_page(claim):
    """Parse one already-claimed page's OCR text and record success or
    failure. Much simpler than extract_with_llm.py's process_page(): no
    image fetch, no B2, so no download-failure bookkeeping either -- the
    only way this fails is the model call itself or a bad DB write."""
    page_id = claim["page_id"]
    page_no = claim["page_no"]
    folder = claim["folder"]
    stem = claim["stem"]
    context = f"{folder}/{stem} page {page_no} (from {claim['ocr_source_tag']} OCR)"

    print(f"  page {page_no}: parsing OCR text from {claim['ocr_source_tag']} ({len(claim['ocr_text'])} chars)")
    raw_text = None
    try:
        entries, raw_text = parse_page_text(claim["ocr_text"], context=context)
        for entry in entries:
            entry.setdefault("source_folder", folder)
            entry.setdefault("source_pdf", stem)
            entry["pdf_page"] = page_no - 1  # known exactly from `pages`, not asked of the model
            flag_if_printed_page_missing(entry)
        entries_json = json.dumps(entries)
        print(f"    saving {len(entries)} entries for {context}: {entries_json[:500]!r}")
        db_save_extraction_success(
            page_id, f"textparse:{TEXT_MODEL} <- page_ocr_text[{claim['ocr_source_tag']}]", MODEL_TAG, entries, raw_text
        )
        print(f"done: {context}")
        return True
    except Exception as exc:
        content_failure = is_content_failure(exc)
        raw_text = getattr(exc, "raw_text", raw_text)
        print(f"WARNING: page {page_no} of {folder}/{stem} failed: {exc}; will retry next run")
        db_save_extraction_failure(
            page_id,
            f"textparse:{TEXT_MODEL} <- page_ocr_text[{claim['ocr_source_tag']}]",
            MODEL_TAG,
            str(exc),
            raw_text,
            content_failure,
        )
        return False


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
    wait_for_ollama()
    print(f"text model: {TEXT_MODEL} (tag: {MODEL_TAG}); primary model tag: {PRIMARY_MODEL_TAG}")
    if MAX_PAGES_PER_WORKER:
        print(f"MAX_PAGES_PER_WORKER set: stopping after {MAX_PAGES_PER_WORKER} page(s)")

    processed = 0
    any_incomplete = False
    limited = False
    contended = False
    while True:
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(f"runtime guard tripped after {elapsed():.0f}s; stopping before claiming another page")
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        if MAX_PAGES_PER_WORKER and processed >= MAX_PAGES_PER_WORKER:
            print(f"MAX_PAGES_PER_WORKER limit ({MAX_PAGES_PER_WORKER}) reached; stopping")
            limited = True
            break

        claim = claim_next_ocr_page()
        if claim is None:
            break
        if claim is CLAIM_CONTENDED:
            print("gave up after repeated claim contention, not confirmed exhaustion; other workers or a later run may still find pages")
            contended = True
            break
        ok = process_ocr_page(claim)
        if not ok:
            any_incomplete = True
        processed += 1
        print(f"count of pages processed so far: {processed}")

    print(f"processed {processed} page(s) total; stopped for the reason logged above")

    if limited:
        print(f"limited run: stopped at MAX_PAGES_PER_WORKER ({MAX_PAGES_PER_WORKER}); whether more pages remain is unknown")
    elif contended:
        print("stopped on claim contention; backlog status unknown, not confirmed exhausted")
    else:
        capped = count_capped_failures()
        if capped:
            print(
                f"claimable backlog exhausted, but {capped} page(s) had content the model "
                f"couldn't produce a usable response for after {MAX_ATTEMPTS_PER_PAGE} attempts "
                "and need manual review or a different text model"
            )
        else:
            print("all pages processed")

    if any_incomplete:
        print("one or more pages failed text-parse extraction; exiting non-zero so this is visible")
        sys.exit(1)


if __name__ == "__main__":
    main()
