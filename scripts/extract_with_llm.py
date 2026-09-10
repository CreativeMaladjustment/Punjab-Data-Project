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
extract-pages.yml's `workers` matrix) without ever claiming the same page:
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


def db_connect():
    return psycopg2.connect(SUPABASE_DB_URL)


CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60  # 3 hours; a worker that dies mid-page leaves
# its claim behind for this long before another worker (or the same one, next
# run) is allowed to pick the page back up.

CLAIM_MAX_ATTEMPTS = 5  # see the "lost the race" note in claim_next_page()

# candidate CTE narrows to one page: uploaded, and either never attempted
# under model_tag, or claimed/failed but stale (claimed_at is set on both a
# fresh claim and a failure, so it doubles as "last touched" either way).
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
CLAIM_NEXT_PAGE_SQL = """
    WITH candidate AS (
        SELECT p.id
        FROM pages p
        LEFT JOIN llm_extractions le
            ON le.page_id = p.id AND le.model_tag = %(model_tag)s
        WHERE p.image_uploaded_at IS NOT NULL
          AND (
            le.id IS NULL
            OR (le.status IN ('claimed', 'failed')
                AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second')
          )
        ORDER BY random()
        LIMIT 1
        FOR UPDATE OF p SKIP LOCKED
    )
    INSERT INTO llm_extractions (page_id, model, model_tag, status, claimed_at)
    SELECT candidate.id, %(model)s, %(model_tag)s, 'claimed', now()
    FROM candidate
    ON CONFLICT (page_id, model_tag) DO UPDATE SET
        status = 'claimed', claimed_at = now(), error_message = NULL, raw_response = NULL
    WHERE llm_extractions.status IN ('claimed', 'failed')
      AND llm_extractions.claimed_at < now() - %(claim_timeout)s * interval '1 second'
    RETURNING page_id
"""


def claim_next_page(conn, model, model_tag, claim_timeout_seconds):
    """Atomically claim one page still needing extraction under model_tag.
    Returns {"page_id", "page_no", "account", "bucket", "image_key",
    "folder", "stem"}, or None if nothing is claimable right now.

    A single attempt can come back empty even when pages *are* still
    available: it narrows to exactly one candidate up front, and if another
    worker's concurrent claim wins the race for that specific page (see the
    module-level SQL comment), this attempt's write affects zero rows --
    indistinguishable, from one query alone, from "nothing left at all".
    Retrying a few times (each picks a fresh random candidate) resolves
    that ambiguity in practice; only genuine exhaustion survives every
    attempt. A worker that gives up after CLAIM_MAX_ATTEMPTS real races in
    a row just exits a little early -- a later run mops up whatever's left,
    never a double-claim or a permanently skipped page.
    """
    for _ in range(CLAIM_MAX_ATTEMPTS):
        with conn.cursor() as cur:
            cur.execute(
                CLAIM_NEXT_PAGE_SQL,
                {"model_tag": model_tag, "claim_timeout": claim_timeout_seconds, "model": model},
            )
            row = cur.fetchone()
        conn.commit()  # releases the row lock candidate took, whether or not it matched
        if row is not None:
            break
    else:
        return None
    page_id = row[0]

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
        },
        timeout=(OLLAMA_CONNECT_TIMEOUT_SECONDS, OLLAMA_READ_TIMEOUT_SECONDS),
    )
    resp.raise_for_status()
    text = resp.json()["response"].strip()
    text = re.sub(r"^```(json)?|```$", "", text, flags=re.M).strip()
    print(
        f"    ollama response for {context} in {time.time() - started:.1f}s "
        f"({len(text)} chars): {text[:300]!r}"
    )
    parsed = json.loads(text)  # fail loudly; the page can be retried next run
    return _coerce_to_entry_list(parsed)


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


def db_save_extraction_success(conn, page_id, model, model_tag, entries):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions (page_id, model, model_tag, status, raw_response) "
            "VALUES (%s, %s, %s, 'success', %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'success', raw_response = EXCLUDED.raw_response, "
            "error_message = NULL "
            "RETURNING id",
            (page_id, model, model_tag, Json(entries)),
        )
        extraction_id = cur.fetchone()[0]
        cur.execute("DELETE FROM catalogue_entries WHERE extraction_id = %s", (extraction_id,))
        for idx, entry in enumerate(entries):
            cur.execute(INSERT_ENTRY_SQL, build_entry_row(extraction_id, idx, entry))
    conn.commit()


def db_save_extraction_failure(conn, page_id, model, model_tag, error_message):
    # If db_save_extraction_success() raised partway through (e.g. a bad
    # catalogue_entries insert), the connection is left in an aborted
    # transaction; rolling back first (a no-op if there's nothing to undo)
    # keeps this write from failing too and taking down the whole run.
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions (page_id, model, model_tag, status, error_message) "
            "VALUES (%s, %s, %s, 'failed', %s) "
            "ON CONFLICT (page_id, model_tag) DO UPDATE SET "
            "status = 'failed', error_message = EXCLUDED.error_message",
            (page_id, model, model_tag, error_message),
        )
    conn.commit()


def process_page(conn, clients, claim):
    """Extract one already-claimed page and record success or failure.
    claim: the dict returned by claim_next_page(). Returns True on
    success, False if it needs a retry."""
    page_id = claim["page_id"]
    page_no = claim["page_no"]
    account = claim["account"]
    bucket = claim["bucket"]
    image_key = claim["image_key"]
    folder = claim["folder"]
    stem = claim["stem"]
    context = f"{folder}/{stem} page {page_no}"

    print(f"  page {page_no}: b2 account={account} bucket={bucket} key={image_key}")
    try:
        try:
            client = clients[account]
        except KeyError:
            raise RuntimeError(
                f"page {folder}/{stem} page_no={page_no} is recorded in account "
                f"{account!r}, but that account isn't configured in this run's secrets"
            )
        image_bytes = b2_get_bytes(client, bucket, image_key)
        entries = extract_page(image_bytes, context=context)
        for entry in entries:
            entry.setdefault("source_folder", folder)
            entry.setdefault("source_pdf", stem)
            entry["pdf_page"] = page_no - 1  # known exactly; don't trust the model's guess
            flag_if_printed_page_missing(entry)
        entries_json = json.dumps(entries)
        print(f"    saving {len(entries)} entries for {context}: {entries_json[:500]!r}")
        db_save_extraction_success(conn, page_id, OLLAMA_MODEL, MODEL_TAG, entries)
        print(f"done: {context}")
        return True
    except Exception as exc:
        print(f"WARNING: page {page_no} of {folder}/{stem} failed: {exc}; will retry next run")
        db_save_extraction_failure(conn, page_id, OLLAMA_MODEL, MODEL_TAG, str(exc))
        return False


def main():
    wait_for_ollama()
    conn = db_connect()
    clients = {aid: b2_client(acct) for aid, acct in B2_ACCOUNTS.items()}

    print(f"model: {OLLAMA_MODEL} (tag: {MODEL_TAG})")
    processed = 0
    any_incomplete = False
    while True:
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before claiming another page"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)

        claim = claim_next_page(conn, OLLAMA_MODEL, MODEL_TAG, CLAIM_TIMEOUT_SECONDS)
        if claim is None:
            break
        if not process_page(conn, clients, claim):
            any_incomplete = True
        processed += 1

    print(f"processed {processed} page(s); no more claimable pages for this worker")

    if any_incomplete:
        print("one or more pages failed extraction; exiting non-zero so this is visible")
        sys.exit(1)

    print("all pages processed")


if __name__ == "__main__":
    main()
