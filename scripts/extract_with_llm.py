"""Extract catalogue entries from rendered page images using a local vision
LLM served by Ollama, running entirely on the GitHub Actions runner — no
external API calls, no API key. Reads the page images process_pcloud.py
already uploaded to B2 and writes one JSON file per page (following
pipeline/schema.md's entry schema) back to B2.

B2 is the source of truth for resumability, same pattern as
process_pcloud.py: existing output keys are listed *once* per run (cheap
"Class C" ListObjectsV2 calls) rather than checked individually with
HeadObject per page — B2's free tier caps "Class B" transactions (which
HeadObject bills as) at 2,500/day, and a per-page-HEAD idiom burns through
that almost immediately at this scale. A
extractions/<MODEL_TAG>/processed/<folder>/<stem>.done marker is only written
once every page for that source PDF is confirmed present. Results are
namespaced by model (OLLAMA_MODEL,
slugified into MODEL_TAG) so multiple models can be tried against the same
page images without clobbering each other's output — run the workflow once
per model to bake them off against each other.

Note that extracting each page still costs one real B2 download (GetObject,
genuinely "Class B" — fetching the image bytes to send to Ollama isn't
avoidable) per not-yet-processed page, so a corpus with more than ~2,500
not-yet-extracted pages will still need multiple days/resumed runs against a
free-tier B2 account regardless of this optimization.

Supports up to two B2 accounts/buckets, same as process_pcloud.py (see
load_b2_accounts) — needed because process_pcloud.py may have written source
images to either account depending on which was active when. Source images
and already-done extraction outputs are looked up across ALL configured
accounts; all *new* writes go to a single "active" account
(B2_ACTIVE_ACCOUNT) only. Unlike process_pcloud.py, dedup here is granted at
the page level across accounts (not just per-PDF): skipping an
already-extracted page wherever it landed is worth the small inconsistency
of one PDF's pages potentially ending up split across two accounts, because
redoing an LLM extraction is far more expensive than process_pcloud.py's
redo cost (re-rendering an image locally).

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
import requests

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ["OLLAMA_MODEL"]
MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", OLLAMA_MODEL)


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
B2_ACTIVE_ACCOUNT = os.environ.get("B2_ACTIVE_ACCOUNT", "1").strip() or "1"
if B2_ACTIVE_ACCOUNT not in B2_ACCOUNTS:
    raise RuntimeError(
        f"B2_ACTIVE_ACCOUNT={B2_ACTIVE_ACCOUNT!r} is not a configured B2 account "
        f"(configured: {sorted(B2_ACCOUNTS)})"
    )

MAX_RUNTIME_SECONDS = 18000  # 5 hours; runner guard, exit 42 to hand off to a fresh run
RUNTIME_GUARD_EXIT_CODE = 42
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

IMAGE_KEY_RE = re.compile(r"^images/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.webp$")


def elapsed():
    return time.time() - START_TIME


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
    )


def list_existing_keys(client, bucket, prefix):
    """Return every existing object key under prefix in a handful of B2
    "Class C" list transactions, rather than one "Class B" HeadObject call
    per key. B2's free tier caps Class B at 2,500/day; checking existence
    per-page via HeadObject burns through that almost immediately at this
    scale (thousands of pages), while ListObjectsV2 handles up to 1000 keys
    per call and is billed in the much cheaper class.
    """
    paginator = client.get_paginator("list_objects_v2")
    keys = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def b2_get_bytes(client, bucket, key):
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def b2_put_bytes(client, bucket, key, data, content_type):
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def list_page_images(clients_and_buckets):
    """Return {(folder, stem): [(page_no, client, bucket, image_key), ...]}
    for every page image found in ANY configured B2 account, grouped by
    source PDF — process_pcloud.py may have written images to either
    account depending on which was active at the time, so this has to look
    across all of them to find everything there is to extract."""
    by_pdf = {}
    for client, bucket in clients_and_buckets:
        paginator = client.get_paginator("list_objects_v2")
        for result in paginator.paginate(Bucket=bucket, Prefix="images/"):
            for obj in result.get("Contents", []):
                m = IMAGE_KEY_RE.match(obj["Key"])
                if not m:
                    continue
                key = (m["folder"], m["stem"])
                by_pdf.setdefault(key, []).append((int(m["page"]), client, bucket, obj["Key"]))
    for pages in by_pdf.values():
        pages.sort(key=lambda p: p[0])
    return by_pdf


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


def normalize_entry(entry, folder, stem):
    """Enforce the schema's int type for printed_page regardless of what the
    model actually emitted (it may ignore the prompt's instructions), so
    downstream code doing int(printed_page) (e.g. postprocess.py) never
    breaks on a stray "" or None."""
    entry.setdefault("source_folder", folder)
    entry.setdefault("source_pdf", stem)
    printed_page = entry.get("printed_page")
    if isinstance(printed_page, str):
        printed_page = printed_page.strip()
    if isinstance(printed_page, int) and not isinstance(printed_page, bool):
        return entry
    if isinstance(printed_page, str) and printed_page.isdigit():
        entry["printed_page"] = int(printed_page)
        return entry
    entry["printed_page"] = 0
    entry.setdefault("flags", [])
    entry["flags"].append({"field": "printed_page", "issue": "not visible or unparsable on page"})
    return entry


def extract_page(image_bytes):
    b64 = base64.b64encode(image_bytes).decode()
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
        timeout=600,
    )
    resp.raise_for_status()
    text = resp.json()["response"].strip()
    text = re.sub(r"^```(json)?|```$", "", text, flags=re.M).strip()
    entries = json.loads(text)  # fail loudly; the page can be retried next run
    if not isinstance(entries, list):
        raise ValueError(f"expected a JSON array, got {type(entries).__name__}")
    return entries


def process_pdf(write_client, write_bucket, folder, stem, page_keys, existing):
    """page_keys: [(page_no, source_client, source_bucket, image_key), ...] —
    each page's source image may live in a different configured B2 account
    than the one this run is writing to. `existing` is the union of
    extraction output keys across ALL configured accounts (mutated in place
    as outputs are confirmed), so a page or PDF already done in ANY account
    is skipped; every new write goes to write_client/write_bucket (the
    active account) only. Returns True once every page for this PDF is
    confirmed present somewhere; False if any page still needs a retry.
    """
    done_key = f"extractions/{MODEL_TAG}/processed/{folder}/{stem}.done"
    if done_key in existing:
        print(f"skip (already done): {MODEL_TAG}/{folder}/{stem}")
        return True

    print(f"processing: {MODEL_TAG}/{folder}/{stem} ({len(page_keys)} pages)")
    all_ok = True
    for page_no, source_client, source_bucket, image_key in page_keys:
        out_key = f"extractions/{MODEL_TAG}/{folder}/{stem}/page_{page_no:04d}.json"
        if out_key in existing:
            continue
        try:
            entries = extract_page(b2_get_bytes(source_client, source_bucket, image_key))
            entries = [normalize_entry(e, folder, stem) for e in entries]
            body = json.dumps(entries, ensure_ascii=False, indent=2).encode()
            b2_put_bytes(write_client, write_bucket, out_key, body, "application/json")
            existing.add(out_key)
        except Exception as exc:
            print(f"WARNING: page {page_no} of {folder}/{stem} failed: {exc}; will retry next run")
            all_ok = False

    if all_ok:
        b2_put_bytes(write_client, write_bucket, done_key, f"completed at {time.time()}".encode(), "text/plain")
        existing.add(done_key)
        print(f"done: {MODEL_TAG}/{folder}/{stem}")
    else:
        print(f"WARNING: not all pages verified for {folder}/{stem}; will retry next run")
    return all_ok


def main():
    wait_for_ollama()
    clients = {aid: b2_client(acct) for aid, acct in B2_ACCOUNTS.items()}
    write_client = clients[B2_ACTIVE_ACCOUNT]
    write_bucket = B2_ACCOUNTS[B2_ACTIVE_ACCOUNT]["bucket"]
    clients_and_buckets = [(clients[aid], acct["bucket"]) for aid, acct in B2_ACCOUNTS.items()]

    print(f"model: {OLLAMA_MODEL} (tag: {MODEL_TAG})")
    print(f"listing page images across {len(B2_ACCOUNTS)} configured B2 account(s)...")
    by_pdf = list_page_images(clients_and_buckets)
    total_pages = sum(len(v) for v in by_pdf.values())
    print(f"found {len(by_pdf)} source PDF(s), {total_pages} page(s)")

    print(f"listing existing extractions for {MODEL_TAG} across all configured B2 account(s)...")
    existing = set()
    for client, bucket in clients_and_buckets:
        existing |= list_existing_keys(client, bucket, f"extractions/{MODEL_TAG}/")
    print(f"found {len(existing)} existing extraction object(s)")

    any_incomplete = False
    for (folder, stem), page_keys in sorted(by_pdf.items()):
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before starting a new file"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        if not process_pdf(write_client, write_bucket, folder, stem, page_keys, existing):
            any_incomplete = True

    if any_incomplete:
        print("one or more PDFs had pages that could not be verified; exiting non-zero so this is visible")
        sys.exit(1)

    print("all pages processed")


if __name__ == "__main__":
    main()
