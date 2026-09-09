"""Extract catalogue entries from rendered page images using a local vision
LLM served by Ollama, running entirely on the GitHub Actions runner — no
external API calls, no API key. Reads the page images process_pcloud.py
already uploaded to B2 and writes one JSON file per page (following
pipeline/schema.md's entry schema) back to B2.

B2 is the source of truth for resumability, same pattern as
process_pcloud.py: every page's output is checked before being redone, and a
processed/<stem>.done marker is only written once every page for that source
PDF is verified present. Results are namespaced by model (OLLAMA_MODEL,
slugified into MODEL_TAG) so multiple models can be tried against the same
page images without clobbering each other's output — run the workflow once
per model to bake them off against each other.

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
from botocore.exceptions import ClientError

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ["OLLAMA_MODEL"]
MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", OLLAMA_MODEL)

B2_ENDPOINT = os.environ["B2_ENDPOINT"]
if not B2_ENDPOINT.startswith(("http://", "https://")):
    # The B2 console's bucket details page shows the endpoint without a
    # scheme; boto3 requires a full URL.
    B2_ENDPOINT = f"https://{B2_ENDPOINT}"
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_APPLICATION_KEY = os.environ["B2_APPLICATION_KEY"]
B2_BUCKET_NAME = os.environ["B2_BUCKET_NAME"]

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
(printed in a margin, header, or footer) and put it in `printed_page`; leave \
it "" if none is visible. Leave `quarter` as "" — it isn't known for this \
source. Output the JSON array only, no commentary.

SCHEMA:
""" + SCHEMA_PATH.read_text(encoding="utf-8")

IMAGE_KEY_RE = re.compile(r"^images/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.webp$")


def elapsed():
    return time.time() - START_TIME


def b2_client():
    return boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_KEY_ID,
        aws_secret_access_key=B2_APPLICATION_KEY,
    )


def b2_exists(client, key):
    try:
        client.head_object(Bucket=B2_BUCKET_NAME, Key=key)
        return True
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise


def b2_get_bytes(client, key):
    return client.get_object(Bucket=B2_BUCKET_NAME, Key=key)["Body"].read()


def b2_put_bytes(client, key, data, content_type):
    client.put_object(Bucket=B2_BUCKET_NAME, Key=key, Body=data, ContentType=content_type)


def list_page_images(client):
    """Return {(folder, stem): [(page_no, image_key), ...]} for every page
    image process_pcloud.py has uploaded, grouped by source PDF."""
    paginator = client.get_paginator("list_objects_v2")
    by_pdf = {}
    for result in paginator.paginate(Bucket=B2_BUCKET_NAME, Prefix="images/"):
        for obj in result.get("Contents", []):
            m = IMAGE_KEY_RE.match(obj["Key"])
            if not m:
                continue
            key = (m["folder"], m["stem"])
            by_pdf.setdefault(key, []).append((int(m["page"]), obj["Key"]))
    for pages in by_pdf.values():
        pages.sort()
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


def process_pdf(client, folder, stem, page_keys):
    done_key = f"extractions/{MODEL_TAG}/processed/{folder}/{stem}.done"
    if b2_exists(client, done_key):
        print(f"skip (already done): {MODEL_TAG}/{folder}/{stem}")
        return

    print(f"processing: {MODEL_TAG}/{folder}/{stem} ({len(page_keys)} pages)")
    for page_no, image_key in page_keys:
        out_key = f"extractions/{MODEL_TAG}/{folder}/{stem}/page_{page_no:04d}.json"
        if b2_exists(client, out_key):
            continue
        try:
            entries = extract_page(b2_get_bytes(client, image_key))
            for entry in entries:
                entry.setdefault("source_folder", folder)
                entry.setdefault("source_pdf", stem)
            body = json.dumps(entries, ensure_ascii=False, indent=2).encode()
            b2_put_bytes(client, out_key, body, "application/json")
        except Exception as exc:
            print(f"WARNING: page {page_no} of {folder}/{stem} failed: {exc}; will retry next run")

    all_present = all(
        b2_exists(client, f"extractions/{MODEL_TAG}/{folder}/{stem}/page_{page_no:04d}.json")
        for page_no, _ in page_keys
    )
    if all_present:
        b2_put_bytes(client, done_key, f"completed at {time.time()}".encode(), "text/plain")
        print(f"done: {MODEL_TAG}/{folder}/{stem}")
    else:
        print(f"WARNING: not all pages verified for {folder}/{stem}; will retry next run")


def main():
    wait_for_ollama()
    client = b2_client()

    print(f"model: {OLLAMA_MODEL} (tag: {MODEL_TAG})")
    print("listing page images in B2...")
    by_pdf = list_page_images(client)
    total_pages = sum(len(v) for v in by_pdf.values())
    print(f"found {len(by_pdf)} source PDF(s), {total_pages} page(s)")

    for (folder, stem), page_keys in sorted(by_pdf.items()):
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before starting a new file"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        process_pdf(client, folder, stem, page_keys)

    print("all pages processed")


if __name__ == "__main__":
    main()
