"""Download PDFs from a public pCloud folder, render each page as a
vision-optimized WebP image, and upload it to Backblaze B2. Optionally (see
UPLOAD_PAGE_PDFS) also splits out and uploads a single-page PDF per page.

B2 is the source of truth for resumability: existing objects are listed
*once* per run (a handful of cheap "Class C" ListObjectsV2 calls) rather than
checked individually with HeadObject per page — B2's free tier caps "Class B"
transactions (which HeadObject bills as) at 2,500/day, and a per-page-HEAD
idiom burns through that almost immediately at this scale. A killed or
re-dispatched run just continues where it left off. Designed to run as-is
inside GitHub Actions (see .github/workflows/process-pdfs.yml) but only needs
boto3/requests/pypdf/pdf2image/pillow and network access to run anywhere.

B2 exposes an S3-compatible API, so the storage side of this script talks to
it with the ordinary boto3 "s3" client pointed at the bucket's B2 endpoint.
"""
import io
import os
import sys
import time
import pathlib
import tempfile

import boto3
import requests
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path

PCLOUD_CODE = os.environ["PCLOUD_CODE"]  # the pCloud public-link share code
PCLOUD_HOSTS = ["api.pcloud.com", "eapi.pcloud.com"]

B2_ENDPOINT = os.environ["B2_ENDPOINT"]  # e.g. https://s3.us-west-004.backblazeb2.com
if not B2_ENDPOINT.startswith(("http://", "https://")):
    # The B2 console's bucket details page shows the endpoint without a
    # scheme (e.g. "s3.us-west-004.backblazeb2.com"), which is easy to paste
    # as-is; boto3 requires a full URL.
    B2_ENDPOINT = f"https://{B2_ENDPOINT}"
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_APPLICATION_KEY = os.environ["B2_APPLICATION_KEY"]
B2_BUCKET_NAME = os.environ["B2_BUCKET_NAME"]

TMP_DIR = pathlib.Path(os.environ.get("PCLOUD_TMPDIR") or tempfile.mkdtemp(prefix="pcloud_work_"))
IMAGE_DPI = 200
IMAGE_QUALITY = 85
# The single-page PDFs aren't currently used by anything downstream (only the
# WebP images are fed to the LLM extraction stage) and roughly double both
# runtime and B2 storage for no present benefit. Off by default; flip
# UPLOAD_PAGE_PDFS=true to bring them back if something needs them later.
UPLOAD_PAGE_PDFS = os.environ.get("UPLOAD_PAGE_PDFS", "").strip().lower() in ("1", "true", "yes")
MAX_RUNTIME_SECONDS = 18000  # 5 hours; runner guard, exit 42 to hand off to a fresh run
RUNTIME_GUARD_EXIT_CODE = 42

START_TIME = time.time()


def elapsed():
    return time.time() - START_TIME


def pcloud_get(path, params):
    """GET a pCloud public-API endpoint, falling back to eapi.pcloud.com."""
    last_error = None
    for host in PCLOUD_HOSTS:
        try:
            resp = requests.get(f"https://{host}/{path}", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            continue
        if data.get("result") != 0:
            last_error = RuntimeError(
                f"pCloud API error on {host}/{path}: {data}"
            )
            continue
        return data
    raise RuntimeError(f"pCloud API request failed on all hosts: {last_error}")


MAX_FOLDER_DEPTH = 50


def list_pdfs_recursive(code):
    """Walk the public link's folder tree and return every PDF found.

    Each entry is {"fileid": int, "name": str, "folder": str} where "folder"
    is the "/"-joined path of subfolder names the file lives under (relative
    to the shared link's root).

    Requests the tree with recursive=1 so pCloud embeds each subfolder's
    contents inline in one response. Falls back to a per-folder showpublink
    call (folderid=...) only for a folder that comes back without embedded
    contents — tracking visited folder ids and capping depth so a folderid
    the public-link API doesn't actually scope (it may just re-return the
    root every time) can't recurse forever instead of erroring clearly.
    """
    pdfs = []
    visited_folderids = set()

    def walk(entries, folder_path, depth):
        if depth > MAX_FOLDER_DEPTH:
            raise RuntimeError(
                f"pCloud folder tree exceeded max depth at {'/'.join(folder_path)!r}"
            )
        for entry in entries:
            if entry.get("isfolder"):
                sub_path = folder_path + [entry["name"]]
                if "contents" in entry:
                    walk(entry["contents"], sub_path, depth + 1)
                    continue
                folderid = entry["folderid"]
                if folderid in visited_folderids:
                    continue
                visited_folderids.add(folderid)
                data = pcloud_get("showpublink", {"code": code, "folderid": folderid})
                walk(data["metadata"].get("contents", []), sub_path, depth + 1)
            elif entry["name"].lower().endswith(".pdf"):
                pdfs.append(
                    {
                        "fileid": entry["fileid"],
                        "name": entry["name"],
                        "folder": "/".join(folder_path),
                    }
                )

    root = pcloud_get("showpublink", {"code": code, "recursive": 1})
    walk(root["metadata"].get("contents", []), [], 0)
    return pdfs


def pcloud_download_url(code, fileid):
    data = pcloud_get("getpublinkdownload", {"code": code, "fileid": fileid})
    return f"https://{data['hosts'][0]}{data['path']}"


def download_to(url, dest_path):
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def b2_client():
    return boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_KEY_ID,
        aws_secret_access_key=B2_APPLICATION_KEY,
    )


def list_existing_keys(client, prefix):
    """Return every existing object key under prefix in a handful of B2
    "Class C" list transactions, rather than one "Class B" HeadObject call
    per key. B2's free tier caps Class B at 2,500/day; checking existence
    per-page via HeadObject burns through that almost immediately at this
    scale (thousands of pages), while ListObjectsV2 handles up to 1000 keys
    per call and is billed in the much cheaper class.
    """
    paginator = client.get_paginator("list_objects_v2")
    keys = set()
    for page in paginator.paginate(Bucket=B2_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def b2_put_file(client, key, path, content_type):
    with open(path, "rb") as f:
        client.put_object(Bucket=B2_BUCKET_NAME, Key=key, Body=f, ContentType=content_type)


def b2_put_bytes(client, key, data, content_type="text/plain"):
    client.put_object(Bucket=B2_BUCKET_NAME, Key=key, Body=data, ContentType=content_type)


def split_and_upload_pages(client, pdf_path, folder, stem, work_dir, existing):
    """Render each page of pdf_path as a WebP image (and, if UPLOAD_PAGE_PDFS
    is set, also split out a single-page PDF), uploading each to B2 (skipping
    any key already in `existing`, mutated in place as uploads succeed).
    Images are rendered directly from the source PDF via pdf2image's
    first_page/last_page, with no per-page PDF needed as an intermediate.
    Returns True only if every page uploaded successfully this run (a
    put_object that doesn't raise is treated as confirmed; there's no
    separate HeadObject re-verification pass — see list_existing_keys).
    """
    reader = PdfReader(str(pdf_path))
    page_count = len(reader.pages)
    all_ok = True

    for idx in range(page_count):
        page_no = idx + 1
        image_key = f"images/{folder}/{stem}/page_{page_no:04d}.webp"

        if image_key not in existing:
            image_path = work_dir / f"page_{page_no:04d}.webp"
            try:
                images = convert_from_path(
                    str(pdf_path), dpi=IMAGE_DPI, first_page=page_no, last_page=page_no
                )
                buf = io.BytesIO()
                images[0].save(buf, format="WEBP", quality=IMAGE_QUALITY)
                buf.seek(0)
                image_path.write_bytes(buf.getvalue())
                b2_put_file(client, image_key, image_path, "image/webp")
                existing.add(image_key)
            except Exception as exc:
                print(f"WARNING: page {page_no} of {folder}/{stem} (image) failed: {exc}; will retry next run")
                all_ok = False
            finally:
                image_path.unlink(missing_ok=True)

        if UPLOAD_PAGE_PDFS:
            page_key = f"pages/{folder}/{stem}/page_{page_no:04d}.pdf"
            if page_key not in existing:
                page_pdf_path = work_dir / f"page_{page_no:04d}.pdf"
                try:
                    writer = PdfWriter()
                    writer.add_page(reader.pages[idx])
                    with open(page_pdf_path, "wb") as f:
                        writer.write(f)
                    b2_put_file(client, page_key, page_pdf_path, "application/pdf")
                    existing.add(page_key)
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (pdf) failed: {exc}; will retry next run")
                    all_ok = False
                finally:
                    page_pdf_path.unlink(missing_ok=True)

    return all_ok


def process_pdf(client, item, existing):
    """The .done marker is namespaced by output mode (images-only vs
    images+pdfs), not just by stem: UPLOAD_PAGE_PDFS is a per-run toggle, so
    a plain "done" flag would let a run with it off permanently block a
    later backfill run with it on for the same PDF (the .done from the
    earlier run would short-circuit process_pdf before split_and_upload_pages
    ever got a chance to add the missing PDFs).
    """
    folder = item["folder"]
    stem = pathlib.Path(item["name"]).stem
    images_done_key = f"processed/{folder}/{stem}.done"
    pdfs_done_key = f"processed/{folder}/{stem}.with-pdfs.done"
    required_done_key = pdfs_done_key if UPLOAD_PAGE_PDFS else images_done_key

    if required_done_key in existing:
        print(f"skip (already done): {folder}/{stem}")
        return

    print(f"processing: {folder}/{stem}")
    work_dir = TMP_DIR / folder / stem
    work_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = work_dir / item["name"]

    try:
        url = pcloud_download_url(PCLOUD_CODE, item["fileid"])
        download_to(url, pdf_path)

        all_uploaded = split_and_upload_pages(client, pdf_path, folder, stem, work_dir, existing)
        if all_uploaded:
            if images_done_key not in existing:
                b2_put_bytes(client, images_done_key, f"completed at {time.time()}".encode())
                existing.add(images_done_key)
            if UPLOAD_PAGE_PDFS and pdfs_done_key not in existing:
                b2_put_bytes(client, pdfs_done_key, f"completed at {time.time()}".encode())
                existing.add(pdfs_done_key)
            print(f"done: {folder}/{stem}")
        else:
            print(f"WARNING: not all pages verified for {folder}/{stem}; will retry next run")
    finally:
        pdf_path.unlink(missing_ok=True)
        for f in work_dir.glob("*"):
            f.unlink(missing_ok=True)
        try:
            work_dir.rmdir()
        except OSError:
            pass


def main():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    client = b2_client()

    print("listing PDFs on pCloud...")
    pdfs = list_pdfs_recursive(PCLOUD_CODE)
    print(f"found {len(pdfs)} PDF(s)")

    print("listing existing B2 objects...")
    existing = list_existing_keys(client, "images/") | list_existing_keys(client, "processed/")
    if UPLOAD_PAGE_PDFS:
        existing |= list_existing_keys(client, "pages/")
    print(f"found {len(existing)} existing object(s) in B2")

    for item in pdfs:
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before starting a new file"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        process_pdf(client, item, existing)

    print("all PDFs processed")


if __name__ == "__main__":
    main()
