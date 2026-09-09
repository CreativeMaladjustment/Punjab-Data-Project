"""Download PDFs from a public pCloud folder, split into single-page PDFs,
render each page as a vision-optimized image, and upload both to Backblaze B2.

B2 is the source of truth for resumability: every unit of work (a page PDF, a
page image, a "this whole PDF is done" marker) is checked against B2 before
it is redone, so a killed or re-dispatched run just continues where it left
off. Designed to run as-is inside GitHub Actions (see
.github/workflows/process-pdfs.yml) but only needs boto3/requests/pypdf/
pdf2image/pillow and network access to run anywhere.

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
from botocore.exceptions import ClientError
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path

PCLOUD_CODE = os.environ["PCLOUD_CODE"]  # the pCloud public-link share code
PCLOUD_HOSTS = ["api.pcloud.com", "eapi.pcloud.com"]

B2_ENDPOINT = os.environ["B2_ENDPOINT"]  # e.g. https://s3.us-west-004.backblazeb2.com
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_APPLICATION_KEY = os.environ["B2_APPLICATION_KEY"]
B2_BUCKET_NAME = os.environ["B2_BUCKET_NAME"]

TMP_DIR = pathlib.Path(os.environ.get("PCLOUD_TMPDIR") or tempfile.mkdtemp(prefix="pcloud_work_"))
IMAGE_DPI = 200
IMAGE_QUALITY = 85
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


def list_pdfs_recursive(code):
    """Walk the public link's folder tree and return every PDF found.

    Each entry is {"fileid": int, "name": str, "folder": str} where "folder"
    is the "/"-joined path of subfolder names the file lives under (relative
    to the shared link's root).
    """
    pdfs = []

    def walk(folderid, folder_path):
        params = {"code": code}
        if folderid is not None:
            params["folderid"] = folderid
        data = pcloud_get("showpublink", params)
        for entry in data["metadata"].get("contents", []):
            if entry.get("isfolder"):
                sub_path = folder_path + [entry["name"]]
                walk(entry["folderid"], sub_path)
            elif entry["name"].lower().endswith(".pdf"):
                pdfs.append(
                    {
                        "fileid": entry["fileid"],
                        "name": entry["name"],
                        "folder": "/".join(folder_path),
                    }
                )

    walk(None, [])
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


def b2_exists(client, key):
    try:
        client.head_object(Bucket=B2_BUCKET_NAME, Key=key)
        return True
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise


def b2_put_file(client, key, path, content_type):
    with open(path, "rb") as f:
        client.put_object(Bucket=B2_BUCKET_NAME, Key=key, Body=f, ContentType=content_type)


def b2_put_bytes(client, key, data, content_type="text/plain"):
    client.put_object(Bucket=B2_BUCKET_NAME, Key=key, Body=data, ContentType=content_type)


def split_and_upload_pages(client, pdf_path, folder, stem, work_dir):
    """Split pdf_path into single-page PDFs + page images, uploading each to
    B2 (skipping any that already exist there). Returns True once every page
    is confirmed present on B2.
    """
    reader = PdfReader(str(pdf_path))
    page_count = len(reader.pages)

    for idx in range(page_count):
        page_no = idx + 1
        page_key = f"pages/{folder}/{stem}/page_{page_no:04d}.pdf"
        image_key = f"images/{folder}/{stem}/page_{page_no:04d}.webp"

        page_pdf_path = work_dir / f"page_{page_no:04d}.pdf"
        image_path = work_dir / f"page_{page_no:04d}.webp"

        try:
            if not b2_exists(client, page_key):
                writer = PdfWriter()
                writer.add_page(reader.pages[idx])
                with open(page_pdf_path, "wb") as f:
                    writer.write(f)
                b2_put_file(client, page_key, page_pdf_path, "application/pdf")

            if not b2_exists(client, image_key):
                if not page_pdf_path.exists():
                    writer = PdfWriter()
                    writer.add_page(reader.pages[idx])
                    with open(page_pdf_path, "wb") as f:
                        writer.write(f)
                images = convert_from_path(str(page_pdf_path), dpi=IMAGE_DPI)
                buf = io.BytesIO()
                images[0].save(buf, format="WEBP", quality=IMAGE_QUALITY)
                buf.seek(0)
                image_path.write_bytes(buf.getvalue())
                b2_put_file(client, image_key, image_path, "image/webp")
        finally:
            page_pdf_path.unlink(missing_ok=True)
            image_path.unlink(missing_ok=True)

    # Final verification pass before the .done marker is written.
    for idx in range(page_count):
        page_no = idx + 1
        page_key = f"pages/{folder}/{stem}/page_{page_no:04d}.pdf"
        image_key = f"images/{folder}/{stem}/page_{page_no:04d}.webp"
        if not b2_exists(client, page_key) or not b2_exists(client, image_key):
            return False
    return True


def process_pdf(client, item):
    folder = item["folder"]
    stem = pathlib.Path(item["name"]).stem
    done_key = f"processed/{folder}/{stem}.done"

    if b2_exists(client, done_key):
        print(f"skip (already done): {folder}/{stem}")
        return

    print(f"processing: {folder}/{stem}")
    work_dir = TMP_DIR / folder / stem
    work_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = work_dir / item["name"]

    try:
        url = pcloud_download_url(PCLOUD_CODE, item["fileid"])
        download_to(url, pdf_path)

        all_uploaded = split_and_upload_pages(client, pdf_path, folder, stem, work_dir)
        if all_uploaded:
            b2_put_bytes(client, done_key, f"completed at {time.time()}".encode())
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

    for item in pdfs:
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before starting a new file"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        process_pdf(client, item)

    print("all PDFs processed")


if __name__ == "__main__":
    main()
