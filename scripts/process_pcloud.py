"""Download PDFs from a public pCloud folder, render each page as a
vision-optimized WebP image, and upload it to Backblaze B2. Optionally (see
UPLOAD_PAGE_PDFS) also splits out and uploads a single-page PDF per page.

Postgres (Supabase) is the source of truth for what's already been done —
see supabase/migrations/20260909140000_init_processing_schema.sql. Every
page's upload is recorded as a row in `pages` (which B2 account/bucket holds
it, and when); a PDF is considered done once every one of its pages has a
row with image_uploaded_at set (and page_pdf_uploaded_at too, if
UPLOAD_PAGE_PDFS). Because the DB records exactly which account holds each
page, pages of one PDF can legitimately live in different B2 accounts (e.g.
after switching B2_ACTIVE_ACCOUNT partway through) with no ambiguity — unlike
the older B2-object-listing approach, there's no need to ever "redo a whole
PDF fresh" just to avoid split-page confusion.

New uploads always go to a single "active" B2 account (B2_ACTIVE_ACCOUNT;
see load_b2_accounts) — this script never reads from B2, only writes there
and records what it wrote in Postgres.

Designed to run as-is inside GitHub Actions (see
.github/workflows/process-pdfs.yml) but only needs boto3/psycopg2/requests/
pypdf/pdf2image/pillow and network access to run anywhere.
"""
import io
import os
import sys
import time
import pathlib
import tempfile

import boto3
import psycopg2
import requests
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path

PCLOUD_CODE = os.environ["PCLOUD_CODE"]  # the pCloud public-link share code
PCLOUD_HOSTS = ["api.pcloud.com", "eapi.pcloud.com"]

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
B2_ACTIVE_ACCOUNT = os.environ.get("B2_ACTIVE_ACCOUNT", "1").strip() or "1"
if B2_ACTIVE_ACCOUNT not in B2_ACCOUNTS:
    raise RuntimeError(
        f"B2_ACTIVE_ACCOUNT={B2_ACTIVE_ACCOUNT!r} is not a configured B2 account "
        f"(configured: {sorted(B2_ACCOUNTS)})"
    )

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


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
    )


def b2_put_file(client, bucket, key, path, content_type):
    with open(path, "rb") as f:
        client.put_object(Bucket=bucket, Key=key, Body=f, ContentType=content_type)


def db_connect():
    conn = psycopg2.connect(SUPABASE_DB_URL)
    conn.autocommit = True  # each statement durable immediately, matching the
    # old B2-marker semantics: a crash mid-run should never lose progress
    # already recorded, and there's no multi-statement transaction here that
    # needs all-or-nothing atomicity.
    return conn


def db_upsert_pcloud_file(conn, fileid, name, folder):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pcloud_files (pcloud_fileid, name, folder) VALUES (%s, %s, %s) "
            "ON CONFLICT (pcloud_fileid) DO NOTHING",
            (fileid, name, folder),
        )


def db_set_page_count(conn, fileid, page_count):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pcloud_files SET page_count = %s WHERE pcloud_fileid = %s",
            (page_count, fileid),
        )


def db_get_pdf_state(conn, fileid):
    """Return (page_count_or_None, {page_no with image done}, {page_no with pdf done})."""
    with conn.cursor() as cur:
        cur.execute("SELECT page_count FROM pcloud_files WHERE pcloud_fileid = %s", (fileid,))
        row = cur.fetchone()
        page_count = row[0] if row else None

        cur.execute(
            "SELECT page_no, image_uploaded_at IS NOT NULL, page_pdf_uploaded_at IS NOT NULL "
            "FROM pages WHERE pcloud_fileid = %s",
            (fileid,),
        )
        images_done = set()
        pdfs_done = set()
        for page_no, image_ok, pdf_ok in cur.fetchall():
            if image_ok:
                images_done.add(page_no)
            if pdf_ok:
                pdfs_done.add(page_no)
    return page_count, images_done, pdfs_done


def db_mark_image_uploaded(conn, fileid, page_no, account, bucket, image_key):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pages (pcloud_fileid, page_no, b2_account, b2_bucket, image_key, image_uploaded_at) "
            "VALUES (%s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (pcloud_fileid, page_no) DO UPDATE SET "
            "b2_account = EXCLUDED.b2_account, b2_bucket = EXCLUDED.b2_bucket, "
            "image_key = EXCLUDED.image_key, image_uploaded_at = now()",
            (fileid, page_no, account, bucket, image_key),
        )


def db_mark_pdf_uploaded(conn, fileid, page_no, account, bucket, page_pdf_key):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pages SET page_pdf_account = %s, page_pdf_bucket = %s, "
            "page_pdf_key = %s, page_pdf_uploaded_at = now() "
            "WHERE pcloud_fileid = %s AND page_no = %s",
            (account, bucket, page_pdf_key, fileid, page_no),
        )


def render_and_upload_image(client, bucket, pdf_path, folder, stem, page_no, work_dir):
    image_key = f"images/{folder}/{stem}/page_{page_no:04d}.webp"
    image_path = work_dir / f"page_{page_no:04d}.webp"
    try:
        images = convert_from_path(str(pdf_path), dpi=IMAGE_DPI, first_page=page_no, last_page=page_no)
        buf = io.BytesIO()
        images[0].save(buf, format="WEBP", quality=IMAGE_QUALITY)
        buf.seek(0)
        image_path.write_bytes(buf.getvalue())
        b2_put_file(client, bucket, image_key, image_path, "image/webp")
        return image_key
    finally:
        image_path.unlink(missing_ok=True)


def split_and_upload_page_pdf(client, bucket, reader, idx, folder, stem, page_no, work_dir):
    page_key = f"pages/{folder}/{stem}/page_{page_no:04d}.pdf"
    page_pdf_path = work_dir / f"page_{page_no:04d}.pdf"
    try:
        writer = PdfWriter()
        writer.add_page(reader.pages[idx])
        with open(page_pdf_path, "wb") as f:
            writer.write(f)
        b2_put_file(client, bucket, page_key, page_pdf_path, "application/pdf")
        return page_key
    finally:
        page_pdf_path.unlink(missing_ok=True)


def process_pdf(conn, client, bucket, item):
    """Returns True once every page needed (images, and pdfs if
    UPLOAD_PAGE_PDFS) is confirmed uploaded — anywhere, per the DB, not
    necessarily in the active account, since a page done in a prior run
    under a different B2_ACTIVE_ACCOUNT still counts."""
    folder = item["folder"]
    stem = pathlib.Path(item["name"]).stem
    fileid = item["fileid"]

    db_upsert_pcloud_file(conn, fileid, item["name"], folder)
    page_count, images_done, pdfs_done = db_get_pdf_state(conn, fileid)

    if page_count is not None:
        needed = set(range(1, page_count + 1))
        if needed <= images_done and (not UPLOAD_PAGE_PDFS or needed <= pdfs_done):
            print(f"skip (already done per DB): {folder}/{stem}")
            return True

    print(f"processing: {folder}/{stem}")
    work_dir = TMP_DIR / folder / stem
    work_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = work_dir / item["name"]

    try:
        url = pcloud_download_url(PCLOUD_CODE, item["fileid"])
        download_to(url, pdf_path)

        reader = PdfReader(str(pdf_path))
        page_count = len(reader.pages)
        db_set_page_count(conn, fileid, page_count)

        all_ok = True
        for idx in range(page_count):
            page_no = idx + 1

            if page_no not in images_done:
                try:
                    image_key = render_and_upload_image(client, bucket, pdf_path, folder, stem, page_no, work_dir)
                    db_mark_image_uploaded(conn, fileid, page_no, B2_ACTIVE_ACCOUNT, bucket, image_key)
                    images_done.add(page_no)
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (image) failed: {exc}; will retry next run")
                    all_ok = False
                    continue  # don't attempt the page PDF for a page whose image just failed

            if UPLOAD_PAGE_PDFS and page_no not in pdfs_done:
                try:
                    page_pdf_key = split_and_upload_page_pdf(
                        client, bucket, reader, idx, folder, stem, page_no, work_dir
                    )
                    db_mark_pdf_uploaded(conn, fileid, page_no, B2_ACTIVE_ACCOUNT, bucket, page_pdf_key)
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (pdf) failed: {exc}; will retry next run")
                    all_ok = False

        if all_ok:
            print(f"done: {folder}/{stem}")
        else:
            print(f"WARNING: not all pages uploaded for {folder}/{stem}; will retry next run")
        return all_ok
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
    conn = db_connect()
    client = b2_client(B2_ACCOUNTS[B2_ACTIVE_ACCOUNT])
    bucket = B2_ACCOUNTS[B2_ACTIVE_ACCOUNT]["bucket"]

    print("listing PDFs on pCloud...")
    pdfs = list_pdfs_recursive(PCLOUD_CODE)
    print(f"found {len(pdfs)} PDF(s)")
    print(f"writing new uploads to B2 account {B2_ACTIVE_ACCOUNT}")

    for item in pdfs:
        if elapsed() > MAX_RUNTIME_SECONDS:
            print(
                f"runtime guard tripped after {elapsed():.0f}s; "
                "stopping before starting a new file"
            )
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        process_pdf(conn, client, bucket, item)

    print("all PDFs processed")


if __name__ == "__main__":
    main()
