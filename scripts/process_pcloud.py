"""Download PDFs from a public pCloud folder, render each page as a
vision-optimized WebP image, and upload it to Backblaze B2. Optionally (see
UPLOAD_PAGE_PDFS) also splits out and uploads a single-page PDF per page.

Postgres (Supabase) is the source of truth for what's already been done —
see supabase/migrations/20260909140000_init_processing_schema.sql. Every
page's upload is recorded as a row in `pages` (which B2 account/bucket holds
it, and when); a PDF is considered done once every one of its pages has a
row with image_uploaded_at set (and page_pdf_uploaded_at too, if
UPLOAD_PAGE_PDFS).

New PDFs are spread across whichever of the one or two configured B2
accounts are set up, round-robin (PDF 1 to account 1, PDF 2 to account 2,
PDF 3 back to account 1, and so on) rather than all going to one "active"
account.
If a page's upload to its assigned account fails, it's retried immediately
against every other configured account before being given up on for this
run -- so a single account being full or erroring doesn't stall pages that
the other account can still take. If uploads fail on every configured
account several times in a row, that's treated as every account being stuck
(not a one-off blip) and the whole run stops early (see
BOTH_ACCOUNTS_FAILURE_THRESHOLD) rather than grinding through the rest of
the runtime budget failing the same way. Because the DB records exactly
which account holds each page, pages of one PDF can legitimately live in
different B2 accounts with no ambiguity — there's no need to ever "redo a
whole PDF fresh" just to avoid split-page confusion.

This script never reads existing content from B2, only writes there and
records what it wrote in Postgres.

Three ways to run it (see .github/workflows/process-pdfs.yml):
- No args, no PCLOUD_PDFS_JSON: the original full scan -- lists every PDF
  and processes whichever aren't already done, sequentially, one process.
- `--list-remaining`: prints one compact JSON line, an array of up to
  MAX_PARALLEL_PDF_WORKERS slices (each a list of {fileid, name, folder,
  account}) covering every PDF not yet done, and nothing else. Powers the
  `list-remaining` job that builds the `process` job's matrix -- each
  matrix entry is a disjoint slice assigned in advance (see
  chunk_remaining_pdfs()), so unlike the LLM extraction pipeline's
  claim_next_page(), no live atomic claiming is needed to keep parallel
  workers from picking the same PDF, and the matrix stays a small, fixed
  size regardless of how large the backlog grows.
- PCLOUD_PDFS_JSON set (one slice, as produced by chunk_remaining_pdfs()):
  processes just those PDFs, sequentially, and returns. Used by the
  `process` job's matrix, one call per matrix entry.

Designed to run as-is inside GitHub Actions but only needs
boto3/psycopg2/requests/pypdf/pdf2image/pillow and network access to run
anywhere.
"""
import io
import json
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
ACCOUNT_ORDER = sorted(B2_ACCOUNTS)  # e.g. ["1"] or ["1", "2"]; round-robin order

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
# If a page's upload fails on every configured account this many times in a
# row, that's treated as every account being stuck (full, erroring, etc.)
# rather than a run of one-off blips, and the whole run stops early instead
# of spending the rest of the runtime budget failing the same way.
BOTH_ACCOUNTS_FAILURE_THRESHOLD = 5
BOTH_ACCOUNTS_FAILURE_EXIT_CODE = 43
# How many `process` matrix jobs .github/workflows/process-pdfs.yml's
# `list-remaining` job fans work out into. Fixed, not backlog-size-dependent:
# GitHub Actions caps a single job's matrix at 256 combinations, so one
# matrix entry per remaining PDF would eventually break outright as the
# backlog grows. chunk_remaining_pdfs() instead always produces at most this
# many slices, each containing a share of the remaining PDFs, however many
# there are.
MAX_PARALLEL_PDF_WORKERS = 10

START_TIME = time.time()


class AllAccountsFailedError(Exception):
    """Raised once uploads have failed on every configured B2 account too
    many times in a row (see BOTH_ACCOUNTS_FAILURE_THRESHOLD)."""


class UploadHealthTracker:
    """Tracks consecutive page uploads that failed on every configured B2
    account. Any single success (on any account) resets the streak -- only
    a sustained run of total failures looks like every account being stuck."""

    def __init__(self, threshold, account_count):
        self.threshold = threshold
        self.account_count = account_count
        self.consecutive_total_failures = 0

    def record_success(self):
        self.consecutive_total_failures = 0

    def record_total_failure(self):
        if self.account_count < 2:
            # Nothing to fall back to -- a "failed on every account" streak
            # is trivially true after any single real failure, so with only
            # one account configured this must behave exactly like before
            # this class existed: log it and retry next run, never stop early.
            return
        self.consecutive_total_failures += 1
        if self.consecutive_total_failures >= self.threshold:
            raise AllAccountsFailedError(
                f"uploads failed on every configured B2 account "
                f"{self.consecutive_total_failures} times in a row"
            )


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


def upload_with_fallback(clients, key, local_path, content_type, primary_account):
    """Try uploading local_path to `key` under `primary_account` first, then
    every other configured account in ACCOUNT_ORDER. Returns the account id
    that succeeded, or raises the last exception if every account failed."""
    order = [primary_account] + [a for a in ACCOUNT_ORDER if a != primary_account]
    last_exc = None
    for account_id in order:
        try:
            b2_put_file(clients[account_id], B2_ACCOUNTS[account_id]["bucket"], key, local_path, content_type)
            return account_id
        except Exception as exc:
            last_exc = exc
            print(f"WARNING: upload to account {account_id} failed: {exc}")
    raise last_exc


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
            "ON CONFLICT (pcloud_fileid) DO UPDATE SET name = EXCLUDED.name, folder = EXCLUDED.folder",
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


def render_image(pdf_path, folder, stem, page_no, work_dir):
    """Render one page to a local WebP file. Returns (image_key, local_path)."""
    image_key = f"images/{folder}/{stem}/page_{page_no:04d}.webp"
    image_path = work_dir / f"page_{page_no:04d}.webp"
    images = convert_from_path(str(pdf_path), dpi=IMAGE_DPI, first_page=page_no, last_page=page_no)
    buf = io.BytesIO()
    images[0].save(buf, format="WEBP", quality=IMAGE_QUALITY)
    buf.seek(0)
    image_path.write_bytes(buf.getvalue())
    return image_key, image_path


def render_page_pdf(reader, idx, folder, stem, page_no, work_dir):
    """Split out one page to a local single-page PDF. Returns (page_key, local_path)."""
    page_key = f"pages/{folder}/{stem}/page_{page_no:04d}.pdf"
    page_pdf_path = work_dir / f"page_{page_no:04d}.pdf"
    writer = PdfWriter()
    writer.add_page(reader.pages[idx])
    with open(page_pdf_path, "wb") as f:
        writer.write(f)
    return page_key, page_pdf_path


def _pdf_needs_processing(page_count, images_done, pdfs_done):
    """Pure predicate version of the "is this PDF already done" check, shared
    by process_pdf()'s early-return and list_remaining_pdfs(). A PDF never
    opened before (page_count is None, since it's only set once a run has
    actually read the file -- see db_set_page_count) always needs it."""
    if page_count is None:
        return True
    needed = set(range(1, page_count + 1))
    return not (needed <= images_done and (not UPLOAD_PAGE_PDFS or needed <= pdfs_done))


def list_remaining_pdfs():
    """List every PDF on pCloud not yet fully processed per the DB, each
    tagged with which B2 account it should try first -- round-robin over
    just the remaining ones, spreading the parallel matrix's load evenly.
    Doesn't touch B2."""
    conn = db_connect()
    pdfs = list_pdfs_recursive(PCLOUD_CODE)
    remaining = [
        item
        for item in pdfs
        if _pdf_needs_processing(*db_get_pdf_state(conn, item["fileid"]))
    ]
    return [
        {**item, "account": ACCOUNT_ORDER[i % len(ACCOUNT_ORDER)]}
        for i, item in enumerate(remaining)
    ]


def chunk_remaining_pdfs():
    """list_remaining_pdfs(), split round-robin into at most
    MAX_PARALLEL_PDF_WORKERS slices -- so the `process` job's matrix (see
    .github/workflows/process-pdfs.yml) stays a small, fixed size no matter
    how large the backlog grows. Powers --list-remaining (see main()), which
    the `list-remaining` job calls to build that matrix; each slice becomes
    one matrix entry, processed sequentially by one worker (main()'s
    PCLOUD_PDFS_JSON mode)."""
    remaining = list_remaining_pdfs()
    num_workers = min(MAX_PARALLEL_PDF_WORKERS, len(remaining))
    chunks = [[] for _ in range(num_workers)]
    for i, item in enumerate(remaining):
        chunks[i % num_workers].append(item)
    return chunks


def process_pdf(conn, clients, item, assigned_account, health):
    """Returns True once every page needed (images, and pdfs if
    UPLOAD_PAGE_PDFS) is confirmed uploaded — anywhere, per the DB, not
    necessarily under `assigned_account`, since a page done in a prior run
    still counts, and a page uploaded this run may have fallen back to a
    different account if `assigned_account` failed."""
    folder = item["folder"]
    stem = pathlib.Path(item["name"]).stem
    fileid = item["fileid"]

    db_upsert_pcloud_file(conn, fileid, item["name"], folder)
    page_count, images_done, pdfs_done = db_get_pdf_state(conn, fileid)

    if not _pdf_needs_processing(page_count, images_done, pdfs_done):
        print(f"skip (already done per DB): {folder}/{stem}")
        return True

    print(f"processing: {folder}/{stem} (primary account: {assigned_account})")
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
                    image_key, image_path = render_image(pdf_path, folder, stem, page_no, work_dir)
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (image) failed to render: {exc}; will retry next run")
                    all_ok = False
                    continue  # not a B2 problem -- don't count it against the health tracker
                try:
                    account_id = upload_with_fallback(clients, image_key, image_path, "image/webp", assigned_account)
                    bucket = B2_ACCOUNTS[account_id]["bucket"]
                    db_mark_image_uploaded(conn, fileid, page_no, account_id, bucket, image_key)
                    images_done.add(page_no)
                    health.record_success()
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (image) failed to upload: {exc}; will retry next run")
                    all_ok = False
                    health.record_total_failure()
                    continue  # don't attempt the page PDF for a page whose image just failed
                finally:
                    image_path.unlink(missing_ok=True)

            if UPLOAD_PAGE_PDFS and page_no not in pdfs_done:
                try:
                    page_pdf_key, page_pdf_path = render_page_pdf(reader, idx, folder, stem, page_no, work_dir)
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (pdf) failed to render: {exc}; will retry next run")
                    all_ok = False
                    continue  # not a B2 problem -- don't count it against the health tracker
                try:
                    account_id = upload_with_fallback(clients, page_pdf_key, page_pdf_path, "application/pdf", assigned_account)
                    bucket = B2_ACCOUNTS[account_id]["bucket"]
                    db_mark_pdf_uploaded(conn, fileid, page_no, account_id, bucket, page_pdf_key)
                    health.record_success()
                except Exception as exc:
                    print(f"WARNING: page {page_no} of {folder}/{stem} (pdf) failed to upload: {exc}; will retry next run")
                    all_ok = False
                    health.record_total_failure()
                finally:
                    page_pdf_path.unlink(missing_ok=True)

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
    if "--list-remaining" in sys.argv[1:]:
        # Prints one compact JSON line to stdout and nothing else -- the
        # `list-remaining` workflow job captures it straight into a matrix.
        print(json.dumps(chunk_remaining_pdfs(), separators=(",", ":")))
        return

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    conn = db_connect()
    clients = {account_id: b2_client(account) for account_id, account in B2_ACCOUNTS.items()}
    health = UploadHealthTracker(BOTH_ACCOUNTS_FAILURE_THRESHOLD, len(ACCOUNT_ORDER))

    pdfs_json = os.environ.get("PCLOUD_PDFS_JSON", "").strip()
    if pdfs_json:
        # One slice of the remaining-PDF backlog per invocation -- used by
        # the `process` job's matrix (see .github/workflows/process-pdfs.yml),
        # where chunk_remaining_pdfs() already split the full remaining list
        # into disjoint slices (each item already carries its assigned B2
        # account), so there's no listing left to do here, just looping
        # through this worker's share.
        items = json.loads(pdfs_json)
        try:
            for item in items:
                process_pdf(conn, clients, item, item["account"], health)
        except AllAccountsFailedError as exc:
            print(f"stopping: {exc}")
            sys.exit(BOTH_ACCOUNTS_FAILURE_EXIT_CODE)
        return

    print("listing PDFs on pCloud...")
    pdfs = list_pdfs_recursive(PCLOUD_CODE)
    print(f"found {len(pdfs)} PDF(s)")
    print(f"round-robinning new uploads across account(s): {', '.join(ACCOUNT_ORDER)}")

    try:
        for i, item in enumerate(pdfs):
            if elapsed() > MAX_RUNTIME_SECONDS:
                print(
                    f"runtime guard tripped after {elapsed():.0f}s; "
                    "stopping before starting a new file"
                )
                sys.exit(RUNTIME_GUARD_EXIT_CODE)
            assigned_account = ACCOUNT_ORDER[i % len(ACCOUNT_ORDER)]
            process_pdf(conn, clients, item, assigned_account, health)
    except AllAccountsFailedError as exc:
        print(f"stopping: {exc}")
        sys.exit(BOTH_ACCOUNTS_FAILURE_EXIT_CODE)

    print("all PDFs processed")


if __name__ == "__main__":
    main()
