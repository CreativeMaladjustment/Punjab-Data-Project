"""Reconcile the `pages` table against what's actually in B2, across both
configured accounts, and free up account 2's space along the way.

Lists every images/*.webp object in account 1 and account 2, and for every
page found in EITHER (not just ones already flagged as duplicates -- see
scripts/audit_b2_pages.py, which detects these problems but never fixes
them), works out where that page's image should be recorded as living:

  - account 1, if it's there (account 1 is authoritative and never touched)
  - otherwise account 2, wherever it actually is

The `pages` row for that page is then created or corrected to match --
whether it was missing entirely, pointing at the wrong account/bucket/key,
or already correct. Only once the row is confirmed to point at account 1
is the redundant account-2 copy (if the page exists in both) deleted, so a
run interrupted partway through never leaves a row pointing at a
just-deleted object, and a page found in account 2 alone is left alone
(it's not a duplicate -- it's simply not processed under account 1 yet).

A page whose image can't be mapped to a `pcloud_files` row (no matching
folder/name) is skipped rather than acted on blindly.

Defaults to a dry run (lists what it would do, writes nothing, deletes
nothing). Pass --execute to actually commit. Safe to re-run: every DB
write is an upsert, and a page already correct with no lingering
account-2 duplicate is a no-op.

Usage:
  python scripts/resolve_duplicate_pages.py            # dry run
  python scripts/resolve_duplicate_pages.py --execute   # actually fix + delete
"""
import os
import pathlib
import re
import sys

import boto3
import psycopg2

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

EXECUTE = "--execute" in sys.argv[1:]

IMAGE_KEY_RE = re.compile(r"^images/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.webp$")


def load_b2_accounts():
    """Return {"1": {"endpoint", "key_id", "app_key", "bucket"}, "2": {...}}.
    Both accounts are required here -- there's nothing to reconcile with only one.
    """
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

    primary = _account("")
    if primary is None:
        raise RuntimeError("B2 account 1 is not fully configured")
    secondary = _account("_2")
    if secondary is None:
        raise RuntimeError(
            "B2 account 2 is not fully configured -- nothing to reconcile without a second account"
        )
    return {"1": primary, "2": secondary}


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
    )


def list_all(client, bucket, prefix):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def list_images(client, bucket):
    """Return {(folder, stem, page_no): key} for every images/*.webp object."""
    images = {}
    for obj in list_all(client, bucket, "images/"):
        m = IMAGE_KEY_RE.match(obj["Key"])
        if m:
            images[(m["folder"], m["stem"], int(m["page"]))] = obj["Key"]
    return images


def db_connect():
    conn = psycopg2.connect(SUPABASE_DB_URL)
    conn.autocommit = True  # each fix is durable immediately, same as
    # process_pcloud.py -- a run interrupted partway through should never
    # lose progress already made, and there's no multi-statement atomicity
    # needed here.
    return conn


def fetch_fileid_by_folder_stem(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pcloud_fileid, name, folder FROM pcloud_files")
        rows = cur.fetchall()
    conn.rollback()  # read-only; drop the implicit transaction
    return {(folder, pathlib.Path(name).stem): fileid for fileid, name, folder in rows}


def fetch_page_row(conn, fileid, page_no):
    """Return (b2_account, b2_bucket, image_key) for this page, or None."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT b2_account, b2_bucket, image_key FROM pages WHERE pcloud_fileid = %s AND page_no = %s",
            (fileid, page_no),
        )
        row = cur.fetchone()
    conn.rollback()
    return row


def upsert_page_location(conn, fileid, page_no, account, bucket, image_key):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pages (pcloud_fileid, page_no, b2_account, b2_bucket, image_key, image_uploaded_at) "
            "VALUES (%s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (pcloud_fileid, page_no) DO UPDATE SET "
            "b2_account = EXCLUDED.b2_account, b2_bucket = EXCLUDED.b2_bucket, "
            "image_key = EXCLUDED.image_key",
            # image_uploaded_at is deliberately left untouched on conflict --
            # this corrects *where* the image is recorded, not *when* it was
            # uploaded, so an existing row's original timestamp survives.
            (fileid, page_no, account, bucket, image_key),
        )


def main():
    accounts = load_b2_accounts()
    client_1 = b2_client(accounts["1"])
    client_2 = b2_client(accounts["2"])
    bucket_1 = accounts["1"]["bucket"]
    bucket_2 = accounts["2"]["bucket"]

    print(f"mode: {'EXECUTE (will fix pages rows and delete account-2 duplicates)' if EXECUTE else 'DRY RUN (no changes)'}")

    print(f"listing images/ in account 1 (bucket {bucket_1})...")
    images_1 = list_images(client_1, bucket_1)
    print(f"  found {len(images_1)} image(s)")

    print(f"listing images/ in account 2 (bucket {bucket_2})...")
    images_2 = list_images(client_2, bucket_2)
    print(f"  found {len(images_2)} image(s)")

    all_keys = sorted(set(images_1) | set(images_2))
    print(f"\n{len(all_keys)} distinct page(s) found across both accounts")

    conn = db_connect()
    fileid_by_folder_stem = fetch_fileid_by_folder_stem(conn)

    fixed = 0
    deleted = 0
    already_ok = 0
    skipped = 0
    for folder, stem, page_no in all_keys:
        in_1 = (folder, stem, page_no) in images_1
        in_2 = (folder, stem, page_no) in images_2
        if in_1:
            correct_account, correct_bucket, correct_key = "1", bucket_1, images_1[(folder, stem, page_no)]
        else:
            correct_account, correct_bucket, correct_key = "2", bucket_2, images_2[(folder, stem, page_no)]
        needs_delete = in_1 and in_2  # a page in both accounts always has its account-2 copy freed

        fileid = fileid_by_folder_stem.get((folder, stem))
        if fileid is None:
            print(f"  SKIP {folder}/{stem} page {page_no}: no matching pcloud_files row; can't safely act")
            skipped += 1
            continue

        current = fetch_page_row(conn, fileid, page_no)
        needs_fix = current != (correct_account, correct_bucket, correct_key)

        if not needs_fix and not needs_delete:
            already_ok += 1
            continue

        action = []
        if needs_fix:
            action.append("CREATE pages row" if current is None else "REPOINT pages row")
        if needs_delete:
            action.append("delete account-2 copy")
        print(f"  {folder}/{stem} page {page_no}: {', '.join(action)} -> account {correct_account}")

        if EXECUTE:
            if needs_fix:
                upsert_page_location(conn, fileid, page_no, correct_account, correct_bucket, correct_key)
            if needs_delete:
                client_2.delete_object(Bucket=bucket_2, Key=images_2[(folder, stem, page_no)])
        if needs_fix:
            fixed += 1
        if needs_delete:
            deleted += 1

    verb = "did" if EXECUTE else "would do"
    print(f"\nalready correct: {already_ok}, {verb} fix: {fixed}, {verb} delete: {deleted}, skipped: {skipped}")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### Resolve duplicate pages\n\n")
            f.write(f"- **Mode:** {'EXECUTE' if EXECUTE else 'dry run'}\n")
            f.write(f"- **Already correct:** {already_ok}\n")
            f.write(f"- **{'Fixed' if EXECUTE else 'Would fix'} `pages` rows:** {fixed}\n")
            f.write(f"- **{'Deleted' if EXECUTE else 'Would delete'} account-2 duplicates:** {deleted}\n")
            f.write(f"- **Skipped (couldn't safely act):** {skipped}\n")

    if not EXECUTE:
        print("\nDry run complete -- no changes were made. Re-run with --execute to commit.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
