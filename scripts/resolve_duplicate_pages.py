"""Resolve page images that exist in BOTH configured B2 accounts (see
scripts/audit_b2_pages.py, which detects but never fixes this).

Policy: account 1 is authoritative and is never touched. For every page
found in both accounts, this repoints the `pages` row to account 1 (if it
isn't already) and then deletes the redundant copy from account 2 --
freeing account 2's space for PDFs that haven't been processed yet.

Never deletes an account-2 object unless the account-1 copy of that exact
page is confirmed present in this same run's listing, and never repoints a
`pages` row to account 1 without first confirming that copy exists. The
`pages` row is always updated before the account-2 object is deleted, so a
run interrupted partway through never leaves a row pointing at a
just-deleted object.

Defaults to a dry run (lists what it would do, writes nothing, deletes
nothing). Pass --execute to actually commit. Safe to re-run: repointing is
idempotent, and a page whose account-2 copy is already gone is just skipped.

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
    Both accounts are required here -- there's nothing to resolve with only one.
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
            "B2 account 2 is not fully configured -- nothing to resolve without a second account"
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
    conn.autocommit = True  # each repoint is durable immediately, same as
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
    """Return (page_id, b2_account) for this page, or None if no row exists."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, b2_account FROM pages WHERE pcloud_fileid = %s AND page_no = %s",
            (fileid, page_no),
        )
        row = cur.fetchone()
    conn.rollback()
    return row


def repoint_to_account_1(conn, page_id, bucket_1, image_key_1):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pages SET b2_account = '1', b2_bucket = %s, image_key = %s WHERE id = %s",
            (bucket_1, image_key_1, page_id),
        )


def main():
    accounts = load_b2_accounts()
    client_1 = b2_client(accounts["1"])
    client_2 = b2_client(accounts["2"])
    bucket_1 = accounts["1"]["bucket"]
    bucket_2 = accounts["2"]["bucket"]

    print(f"mode: {'EXECUTE (will repoint pages rows and delete account-2 duplicates)' if EXECUTE else 'DRY RUN (no changes)'}")

    print(f"listing images/ in account 1 (bucket {bucket_1})...")
    images_1 = list_images(client_1, bucket_1)
    print(f"  found {len(images_1)} image(s)")

    print(f"listing images/ in account 2 (bucket {bucket_2})...")
    images_2 = list_images(client_2, bucket_2)
    print(f"  found {len(images_2)} image(s)")

    duplicate_keys = sorted(set(images_1) & set(images_2))
    print(f"\nfound {len(duplicate_keys)} page(s) present in both accounts")

    if not duplicate_keys:
        print("nothing to resolve")
        return

    conn = db_connect()
    fileid_by_folder_stem = fetch_fileid_by_folder_stem(conn)

    resolved = 0
    skipped = 0
    for folder, stem, page_no in duplicate_keys:
        key_1 = images_1[(folder, stem, page_no)]
        key_2 = images_2[(folder, stem, page_no)]

        fileid = fileid_by_folder_stem.get((folder, stem))
        if fileid is None:
            print(f"  SKIP {folder}/{stem} page {page_no}: no matching pcloud_files row; can't safely act")
            skipped += 1
            continue

        row = fetch_page_row(conn, fileid, page_no)
        if row is None:
            print(f"  SKIP {folder}/{stem} page {page_no}: no pages row; can't safely act")
            skipped += 1
            continue
        page_id, current_account = row

        print(f"  {folder}/{stem} page {page_no}: repoint to account 1, delete account-2 copy")
        if EXECUTE:
            if current_account != "1":
                repoint_to_account_1(conn, page_id, bucket_1, key_1)
            client_2.delete_object(Bucket=bucket_2, Key=key_2)
        resolved += 1

    print(f"\n{'resolved' if EXECUTE else 'would resolve'}: {resolved}, skipped: {skipped}")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### Resolve duplicate pages\n\n")
            f.write(f"- **Mode:** {'EXECUTE' if EXECUTE else 'dry run'}\n")
            f.write(f"- **Duplicate pages found:** {len(duplicate_keys)}\n")
            f.write(f"- **{'Resolved' if EXECUTE else 'Would resolve'}:** {resolved}\n")
            f.write(f"- **Skipped (couldn't safely act):** {skipped}\n")

    if not EXECUTE:
        print("\nDry run complete -- no changes were made. Re-run with --execute to commit.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
