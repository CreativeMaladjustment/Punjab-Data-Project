"""Audit: list every page-image object in each configured B2 account/bucket,
cross-reference against the `pages` table in Postgres (Supabase), and report:

  - images that exist in B2 but have no corresponding `pages` row (or whose
    `pages` row points at a different account/bucket/key than where the
    image actually is)
  - the same page's image uploaded to BOTH B2 accounts -- a duplicate, most
    likely from B2_ACTIVE_ACCOUNT being switched without process_pcloud.py
    knowing the other account already had this page

Read-only: never writes to B2 or Postgres. Safe to run any time.

Usage:
  python scripts/audit_b2_pages.py
"""
import os
import pathlib
import re
import sys

import boto3
import psycopg2

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

IMAGE_KEY_RE = re.compile(r"^images/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.webp$")


def load_b2_accounts():
    """Return {"1": {"endpoint", "key_id", "app_key", "bucket"}, "2": {...}}.
    Account "1" is required; account "2" is included only if fully configured.
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

    accounts = {}
    primary = _account("")
    if primary is None:
        raise RuntimeError("B2 account 1 is not fully configured")
    accounts["1"] = primary
    secondary = _account("_2")
    if secondary is not None:
        accounts["2"] = secondary
    return accounts


def b2_client(account):
    return boto3.client(
        "s3",
        endpoint_url=account["endpoint"],
        aws_access_key_id=account["key_id"],
        aws_secret_access_key=account["app_key"],
    )


def list_all(client, bucket, prefix):
    """Yield every object {"Key", ...} under prefix."""
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
    return psycopg2.connect(SUPABASE_DB_URL)


def fetch_fileid_by_folder_stem(conn):
    """Return {(folder, stem): pcloud_fileid} for every known pCloud file."""
    with conn.cursor() as cur:
        cur.execute("SELECT pcloud_fileid, name, folder FROM pcloud_files")
        rows = cur.fetchall()
    conn.rollback()  # read-only; drop the implicit transaction
    return {(folder, pathlib.Path(name).stem): fileid for fileid, name, folder in rows}


def fetch_pages_by_fileid(conn):
    """Return {(pcloud_fileid, page_no): (b2_account, b2_bucket, image_key)}."""
    with conn.cursor() as cur:
        cur.execute("SELECT pcloud_fileid, page_no, b2_account, b2_bucket, image_key FROM pages")
        rows = cur.fetchall()
    conn.rollback()
    return {(fileid, page_no): (account, bucket, key) for fileid, page_no, account, bucket, key in rows}


def find_duplicates(images_by_account):
    """Return [((folder, stem, page_no), key_in_account_1, key_in_account_2), ...]
    for every page whose image exists in more than one configured account."""
    account_ids = sorted(images_by_account)
    duplicates = []
    for i, a in enumerate(account_ids):
        for b in account_ids[i + 1:]:
            shared = set(images_by_account[a]) & set(images_by_account[b])
            for page_key in sorted(shared):
                duplicates.append((page_key, a, images_by_account[a][page_key], b, images_by_account[b][page_key]))
    return duplicates


def _group_by_stem(rows):
    """rows: [(group_key, page_no), ...]. Returns [(group_key, count, min_page, max_page), ...]
    sorted by group_key -- collapsing a real-world run's thousands of one-line-per-page entries
    down to one line per affected PDF, which is what keeps the summary under GitHub's
    $GITHUB_STEP_SUMMARY size cap (1 MiB) no matter how many pages are affected."""
    groups = {}
    for group_key, page_no in rows:
        groups.setdefault(group_key, []).append(page_no)
    return sorted((k, len(v), min(v), max(v)) for k, v in groups.items())


def write_summary(duplicates, missing, mismatched):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    duplicate_rows = [
        ((folder, stem, a, b), page_no)
        for (folder, stem, page_no), a, _key_a, b, _key_b in duplicates
    ]

    missing_rows = []
    for account_id, bucket, key, reason in missing:
        m = IMAGE_KEY_RE.match(key)
        missing_rows.append(((account_id, bucket, m["folder"], m["stem"], reason), int(m["page"])))

    mismatched_rows = []
    for account_id, bucket, key, row in mismatched:
        m = IMAGE_KEY_RE.match(key)
        db_account, db_bucket, _db_key = row
        mismatched_rows.append(
            ((account_id, bucket, m["folder"], m["stem"], db_account, db_bucket), int(m["page"]))
        )

    duplicate_groups = _group_by_stem(duplicate_rows)
    missing_groups = _group_by_stem(missing_rows)
    mismatched_groups = _group_by_stem(mismatched_rows)

    with open(summary_path, "a", encoding="utf-8") as f:
        f.write("### B2 / Postgres pages audit\n\n")
        f.write(f"- **Duplicate images (uploaded to more than one B2 account):** {len(duplicates)}\n")
        f.write(f"- **Images with no matching `pages` row:** {len(missing)}\n")
        f.write(f"- **Images whose `pages` row points elsewhere:** {len(mismatched)}\n\n")
        if duplicate_groups:
            f.write("#### Duplicates (grouped by PDF)\n\n")
            for (folder, stem, a, b), count, lo, hi in duplicate_groups:
                f.write(f"- `{folder}/{stem}`: {count} page(s) (range {lo}-{hi}) in both account {a} and account {b}\n")
            f.write("\n")
        if missing_groups:
            f.write("#### Missing pages rows (grouped by PDF)\n\n")
            for (account_id, bucket, folder, stem, reason), count, lo, hi in missing_groups:
                f.write(f"- account {account_id}/`{bucket}` `{folder}/{stem}`: {count} page(s) (range {lo}-{hi}) — {reason}\n")
            f.write("\n")
        if mismatched_groups:
            f.write("#### Mismatched pages rows (grouped by PDF)\n\n")
            for (account_id, bucket, folder, stem, db_account, db_bucket), count, lo, hi in mismatched_groups:
                f.write(
                    f"- account {account_id}/`{bucket}` `{folder}/{stem}`: {count} page(s) (range {lo}-{hi}) "
                    f"— `pages` says account {db_account}/`{db_bucket}`\n"
                )
            f.write("\n")


def main():
    accounts = load_b2_accounts()
    conn = db_connect()
    fileid_by_folder_stem = fetch_fileid_by_folder_stem(conn)
    pages_by_fileid = fetch_pages_by_fileid(conn)

    images_by_account = {}
    buckets = {}
    for account_id, account in accounts.items():
        client = b2_client(account)
        bucket = account["bucket"]
        buckets[account_id] = bucket
        print(f"listing images/ in account {account_id} (bucket {bucket})...")
        images = list_images(client, bucket)
        images_by_account[account_id] = images
        print(f"  found {len(images)} image(s)")

    duplicates_raw = find_duplicates(images_by_account)

    missing = []
    mismatched = []
    for account_id, images in images_by_account.items():
        bucket = buckets[account_id]
        for (folder, stem, page_no), key in sorted(images.items()):
            fileid = fileid_by_folder_stem.get((folder, stem))
            if fileid is None:
                missing.append((account_id, bucket, key, "no matching pcloud_files row"))
                continue
            row = pages_by_fileid.get((fileid, page_no))
            if row is None:
                missing.append((account_id, bucket, key, "no pages row for this page"))
                continue
            db_account, db_bucket, db_key = row
            if (db_account, db_bucket, db_key) != (account_id, bucket, key):
                mismatched.append((account_id, bucket, key, row))

    print(f"\n=== Duplicate images (same page uploaded to more than one account): {len(duplicates_raw)} ===")
    for (folder, stem, page_no), a, key_a, b, key_b in duplicates_raw:
        print(f"  {folder}/{stem} page {page_no}: account {a} -> {key_a} | account {b} -> {key_b}")

    print(f"\n=== Images with no matching pages row: {len(missing)} ===")
    for account_id, bucket, key, reason in missing:
        print(f"  account {account_id}/{bucket}: {key} -- {reason}")

    print(f"\n=== Images whose pages row points elsewhere: {len(mismatched)} ===")
    for account_id, bucket, key, row in mismatched:
        print(f"  account {account_id}/{bucket}: {key} -- pages row says {row}")

    write_summary(duplicates_raw, missing, mismatched)

    if duplicates_raw or missing or mismatched:
        print("\naudit found issue(s); see above")
        sys.exit(1)

    print("\naudit clean: every B2 image has exactly one matching pages row, no duplicates")


if __name__ == "__main__":
    main()
