"""Delete unlinked page images from B2 -- images/*.webp objects that have no
matching `pcloud_files` row at all (scripts/audit_b2_pages.py's "no matching
pcloud_files row" finding).

An image that can't be linked to a `pcloud_files` row has no reason to
exist: it isn't tracked by anything, process_pcloud.py doesn't consult
existing B2 objects before it (re)uploads a PDF's pages anyway (it decides
purely from `pcloud_files`/`pages` state), so keeping it around would
preserve nothing. This just deletes it.

This does NOT touch:
  - images belonging to a PDF that already has a pcloud_files row, even if
    a specific page is missing its `pages` row -- that's a different case
    scripts/resolve_duplicate_pages.py already fixes by creating the row,
    not by deleting anything.
  - page PDFs (pages/*.pdf) or extraction output (extractions/**) -- only
    the images/*.webp objects the audit flags as unlinked.

Defaults to a dry run (lists what it would delete, deletes nothing). Pass
--execute to actually delete. Safe to re-run.

Usage:
  python scripts/remove_unlinked_images.py            # dry run
  python scripts/remove_unlinked_images.py --execute   # actually delete
"""
import os
import pathlib
import re
import sys

import boto3
import psycopg2

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

EXECUTE = "--execute" in sys.argv[1:]

# folder uses .* (not .+): a pCloud PDF sitting in the share's root has
# folder == "", which process_pcloud.py renders as "images//stem/...".
IMAGE_KEY_RE = re.compile(r"^images/(?P<folder>.*)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.webp$")


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
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def list_images_by_stem(client, bucket):
    """Return {(folder, stem): [key, ...]} for every images/*.webp object."""
    by_stem = {}
    for obj in list_all(client, bucket, "images/"):
        m = IMAGE_KEY_RE.match(obj["Key"])
        if m:
            by_stem.setdefault((m["folder"], m["stem"]), []).append(obj["Key"])
    return by_stem


def fetch_known_folder_stems(conn):
    """Return {(folder, stem)} already tracked in pcloud_files -- these are
    out of scope here; resolve_duplicate_pages.py owns fixing them."""
    with conn.cursor() as cur:
        cur.execute("SELECT name, folder FROM pcloud_files")
        rows = cur.fetchall()
    return {(folder, pathlib.Path(name).stem) for name, folder in rows}


def main():
    accounts = load_b2_accounts()
    conn = psycopg2.connect(SUPABASE_DB_URL)
    known = fetch_known_folder_stems(conn)

    print(f"mode: {'EXECUTE (will delete unlinked images)' if EXECUTE else 'DRY RUN (no changes)'}")

    clients = {aid: b2_client(acct) for aid, acct in accounts.items()}
    to_delete = []  # (account_id, bucket, key)

    for account_id, account in accounts.items():
        bucket = account["bucket"]
        print(f"scanning images/ in account {account_id} (bucket {bucket})...")
        by_stem = list_images_by_stem(clients[account_id], bucket)
        for (folder, stem), keys in sorted(by_stem.items()):
            if (folder, stem) in known:
                continue  # tracked -- not this script's job
            for key in keys:
                to_delete.append((account_id, bucket, key))

    print(f"\n{len(to_delete)} unlinked image(s) found (no matching pcloud_files row)")
    for account_id, bucket, key in to_delete:
        print(f"  {'DELETE' if EXECUTE else 'would delete'} account {account_id}/{bucket}: {key}")
        if EXECUTE:
            clients[account_id].delete_object(Bucket=bucket, Key=key)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### Remove unlinked images\n\n")
            f.write(f"- **Mode:** {'EXECUTE' if EXECUTE else 'dry run'}\n")
            f.write(f"- **{'Deleted' if EXECUTE else 'Would delete'}:** {len(to_delete)}\n")

    if not EXECUTE:
        print("\nDry run complete -- no changes were made. Re-run with --execute to commit.")
    else:
        print("\nDone. Re-run process_pcloud.py to reprocess the PDFs whose images were just removed.")


if __name__ == "__main__":
    main()
