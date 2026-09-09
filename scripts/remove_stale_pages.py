"""Delete `pages` rows that have no backing image in either configured B2
account -- the mirror of scripts/remove_unlinked_images.py (which deletes a
B2 image with no `pages` row). A `pages` row with no image anywhere to back
it has no reason to exist: there's nothing to extract from, link, or serve.

"No backing image" means the page's (folder, stem, page_no) doesn't match
any images/*.webp object in EITHER account -- not just the account/bucket
the row happens to currently record. That distinction matters: a row whose
image was simply uploaded under the *other* account (a case
scripts/resolve_duplicate_pages.py fixes by repointing, not deleting) must
never be mistaken for stale just because its recorded location is empty.

Deleting a `pages` row cascades to its `llm_extractions` and
`catalogue_entries` rows (the schema's own ON DELETE CASCADE) -- nothing
further to clean up by hand. `pcloud_files` rows are left alone even if
every one of their pages is removed: process_pcloud.py already treats a
PDF as needing (re)processing whenever any of its pages aren't fully
uploaded, regardless of what `page_count` was previously recorded.

Defaults to a dry run (lists what it would delete, deletes nothing). Pass
--execute to actually delete. Safe to re-run.

Usage:
  python scripts/remove_stale_pages.py            # dry run
  python scripts/remove_stale_pages.py --execute   # actually delete
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


def list_image_keys(client, bucket):
    """Return {(folder, stem, page_no)} for every images/*.webp object."""
    keys = set()
    for obj in list_all(client, bucket, "images/"):
        m = IMAGE_KEY_RE.match(obj["Key"])
        if m:
            keys.add((m["folder"], m["stem"], int(m["page"])))
    return keys


def fetch_all_pages(conn):
    """Return [(page_id, folder, stem, page_no), ...] for every pages row."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT p.id, pf.folder, pf.name, p.page_no "
            "FROM pages p JOIN pcloud_files pf ON pf.pcloud_fileid = p.pcloud_fileid"
        )
        rows = cur.fetchall()
    return [(page_id, folder, pathlib.Path(name).stem, page_no) for page_id, folder, name, page_no in rows]


def delete_pages(conn, page_ids):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pages WHERE id = ANY(%s)", (page_ids,))


def _group_by_stem(rows):
    """rows: [((folder, stem), page_no), ...]. Returns [((folder, stem), count,
    min_page, max_page), ...] sorted -- keeps the summary bounded regardless
    of how many stale pages are found (see audit_b2_pages.py's write_summary,
    which hit GitHub's 1 MiB step-summary cap before this same grouping)."""
    groups = {}
    for key, page_no in rows:
        groups.setdefault(key, []).append(page_no)
    return sorted((k, len(v), min(v), max(v)) for k, v in groups.items())


def main():
    accounts = load_b2_accounts()
    conn = psycopg2.connect(SUPABASE_DB_URL)
    conn.autocommit = True

    print(f"mode: {'EXECUTE (will delete stale pages rows)' if EXECUTE else 'DRY RUN (no changes)'}")

    present = set()
    for account_id, account in accounts.items():
        client = b2_client(account)
        bucket = account["bucket"]
        print(f"listing images/ in account {account_id} (bucket {bucket})...")
        keys = list_image_keys(client, bucket)
        print(f"  found {len(keys)} image(s)")
        present |= keys

    all_pages = fetch_all_pages(conn)
    stale = [(page_id, folder, stem, page_no) for page_id, folder, stem, page_no in all_pages
             if (folder, stem, page_no) not in present]

    print(f"\n{len(stale)} of {len(all_pages)} pages row(s) are stale (no image in either account)")
    for _page_id, folder, stem, page_no in stale:
        print(f"  {'DELETE' if EXECUTE else 'would delete'} pages row: {folder}/{stem} page {page_no}")

    if EXECUTE and stale:
        delete_pages(conn, [page_id for page_id, _f, _s, _p in stale])

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        groups = _group_by_stem([((folder, stem), page_no) for _pid, folder, stem, page_no in stale])
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### Remove stale pages\n\n")
            f.write(f"- **Mode:** {'EXECUTE' if EXECUTE else 'dry run'}\n")
            f.write(f"- **{'Deleted' if EXECUTE else 'Would delete'} pages rows:** {len(stale)}\n")
            if groups:
                f.write("\n#### Affected PDFs\n\n")
                for (folder, stem), count, lo, hi in groups:
                    f.write(f"- `{folder}/{stem}`: {count} page(s) (range {lo}-{hi})\n")

    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        changed = EXECUTE and len(stale) > 0
        with open(output_path, "a", encoding="utf-8") as f:
            f.write(f"changed={'true' if changed else 'false'}\n")

    if not EXECUTE:
        print("\nDry run complete -- no changes were made. Re-run with --execute to commit.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
