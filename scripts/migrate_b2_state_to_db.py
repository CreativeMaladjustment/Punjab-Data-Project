"""One-time backfill: discover the .done markers (and, for LLM output, the
per-page JSON files) that process_pcloud.py and extract_with_llm.py wrote to
B2 before this pipeline moved to Postgres/Supabase as the source of truth,
write the equivalent rows into Postgres, and — only once a stem's migration
is confirmed committed, and only in --execute mode — delete the now-redundant
.done marker objects from B2. Page images, page PDFs, and the per-page
extraction JSON content are left in B2 untouched; only .done/.with-pdfs.done
markers are ever deleted here.

Defaults to a dry run (lists what it would do, writes nothing, deletes
nothing). Pass --execute to actually commit. Safe to re-run: every DB write
is an upsert, and a stem already fully migrated (its pcloud_files/pages rows
already match) is just skipped.

Usage:
  python scripts/migrate_b2_state_to_db.py            # dry run
  python scripts/migrate_b2_state_to_db.py --execute   # actually migrate + delete markers
"""
import json
import os
import pathlib
import re
import sys

import boto3
import psycopg2
import requests
from psycopg2.extras import Json

PCLOUD_CODE = os.environ["PCLOUD_CODE"]
PCLOUD_HOSTS = ["api.pcloud.com", "eapi.pcloud.com"]
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

EXECUTE = "--execute" in sys.argv[1:]

IMAGE_KEY_RE = re.compile(r"^images/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.webp$")
PAGE_PDF_KEY_RE = re.compile(r"^pages/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.pdf$")
IMAGES_DONE_RE = re.compile(r"^processed/(?P<folder>.+)/(?P<stem>[^/]+)(?<!\.with-pdfs)\.done$")
PDFS_DONE_RE = re.compile(r"^processed/(?P<folder>.+)/(?P<stem>[^/]+)\.with-pdfs\.done$")
EXTRACTION_PAGE_RE = re.compile(
    r"^extractions/(?P<model_tag>[^/]+)/(?P<folder>.+)/(?P<stem>[^/]+)/page_(?P<page>\d+)\.json$"
)
EXTRACTION_DONE_RE = re.compile(
    r"^extractions/(?P<model_tag>[^/]+)/processed/(?P<folder>.+)/(?P<stem>[^/]+)\.done$"
)


def load_b2_accounts():
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
    """Yield every object {"Key", "LastModified"} under prefix."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def list_model_tags(client, bucket):
    resp = client.list_objects_v2(Bucket=bucket, Prefix="extractions/", Delimiter="/")
    return [p["Prefix"].split("/")[1] for p in resp.get("CommonPrefixes", [])]


def pcloud_get(path, params):
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
            last_error = RuntimeError(f"pCloud API error on {host}/{path}: {data}")
            continue
        return data
    raise RuntimeError(f"pCloud API request failed on all hosts: {last_error}")


def list_pdfs_recursive(code):
    pdfs = []
    visited = set()

    def walk(entries, folder_path, depth):
        if depth > 50:
            return
        for entry in entries:
            if entry.get("isfolder"):
                sub_path = folder_path + [entry["name"]]
                if "contents" in entry:
                    walk(entry["contents"], sub_path, depth + 1)
                    continue
                folderid = entry["folderid"]
                if folderid in visited:
                    continue
                visited.add(folderid)
                data = pcloud_get("showpublink", {"code": code, "folderid": folderid})
                walk(data["metadata"].get("contents", []), sub_path, depth + 1)
            elif entry["name"].lower().endswith(".pdf"):
                pdfs.append({"fileid": entry["fileid"], "name": entry["name"], "folder": "/".join(folder_path)})

    root = pcloud_get("showpublink", {"code": code, "recursive": 1})
    walk(root["metadata"].get("contents", []), [], 0)
    return pdfs


def db_connect():
    conn = psycopg2.connect(SUPABASE_DB_URL)
    conn.autocommit = True
    return conn


def db_upsert_pcloud_file(conn, fileid, name, folder):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pcloud_files (pcloud_fileid, name, folder) VALUES (%s, %s, %s) "
            "ON CONFLICT (pcloud_fileid) DO NOTHING",
            (fileid, name, folder),
        )


def db_set_page_count_if_higher(conn, fileid, page_count):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pcloud_files SET page_count = %s WHERE pcloud_fileid = %s "
            "AND (page_count IS NULL OR page_count < %s)",
            (page_count, fileid, page_count),
        )


def db_mark_image_uploaded(conn, fileid, page_no, account, bucket, image_key, uploaded_at):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pages (pcloud_fileid, page_no, b2_account, b2_bucket, image_key, image_uploaded_at) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (pcloud_fileid, page_no) DO UPDATE SET "
            "b2_account = EXCLUDED.b2_account, b2_bucket = EXCLUDED.b2_bucket, "
            "image_key = EXCLUDED.image_key, image_uploaded_at = EXCLUDED.image_uploaded_at "
            "WHERE pages.image_uploaded_at IS NULL",
            (fileid, page_no, account, bucket, image_key, uploaded_at),
        )


def db_mark_pdf_uploaded(conn, fileid, page_no, account, bucket, page_pdf_key, uploaded_at):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pages (pcloud_fileid, page_no, b2_account, b2_bucket, image_key, "
            "page_pdf_account, page_pdf_bucket, page_pdf_key, page_pdf_uploaded_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (pcloud_fileid, page_no) DO UPDATE SET "
            "page_pdf_account = EXCLUDED.page_pdf_account, page_pdf_bucket = EXCLUDED.page_pdf_bucket, "
            "page_pdf_key = EXCLUDED.page_pdf_key, page_pdf_uploaded_at = EXCLUDED.page_pdf_uploaded_at "
            "WHERE pages.page_pdf_uploaded_at IS NULL",
            # image_key is NOT NULL; if a pages row doesn't exist yet, reuse the pdf key as a
            # placeholder so this insert can't fail — the real image_key (if any) will overwrite
            # it once/if the image migration step for this account runs.
            (fileid, page_no, account, bucket, page_pdf_key, account, bucket, page_pdf_key, uploaded_at),
        )


def db_get_page_id(conn, fileid, page_no):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM pages WHERE pcloud_fileid = %s AND page_no = %s", (fileid, page_no))
        row = cur.fetchone()
        return row[0] if row else None


def _text(entry, key):
    v = entry.get(key)
    return None if v is None else str(v)


def _int_or_none(entry, key):
    v = entry.get(key)
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    return None


def _bool_or_none(entry, key):
    v = entry.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        low = v.strip().lower()
        if low in ("true", "yes", "1"):
            return True
        if low in ("false", "no", "0", ""):
            return False
    return None


INSERT_ENTRY_SQL = """
    INSERT INTO catalogue_entries (
        extraction_id, entry_index, quarter, pdf_page, printed_page, section, lang,
        char_qualifier, topic, serial, reg, copies, printer_verbatim, printer, pcity,
        author, title, title_native, gloss, pp_verbatim, publisher, pubcity, date,
        price, edition, format, method, educ, copyright, notes, marks, flags,
        source_folder, source_pdf
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (extraction_id, entry_index) DO NOTHING
"""


def db_save_extraction(conn, page_id, model_tag, entries):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO llm_extractions (page_id, model, model_tag, status, raw_response) "
            "VALUES (%s, %s, %s, 'success', %s) "
            "ON CONFLICT (page_id, model_tag) DO NOTHING "
            "RETURNING id",
            # The original un-slugified OLLAMA_MODEL string isn't recoverable from a B2 key
            # alone (slugification is lossy) — model_tag is used for both columns here.
            (page_id, model_tag, model_tag, Json(entries)),
        )
        row = cur.fetchone()
        if row is not None:
            extraction_id = row[0]
        else:
            # llm_extractions row already exists from a prior run of this script.
            # That run may have crashed after this insert but before every
            # catalogue_entries row below was written -- look the extraction up
            # and re-run the entry inserts (ON CONFLICT DO NOTHING) rather than
            # assuming "exists" means "fully migrated".
            cur.execute(
                "SELECT id FROM llm_extractions WHERE page_id = %s AND model_tag = %s",
                (page_id, model_tag),
            )
            extraction_id = cur.fetchone()[0]
        for idx, entry in enumerate(entries):
            cur.execute(
                INSERT_ENTRY_SQL,
                (
                    extraction_id, idx,
                    _text(entry, "quarter"), _int_or_none(entry, "pdf_page"), _int_or_none(entry, "printed_page"),
                    _text(entry, "section"), _text(entry, "lang"), _text(entry, "char"), _text(entry, "topic"),
                    _int_or_none(entry, "serial"), _text(entry, "reg"), _text(entry, "copies"),
                    _text(entry, "printer_verbatim"), _text(entry, "printer"), _text(entry, "pcity"),
                    _text(entry, "author"), _text(entry, "title"), _bool_or_none(entry, "title_native"),
                    _text(entry, "gloss"), _text(entry, "pp_verbatim"), _text(entry, "publisher"),
                    _text(entry, "pubcity"), _text(entry, "date"), _text(entry, "price"),
                    _text(entry, "edition"), _text(entry, "format"), _text(entry, "method"),
                    _text(entry, "educ"), _text(entry, "copyright"), _text(entry, "notes"),
                    _text(entry, "marks"), Json(entry.get("flags") or []),
                    _text(entry, "source_folder"), _text(entry, "source_pdf"),
                ),
            )


def migrate_images_and_pdfs(conn, client, bucket, account_id, fileid_by_key):
    print(f"--- account {account_id}: scanning images/, pages/, processed/ in {bucket} ---")
    by_stem_images = {}
    for obj in list_all(client, bucket, "images/"):
        m = IMAGE_KEY_RE.match(obj["Key"])
        if m:
            by_stem_images.setdefault((m["folder"], m["stem"]), []).append(
                (int(m["page"]), obj["Key"], obj["LastModified"])
            )

    by_stem_pdfs = {}
    for obj in list_all(client, bucket, "pages/"):
        m = PAGE_PDF_KEY_RE.match(obj["Key"])
        if m:
            by_stem_pdfs.setdefault((m["folder"], m["stem"]), []).append(
                (int(m["page"]), obj["Key"], obj["LastModified"])
            )

    done_markers = []  # (key, folder, stem)
    for obj in list_all(client, bucket, "processed/"):
        m = IMAGES_DONE_RE.match(obj["Key"]) or PDFS_DONE_RE.match(obj["Key"])
        if m:
            done_markers.append((obj["Key"], m["folder"], m["stem"]))

    stems = set(by_stem_images) | set(by_stem_pdfs)
    print(f"found {len(stems)} stem(s) with images and/or page-pdfs, {len(done_markers)} done marker(s)")

    migrated_stems = set()
    for folder, stem in sorted(stems):
        item = fileid_by_key.get((folder, stem))
        if item is None:
            print(f"WARNING: {folder}/{stem} has B2 objects but no matching pCloud file anymore; skipping")
            continue
        fileid = item["fileid"]
        print(f"  {folder}/{stem} (fileid {fileid}): "
              f"{len(by_stem_images.get((folder, stem), []))} image(s), "
              f"{len(by_stem_pdfs.get((folder, stem), []))} page-pdf(s)")
        if not EXECUTE:
            continue
        db_upsert_pcloud_file(conn, fileid, item["name"], folder)
        images = by_stem_images.get((folder, stem), [])
        pdfs = by_stem_pdfs.get((folder, stem), [])
        if images:
            db_set_page_count_if_higher(conn, fileid, max(p for p, _, _ in images))
        for page_no, key, last_modified in images:
            db_mark_image_uploaded(conn, fileid, page_no, account_id, bucket, key, last_modified)
        for page_no, key, last_modified in pdfs:
            db_mark_pdf_uploaded(conn, fileid, page_no, account_id, bucket, key, last_modified)
        migrated_stems.add((folder, stem))

    if EXECUTE:
        for key, folder, stem in done_markers:
            if (folder, stem) not in migrated_stems:
                print(f"  skipping marker (stem not confirmed migrated): {key}")
                continue
            print(f"  deleting marker: {key}")
            client.delete_object(Bucket=bucket, Key=key)


def migrate_llm_extractions(conn, client, bucket, account_id, fileid_by_key):
    model_tags = list_model_tags(client, bucket)
    if not model_tags:
        return
    print(f"--- account {account_id}: scanning extractions/ ({len(model_tags)} model(s)) in {bucket} ---")

    for model_tag in model_tags:
        pages_by_stem = {}
        done_markers = []  # (key, folder, stem)
        for obj in list_all(client, bucket, f"extractions/{model_tag}/"):
            m = EXTRACTION_PAGE_RE.match(obj["Key"])
            if m:
                pages_by_stem.setdefault((m["folder"], m["stem"]), []).append((int(m["page"]), obj["Key"]))
                continue
            m = EXTRACTION_DONE_RE.match(obj["Key"])
            if m:
                done_markers.append((obj["Key"], m["folder"], m["stem"]))

        print(f"  model {model_tag}: {len(pages_by_stem)} stem(s), {len(done_markers)} done marker(s)")

        migrated_stems = set()
        for (folder, stem), pages in sorted(pages_by_stem.items()):
            item = fileid_by_key.get((folder, stem))
            if item is None:
                print(f"WARNING: {folder}/{stem} has extraction output but no matching pCloud file anymore; skipping")
                continue
            fileid = item["fileid"]
            if not EXECUTE:
                print(f"    {folder}/{stem}: {len(pages)} page(s) to migrate")
                continue
            stem_fully_migrated = True
            for page_no, key in pages:
                page_id = db_get_page_id(conn, fileid, page_no)
                if page_id is None:
                    print(
                        f"WARNING: {folder}/{stem} page {page_no} has extraction output but no "
                        "pages row yet (run the images/pdfs migration first); skipping"
                    )
                    stem_fully_migrated = False
                    continue
                body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
                entries = json.loads(body)
                db_save_extraction(conn, page_id, model_tag, entries)
            if stem_fully_migrated:
                migrated_stems.add((folder, stem))

        if EXECUTE:
            for key, folder, stem in done_markers:
                if (folder, stem) not in migrated_stems:
                    print(f"    skipping marker (stem not fully migrated): {key}")
                    continue
                print(f"    deleting marker: {key}")
                client.delete_object(Bucket=bucket, Key=key)


def main():
    print(f"mode: {'EXECUTE (will write to DB and delete markers)' if EXECUTE else 'DRY RUN (no changes)'}")
    conn = db_connect()
    accounts = load_b2_accounts()

    print("fetching pCloud file listing (to map folder/stem -> fileid)...")
    pdfs = list_pdfs_recursive(PCLOUD_CODE)
    fileid_by_key = {(item["folder"], pathlib.Path(item["name"]).stem): item for item in pdfs}
    print(f"found {len(pdfs)} pCloud PDF(s)")

    for account_id, account in accounts.items():
        client = b2_client(account)
        bucket = account["bucket"]
        migrate_images_and_pdfs(conn, client, bucket, account_id, fileid_by_key)
        migrate_llm_extractions(conn, client, bucket, account_id, fileid_by_key)

    if not EXECUTE:
        print("\nDry run complete — no changes were made. Re-run with --execute to commit.")
    else:
        print("\nMigration complete.")


if __name__ == "__main__":
    main()
