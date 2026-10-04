"""Audit ditto marks in extracted catalogue entries.

Finds the top pages by ditto mark count in each field and shows:
1. The catalogue entries extracted from those pages
2. The full OCR text used for that extraction
3. Context for how ditto marks appear in the raw data
"""
import json
import os
import sys

import psycopg2
from psycopg2 import sql

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Fields that commonly have ditto marks, with their ditto variants
DITTO_PATTERNS = {
    "author": ["ditto.", "ditto", "do.", "Do.", "-do-"],
    "printer": ["ditto.", "ditto", "do.", "Do.", "-do-"],
    "pcity": ["ditto.", "ditto", "do.", "Do.", "-do-"],
    "publisher": ["ditto.", "ditto", "do.", "Do.", "-do-"],
    "pubcity": ["ditto.", "ditto", "do.", "Do.", "-do-"],
}

TOP_N_PAGES = 5  # Show top 5 pages per field with most dittos

DB_CONNECT_MAX_ATTEMPTS = 5


def db_connect():
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError as exc:
            if attempt == DB_CONNECT_MAX_ATTEMPTS:
                raise
            print(f"DB connect attempt {attempt}/{DB_CONNECT_MAX_ATTEMPTS} failed; retrying", file=sys.stderr)
            import time
            time.sleep(2**attempt)


def find_top_pages_by_ditto_count(field_name):
    """Find top pages with most ditto marks in a specific field."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            # Count ditto marks per page for this field using parameterized queries
            # to prevent SQL injection; use sql.Identifier for field names
            field_ref = sql.Identifier("ce", field_name)
            query = sql.SQL("""
                WITH ditto_counts AS (
                  SELECT
                    p.id,
                    p.page_no,
                    pf.folder,
                    pf.name,
                    COUNT(*) as ditto_count
                  FROM pages p
                  JOIN pcloud_files pf ON pf.pcloud_fileid = p.pcloud_fileid
                  JOIN llm_extractions le ON le.page_id = p.id
                  JOIN catalogue_entries ce ON ce.extraction_id = le.id
                  WHERE le.status = 'success'
                    AND {field} IS NOT NULL
                    AND LOWER({field}) SIMILAR TO %(pattern)s
                  GROUP BY p.id, p.page_no, pf.folder, pf.name
                )
                SELECT id, page_no, folder, name, ditto_count
                FROM ditto_counts
                ORDER BY ditto_count DESC
                LIMIT %(limit)s
            """).format(field=field_ref)
            cur.execute(query, {
                "pattern": "%(ditto|ditto\\.|do|do\\.|do\\-|^\\-do\\-)%",
                "limit": TOP_N_PAGES
            })
            return field_name, cur.fetchall()


def is_ditto_mark(value):
    """Check if a value is a ditto mark variant."""
    if not value:
        return False
    lower = value.lower()
    return any(x in lower for x in ["ditto", "do.", "-do-", "do"])


def show_page_details(page_id, page_no, folder, name):
    """Show extracted entries and OCR text for a page."""
    print(f"\n{'='*80}")
    print(f"PAGE: {folder}/{name} (page_no={page_no}, id={page_id})")
    print(f"{'='*80}")

    with db_connect() as conn:
        with conn.cursor() as cur:
            # Show catalogue entries for this page
            print("\n--- EXTRACTED CATALOGUE ENTRIES ---")
            cur.execute("""
                SELECT le.id, le.model_tag, le.status, ce.entry_index,
                       ce.printer, ce.pcity, ce.author, ce.publisher, ce.pubcity,
                       ce.title, ce.date, ce.serial
                FROM llm_extractions le
                JOIN catalogue_entries ce ON ce.extraction_id = le.id
                WHERE le.page_id = %s AND le.status = 'success'
                ORDER BY le.model_tag, ce.entry_index
            """, (page_id,))

            entries = cur.fetchall()
            ditto_entries = []

            for extraction_id, model_tag, status, entry_index, printer, pcity, author, publisher, pubcity, title, date, serial in entries:
                has_ditto = False
                print(f"\n  Entry {entry_index}:")
                print(f"    serial: {serial}")
                print(f"    author: {author}", end="")
                if is_ditto_mark(author):
                    print(" ⚠️  DITTO", end="")
                    has_ditto = True
                print()

                title_display = (title[:80] + "...") if title and len(title) > 80 else title
                print(f"    title: {title_display}")
                print(f"    date: {date}")
                print(f"    printer: {printer}", end="")
                if is_ditto_mark(printer):
                    print(" ⚠️  DITTO", end="")
                    has_ditto = True
                print()

                print(f"    pcity: {pcity}", end="")
                if is_ditto_mark(pcity):
                    print(" ⚠️  DITTO", end="")
                    has_ditto = True
                print()

                print(f"    publisher: {publisher}", end="")
                if is_ditto_mark(publisher):
                    print(" ⚠️  DITTO", end="")
                    has_ditto = True
                print()

                print(f"    pubcity: {pubcity}", end="")
                if is_ditto_mark(pubcity):
                    print(" ⚠️  DITTO", end="")
                    has_ditto = True
                print()

                if has_ditto:
                    ditto_entries.append(entry_index)

            print(f"\n  Summary: {len(ditto_entries)} entries with ditto marks on this page")

            # Show OCR text for this page
            print("\n--- FULL PAGE OCR TEXT (first 2000 chars) ---")
            cur.execute("""
                SELECT raw_text, model_tag
                FROM page_ocr_text
                WHERE page_id = %s AND status = 'success'
                ORDER BY created_at DESC
                LIMIT 1
            """, (page_id,))

            ocr_row = cur.fetchone()
            if ocr_row:
                ocr_text, ocr_model = ocr_row
                print(f"\n[Source: {ocr_model}]\n")
                print(ocr_text[:2000])
                if len(ocr_text) > 2000:
                    print(f"\n... (text continues, total {len(ocr_text)} characters)")
            else:
                print("(No OCR text found for this page)")


def main():
    print("Auditing ditto marks in catalogue entries...")
    print(f"Finding top {TOP_N_PAGES} pages per field with most ditto marks\n")

    all_pages_seen = set()

    for field_name in sorted(DITTO_PATTERNS.keys()):
        print(f"\n{'='*80}")
        print(f"TOP PAGES BY DITTO COUNT IN '{field_name.upper()}' FIELD")
        print(f"{'='*80}")

        field, pages = find_top_pages_by_ditto_count(field_name)

        if not pages:
            print(f"No pages found with ditto marks in {field}")
            continue

        print(f"Found {len(pages)} pages with ditto marks:\n")
        for page_id, page_no, folder, name, ditto_count in pages:
            print(f"  Page {page_no} ({folder}/{name}): {ditto_count} entries with ditto")
            all_pages_seen.add((page_id, page_no, folder, name))

    # Now show details for all unique pages we found
    print(f"\n\n{'='*80}")
    print(f"DETAILED VIEW: {len(all_pages_seen)} PAGES WITH DITTO MARKS")
    print(f"{'='*80}")

    for page_id, page_no, folder, name in sorted(all_pages_seen):
        show_page_details(page_id, page_no, folder, name)

    print(f"\n{'='*80}")
    print(f"Audit complete — reviewed {len(all_pages_seen)} unique pages")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
