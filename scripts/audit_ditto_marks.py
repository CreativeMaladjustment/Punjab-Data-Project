"""Audit ditto marks in extracted catalogue entries.

Finds pages with potential ditto marks in key fields and shows:
1. The catalogue entries extracted from that page
2. The full OCR text used for that extraction
3. Context for how ditto marks appear in the raw data
"""
import json
import os
import sys

import psycopg2

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Fields that commonly have ditto marks
DITTO_FIELDS = ["printer", "pcity", "author", "publisher", "pubcity"]

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


def find_pages_with_ditto():
    """Find pages that have ditto marks in catalogue entries."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            # Find pages where any entry has "ditto" or "do." in key fields
            query = """
                SELECT DISTINCT p.id, p.page_no, pf.folder, pf.name
                FROM pages p
                JOIN pcloud_files pf ON pf.pcloud_fileid = p.pcloud_fileid
                JOIN llm_extractions le ON le.page_id = p.id
                JOIN catalogue_entries ce ON ce.extraction_id = le.id
                WHERE le.status = 'success'
                  AND (
                    LOWER(ce.printer) SIMILAR TO '%ditto|do\.|do$'
                    OR LOWER(ce.pcity) SIMILAR TO '%ditto|do\.|do$'
                    OR LOWER(ce.author) SIMILAR TO '%ditto|do\.|do$'
                    OR LOWER(ce.publisher) SIMILAR TO '%ditto|do\.|do$'
                    OR LOWER(ce.pubcity) SIMILAR TO '%ditto|do\.|do$'
                  )
                ORDER BY p.id
                LIMIT 10
            """
            cur.execute(query)
            return cur.fetchall()


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
            for extraction_id, model_tag, status, entry_index, printer, pcity, author, publisher, pubcity, title, date, serial in entries:
                print(f"\n  Entry {entry_index} [model: {model_tag}]:")
                print(f"    serial: {serial}")
                print(f"    author: {author}")
                print(f"    title: {title}")
                print(f"    date: {date}")
                print(f"    printer: {printer}")
                print(f"    pcity: {pcity}")
                print(f"    publisher: {publisher}")
                print(f"    pubcity: {pubcity}")

                # Highlight ditto marks
                for field_val, field_name in [
                    (printer, "printer"),
                    (pcity, "pcity"),
                    (author, "author"),
                    (publisher, "publisher"),
                    (pubcity, "pubcity")
                ]:
                    if field_val and any(x in field_val.lower() for x in ["ditto", "do.", "do"]):
                        print(f"    ⚠️  DITTO FOUND in {field_name}: '{field_val}'")

            # Show OCR text for this page
            print("\n--- FULL PAGE OCR TEXT ---")
            cur.execute("""
                SELECT raw_text, model_tag, status
                FROM page_ocr_text
                WHERE page_id = %s AND status = 'success'
                ORDER BY created_at DESC
                LIMIT 1
            """, (page_id,))

            ocr_row = cur.fetchone()
            if ocr_row:
                ocr_text, ocr_model, ocr_status = ocr_row
                print(f"[OCR source: {ocr_model}]")
                print(f"\n{ocr_text[:2000]}...")
                if len(ocr_text) > 2000:
                    print(f"\n... (total {len(ocr_text)} characters)")
            else:
                print("(No OCR text found for this page)")


def main():
    print("Auditing ditto marks in catalogue entries...")
    print(f"Looking for pages with '{', '.join(DITTO_FIELDS)}' containing ditto variants")

    pages_with_ditto = find_pages_with_ditto()
    print(f"\nFound {len(pages_with_ditto)} pages with ditto marks (showing up to 10):\n")

    for page_id, page_no, folder, name in pages_with_ditto:
        show_page_details(page_id, page_no, folder, name)

    print(f"\n{'='*80}")
    print("Audit complete")


if __name__ == "__main__":
    main()
