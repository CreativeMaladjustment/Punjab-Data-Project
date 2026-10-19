"""Test compression on large Supabase records.

Usage:
    export SUPABASE_DB_URL="postgresql://user:pass@host/db"
    export PAGE_ID=123  # optional: specific page to test
    python scripts/test_compression.py
"""
import gzip
import json
import os
import sys

import psycopg2


def test_compression():
    """Fetch a large record and test gzip compression."""
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("ERROR: SUPABASE_DB_URL not set", file=sys.stderr)
        sys.exit(1)

    page_id = os.environ.get("PAGE_ID")

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Find the largest record(s)
        if page_id:
            query = """
                SELECT p.id, p.title, pot.raw_text
                FROM pages p
                LEFT JOIN page_ocr_text pot ON pot.page_id = p.id AND pot.status = 'success'
                WHERE p.id = %s AND pot.raw_text IS NOT NULL
                LIMIT 1
            """
            cur.execute(query, (int(page_id),))
        else:
            query = """
                SELECT p.id, p.title, pot.raw_text
                FROM pages p
                LEFT JOIN page_ocr_text pot ON pot.page_id = p.id AND pot.status = 'success'
                WHERE pot.raw_text IS NOT NULL
                ORDER BY LENGTH(pot.raw_text) DESC
                LIMIT 1
            """
            cur.execute(query)

        result = cur.fetchone()
        if not result:
            print("No large records found", file=sys.stderr)
            sys.exit(1)

        page_id, title, raw_text = result
        original_size = len(raw_text.encode("utf-8"))

        # Test gzip compression
        compressed = gzip.compress(raw_text.encode("utf-8"), compresslevel=9)
        compressed_size = len(compressed)
        compression_ratio = (
            1 - (compressed_size / original_size)
        ) * 100 if original_size > 0 else 0

        # Print results
        print(f"\n{'='*60}")
        print(f"Compression Test Results")
        print(f"{'='*60}")
        print(f"Page ID:            {page_id}")
        print(f"Title:              {title}")
        print(f"\nOriginal size:      {original_size:,} bytes ({original_size / 1024:.2f} KB)")
        print(f"Compressed size:    {compressed_size:,} bytes ({compressed_size / 1024:.2f} KB)")
        print(f"Compression ratio:  {compression_ratio:.2f}%")
        print(f"Savings:            {original_size - compressed_size:,} bytes ({(original_size - compressed_size) / 1024:.2f} KB)")
        print(f"{'='*60}\n")

        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    test_compression()
