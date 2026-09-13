"""Read-only aggregate queries backing the processing dashboard. Every query
here is a plain SELECT/aggregate -- this module never writes to the
database. Kept separate from index.py so the SQL is easy to find and check
against the actual schema (see supabase/migrations/) without wading through
Flask routing code.
"""

TOTAL_PAGES_SQL = """
    SELECT count(*) FROM pages WHERE image_uploaded_at IS NOT NULL
"""

# One row per model that has ever been run against extract_with_llm.py's
# structured-entry extraction. status/content_failure mirror the same
# columns claim_next_page() and process_page() write -- see
# scripts/extract_with_llm.py and supabase/migrations/
# 20260911120000_add_raw_text_and_attempt_cap.sql for what each means.
EXTRACTION_SUMMARY_SQL = """
    SELECT
        model_tag,
        count(*) AS total_attempted,
        count(*) FILTER (WHERE status = 'success') AS success,
        count(*) FILTER (WHERE status = 'failed' AND content_failure) AS content_failed,
        count(*) FILTER (WHERE status = 'failed' AND NOT content_failure) AS transient_failed,
        count(*) FILTER (WHERE status = 'claimed') AS in_progress
    FROM llm_extractions
    GROUP BY model_tag
    ORDER BY model_tag
"""

# Total catalogue_entries rows produced per model -- the actual extracted
# table data, as opposed to how many *pages* succeeded above.
ENTRIES_PER_MODEL_SQL = """
    SELECT le.model_tag, count(ce.id) AS total_entries
    FROM catalogue_entries ce
    JOIN llm_extractions le ON le.id = ce.extraction_id
    GROUP BY le.model_tag
"""

# One row per model that has run the independent full-page OCR pass (see
# scripts/extract_with_llm.py's ocr_full_page() and
# supabase/migrations/20260912160000_add_page_ocr_text.sql). Fully separate
# from EXTRACTION_SUMMARY_SQL above -- a page can succeed at one and fail
# the other.
OCR_SUMMARY_SQL = """
    SELECT
        model_tag,
        count(*) AS total_attempted,
        count(*) FILTER (WHERE status = 'success') AS success,
        count(*) FILTER (WHERE status = 'failed') AS failed
    FROM page_ocr_text
    GROUP BY model_tag
    ORDER BY model_tag
"""


def fetch_dashboard_data(conn):
    """Run every query above and merge extraction/entries/OCR stats into one
    per-model_tag dict, so the template only has to iterate one structure."""
    with conn.cursor() as cur:
        cur.execute(TOTAL_PAGES_SQL)
        (total_pages,) = cur.fetchone()

        cur.execute(EXTRACTION_SUMMARY_SQL)
        extraction_cols = [d.name for d in cur.description]
        extraction_rows = [dict(zip(extraction_cols, row)) for row in cur.fetchall()]

        cur.execute(ENTRIES_PER_MODEL_SQL)
        entries_by_model = dict(cur.fetchall())

        cur.execute(OCR_SUMMARY_SQL)
        ocr_cols = [d.name for d in cur.description]
        ocr_rows = [dict(zip(ocr_cols, row)) for row in cur.fetchall()]

    models = {}
    for row in extraction_rows:
        tag = row["model_tag"]
        models[tag] = {
            "model_tag": tag,
            "extraction_attempted": row["total_attempted"],
            "extraction_success": row["success"],
            "extraction_content_failed": row["content_failed"],
            "extraction_transient_failed": row["transient_failed"],
            "extraction_in_progress": row["in_progress"],
            "total_entries": entries_by_model.get(tag, 0),
            "ocr_attempted": 0,
            "ocr_success": 0,
            "ocr_failed": 0,
        }
    for row in ocr_rows:
        tag = row["model_tag"]
        models.setdefault(
            tag,
            {
                "model_tag": tag,
                "extraction_attempted": 0,
                "extraction_success": 0,
                "extraction_content_failed": 0,
                "extraction_transient_failed": 0,
                "extraction_in_progress": 0,
                "total_entries": 0,
                "ocr_attempted": 0,
                "ocr_success": 0,
                "ocr_failed": 0,
            },
        )
        models[tag]["ocr_attempted"] = row["total_attempted"]
        models[tag]["ocr_success"] = row["success"]
        models[tag]["ocr_failed"] = row["failed"]

    return {
        "total_pages": total_pages,
        "models": sorted(models.values(), key=lambda m: m["model_tag"]),
    }
