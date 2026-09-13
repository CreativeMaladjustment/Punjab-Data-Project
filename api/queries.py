"""Queries backing the processing dashboard, the generic table browser, and
the QC review page. The dashboard functions below are plain SELECT/
aggregate and never write; fetch_table_page() is also read-only. Only the
QC section (save_qc_verdict, save_human_edit) writes to the database, and
each does so narrowly: save_qc_verdict() logs a verdict and, on
'needs_reprocessing', resets the *reviewed* llm_extractions row so it's
reclaimed by the existing worker loop; save_human_edit() never touches an
existing model's llm_extractions/catalogue_entries rows at all -- it
upserts a single separate row tagged HUMAN_MODEL_TAG per page, so a
person's correction is always distinguishable from, and never overwrites,
what a model actually produced. Kept separate from index.py so the SQL is
easy to find and check against the actual schema (see supabase/
migrations/) without wading through Flask routing code.
"""
import psycopg2.sql
from psycopg2.extras import Json

TOTAL_PAGES_SQL = """
    SELECT count(*) FROM pages WHERE image_uploaded_at IS NOT NULL
"""

# Mirrors extract_with_llm.py's CLAIM_TIMEOUT_SECONDS (not imported from
# there directly, same rationale as scripts/extraction_status.py: that
# module does real work at import time -- connecting to B2, requiring
# OLLAMA_MODEL -- that this read-only dashboard has no reason to need).
# Keep in sync if either changes.
CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60

# One row per model that has ever been run against extract_with_llm.py's
# structured-entry extraction. status/content_failure mirror the same
# columns claim_next_page() and process_page() write -- see
# scripts/extract_with_llm.py and supabase/migrations/
# 20260911120000_add_raw_text_and_attempt_cap.sql for what each means.
#
# 'claimed' is split into active/stale rather than counted as one
# "in progress" bucket: a worker that dies mid-page leaves its claim
# behind for CLAIM_TIMEOUT_SECONDS before another worker (or this one,
# next run) reclaims it (see claim_next_page()) -- scripts/
# extraction_status.py already makes this same active/stale distinction
# for exactly that reason, and folding both into one count here would
# show abandoned work as "in progress" for up to 3 hours after the
# worker that claimed it actually died.
EXTRACTION_SUMMARY_SQL = """
    SELECT
        model_tag,
        count(*) AS total_attempted,
        count(*) FILTER (WHERE status = 'success') AS success,
        count(*) FILTER (WHERE status = 'failed' AND content_failure) AS content_failed,
        count(*) FILTER (WHERE status = 'failed' AND NOT content_failure) AS transient_failed,
        count(*) FILTER (
            WHERE status = 'claimed' AND claimed_at >= now() - %(claim_timeout)s * interval '1 second'
        ) AS claimed_active,
        count(*) FILTER (
            WHERE status = 'claimed' AND claimed_at < now() - %(claim_timeout)s * interval '1 second'
        ) AS claimed_stale
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

        cur.execute(EXTRACTION_SUMMARY_SQL, {"claim_timeout": CLAIM_TIMEOUT_SECONDS})
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
            "extraction_claimed_active": row["claimed_active"],
            "extraction_claimed_stale": row["claimed_stale"],
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
                "extraction_claimed_active": 0,
                "extraction_claimed_stale": 0,
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


# ---------------------------------------------------------------------------
# Generic table browser
# ---------------------------------------------------------------------------

# information_schema, not pg_catalog directly, for portability/readability;
# performance doesn't matter here (an admin-only page, queried on demand).
LIST_TABLES_SQL = """
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
"""


def list_tables(conn):
    """[{"name", "kind"}] for every base table and view in the public
    schema -- table_type is 'BASE TABLE' or 'VIEW'. Used both to render
    the table-browser's index and, critically, as the only source of truth
    fetch_table_page() trusts for a table name: it never interpolates a
    caller-supplied name into SQL without first checking it's exactly one
    of these."""
    with conn.cursor() as cur:
        cur.execute(LIST_TABLES_SQL)
        return [{"name": name, "kind": "view" if kind == "VIEW" else "table"} for name, kind in cur.fetchall()]


def list_columns(conn, table_name):
    """Ordered column names for `table_name`, straight from the catalog --
    used both to render the browser's headers/filter inputs and, like
    list_tables(), as the allowlist fetch_table_page() checks a
    caller-supplied column name against before using it in SQL."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %(table_name)s
            ORDER BY ordinal_position
            """,
            {"table_name": table_name},
        )
        return [row[0] for row in cur.fetchall()]


def fetch_table_page(conn, table_name, columns, filters, sort_col, sort_dir, page, per_page):
    """Paginated, filtered, sorted rows from `table_name`. table_name and
    every key in filters/sort_col MUST already have been checked against
    list_tables()/list_columns() by the caller (see index.py's
    _table_or_404()/_column_or_none()) -- this function itself only
    trusts psycopg2.sql.Identifier to quote them safely, it doesn't
    re-validate. filters is {column: substring} -- every value is matched
    case-insensitively as a substring, via casting the column to text, so
    one code path works across every column type without per-type logic
    (at the cost of not being index-sargable, an acceptable trade for a
    login-gated admin tool that's never the pipeline's hot path).
    Returns (rows_as_dicts, total_row_count).
    """
    table_ident = psycopg2.sql.Identifier(table_name)
    where_parts = []
    params = {}
    for i, (col, value) in enumerate(filters.items()):
        key = f"filter_{i}"
        where_parts.append(
            psycopg2.sql.SQL("{}::text ILIKE {}").format(
                psycopg2.sql.Identifier(col), psycopg2.sql.Placeholder(key)
            )
        )
        params[key] = f"%{value}%"
    where_sql = psycopg2.sql.SQL(" AND ").join(where_parts) if where_parts else psycopg2.sql.SQL("true")

    order_sql = psycopg2.sql.SQL("")
    if sort_col:
        order_sql = psycopg2.sql.SQL("ORDER BY {} {}").format(
            psycopg2.sql.Identifier(sort_col),
            psycopg2.sql.SQL("DESC" if sort_dir == "desc" else "ASC"),
        )

    with conn.cursor() as cur:
        cur.execute(
            psycopg2.sql.SQL("SELECT count(*) FROM {} WHERE {}").format(table_ident, where_sql),
            params,
        )
        (total,) = cur.fetchone()

        params_with_paging = dict(params, limit=per_page, offset=(page - 1) * per_page)
        cur.execute(
            psycopg2.sql.SQL(
                "SELECT {} FROM {} WHERE {} {} LIMIT %(limit)s OFFSET %(offset)s"
            ).format(
                psycopg2.sql.SQL(", ").join(psycopg2.sql.Identifier(c) for c in columns),
                table_ident,
                where_sql,
                order_sql,
            ),
            params_with_paging,
        )
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    return rows, total


# ---------------------------------------------------------------------------
# QC review page
# ---------------------------------------------------------------------------

# model/model_tag a human correction is stored under (see save_human_edit())
# -- distinct from any real Ollama model tag, so it's unambiguous in every
# view/filter that groups or displays by model_tag, including the
# dashboard and the generic table browser above.
HUMAN_MODEL_TAG = "human-review"

# Editable catalogue_entries fields, in schema order, excluding id/
# extraction_id/entry_index (identity, not something a reviewer edits) and
# created_at (set by the DB). "flags" is jsonb; everything else here is
# plain text except the two marked otherwise -- see CATALOGUE_ENTRY_FIELD_TYPES.
CATALOGUE_ENTRY_FIELDS = [
    "quarter", "pdf_page", "printed_page", "section", "lang", "char_qualifier",
    "topic", "serial", "reg", "copies", "printer_verbatim", "printer", "pcity",
    "author", "title", "title_native", "gloss", "pp_verbatim", "publisher",
    "pubcity", "date", "price", "edition", "format", "method", "educ",
    "copyright", "notes", "marks", "flags", "source_folder", "source_pdf",
]
CATALOGUE_ENTRY_INT_FIELDS = {"pdf_page", "printed_page", "serial"}
CATALOGUE_ENTRY_BOOL_FIELDS = {"title_native"}
CATALOGUE_ENTRY_JSON_FIELDS = {"flags"}


def fetch_qc_page(conn, page_id, model_tag=None):
    """Everything the QC template needs for one page: its image location,
    every model_tag that has attempted it, the selected extraction (default:
    the first model_tag) and its entries, any existing human correction, the
    page's OCR text(s), this extraction's review history, and prev/next
    page ids for navigation. Returns None if page_id doesn't exist."""
    with conn.cursor() as cur:
        # image_uploaded_at IS NOT NULL excludes placeholder pages
        # process_pcloud.py has recorded but not yet uploaded an image for
        # -- same predicate the dashboard's TOTAL_PAGES_SQL already uses.
        # Without it, a direct /qc/<id> hit (or prev/next navigation) could
        # land on a page with nothing to show and a permanently-broken
        # image link.
        cur.execute(
            """
            SELECT p.id, p.page_no, p.b2_account, p.b2_bucket, p.image_key,
                   pf.pcloud_fileid, pf.name AS pcloud_name, pf.folder AS pcloud_folder
            FROM pages p
            JOIN pcloud_files pf ON pf.pcloud_fileid = p.pcloud_fileid
            WHERE p.id = %(page_id)s AND p.image_uploaded_at IS NOT NULL
            """,
            {"page_id": page_id},
        )
        row = cur.fetchone()
        if row is None:
            return None
        page = dict(zip([d.name for d in cur.description], row))

        cur.execute(
            "SELECT DISTINCT model_tag FROM llm_extractions "
            "WHERE page_id = %(page_id)s AND model_tag <> %(human_tag)s ORDER BY model_tag",
            {"page_id": page_id, "human_tag": HUMAN_MODEL_TAG},
        )
        model_tags = [r[0] for r in cur.fetchall()]
        selected_tag = model_tag if model_tag in model_tags else (model_tags[0] if model_tags else None)

        def _extraction_and_entries(tag):
            if tag is None:
                return None, []
            cur.execute(
                "SELECT id, status, error_message, raw_text, attempt_count, content_failure, created_at "
                "FROM llm_extractions WHERE page_id = %(page_id)s AND model_tag = %(model_tag)s",
                {"page_id": page_id, "model_tag": tag},
            )
            erow = cur.fetchone()
            if erow is None:
                return None, []
            extraction = dict(zip([d.name for d in cur.description], erow))
            cur.execute(
                "SELECT * FROM catalogue_entries WHERE extraction_id = %(extraction_id)s ORDER BY entry_index",
                {"extraction_id": extraction["id"]},
            )
            cols = [d.name for d in cur.description]
            entries = [dict(zip(cols, r)) for r in cur.fetchall()]
            return extraction, entries

        extraction, entries = _extraction_and_entries(selected_tag)
        human_extraction, human_entries = _extraction_and_entries(HUMAN_MODEL_TAG)

        cur.execute(
            "SELECT model_tag, status, raw_text FROM page_ocr_text "
            "WHERE page_id = %(page_id)s ORDER BY model_tag",
            {"page_id": page_id},
        )
        ocr_cols = [d.name for d in cur.description]
        ocr_rows = [dict(zip(ocr_cols, r)) for r in cur.fetchall()]

        reviews = []
        if extraction:
            cur.execute(
                "SELECT verdict, note, created_at FROM qc_reviews "
                "WHERE extraction_id = %(extraction_id)s ORDER BY created_at DESC",
                {"extraction_id": extraction["id"]},
            )
            rcols = [d.name for d in cur.description]
            reviews = [dict(zip(rcols, r)) for r in cur.fetchall()]

        cur.execute(
            "SELECT min(id) FROM pages WHERE id > %(page_id)s AND image_uploaded_at IS NOT NULL",
            {"page_id": page_id},
        )
        (next_id,) = cur.fetchone()
        cur.execute(
            "SELECT max(id) FROM pages WHERE id < %(page_id)s AND image_uploaded_at IS NOT NULL",
            {"page_id": page_id},
        )
        (prev_id,) = cur.fetchone()

    return {
        "page": page,
        "model_tags": model_tags,
        "selected_tag": selected_tag,
        "extraction": extraction,
        "entries": entries,
        "human_extraction": human_extraction,
        "human_entries": human_entries,
        "ocr_rows": ocr_rows,
        "reviews": reviews,
        "prev_id": prev_id,
        "next_id": next_id,
    }


def qc_verdict_target(conn, extraction_id):
    """Look up the (page_id, model_tag) an extraction_id may be verdicted
    against, plus whether it's currently under an *active* claim. Excludes
    HUMAN_MODEL_TAG rows entirely -- the QC form never renders a verdict
    control for a human correction, so an id resolving to one here only
    happens via a crafted request, and there's no model attempt behind it
    to judge. Returns None if extraction_id doesn't exist (or is a human
    row); otherwise (page_id, model_tag, status, actively_claimed).
    actively_claimed mirrors the dashboard's active/stale split: true only
    while a worker could plausibly still be mid-page on it, so callers can
    refuse to act on a row a worker might overwrite moments later.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT page_id, model_tag, status,
                   (status = 'claimed' AND claimed_at >= now() - %(claim_timeout)s * interval '1 second')
            FROM llm_extractions
            WHERE id = %(extraction_id)s AND model_tag <> %(human_tag)s
            """,
            {"extraction_id": extraction_id, "human_tag": HUMAN_MODEL_TAG, "claim_timeout": CLAIM_TIMEOUT_SECONDS},
        )
        return cur.fetchone()


def save_qc_verdict(conn, extraction_id, verdict, note):
    """Log a verdict against `extraction_id`. On 'needs_reprocessing', also
    reset that same llm_extractions row to look like a stale, non-content
    (i.e. unconditionally reclaimable) failure: status='failed',
    content_failure=false, claimed_at pushed further into the past than
    CLAIM_TIMEOUT_SECONDS (the same constant claim_next_page() itself uses
    for staleness, rather than a separately hardcoded duration that could
    silently drift out of sync with it). That's exactly the shape
    claim_next_page()'s CLAIM_NEXT_PAGE_SQL already treats as immediately
    reclaimable -- see scripts/extract_with_llm.py -- so the existing
    worker loop picks the page back up and retries on its own next
    scheduled run; no new pipeline logic needed. raw_response/raw_text/
    attempt_count are left untouched so the previous (wrong) output stays
    visible for comparison once the retry completes.

    Callers must have already checked qc_verdict_target() themselves --
    this function trusts extraction_id and doesn't re-validate status or
    the active-claim race, since the caller needed that same lookup
    anyway to know what to redirect back to.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO qc_reviews (extraction_id, verdict, note) VALUES (%(extraction_id)s, %(verdict)s, %(note)s)",
            {"extraction_id": extraction_id, "verdict": verdict, "note": note or None},
        )
        if verdict == "needs_reprocessing":
            cur.execute(
                """
                UPDATE llm_extractions
                SET status = 'failed', content_failure = false,
                    claimed_at = now() - (%(claim_timeout)s + 1) * interval '1 second'
                WHERE id = %(extraction_id)s
                """,
                {"extraction_id": extraction_id, "claim_timeout": CLAIM_TIMEOUT_SECONDS},
            )
    conn.commit()


def save_human_edit(conn, page_id, entries):
    """Upsert the single HUMAN_MODEL_TAG llm_extractions row for this page
    and replace its catalogue_entries wholesale with `entries` (each a
    dict keyed by CATALOGUE_ENTRY_FIELDS). Never reads or writes any other
    model's llm_extractions/catalogue_entries rows -- a correction is
    always a distinct, clearly-labeled row, not an edit to what a model
    actually produced. entries is small (a page's worth of catalogue rows,
    realistically 0-10) so a delete-then-reinsert per save is simpler than
    diffing, and correct even if entries were reordered or removed.
    Returns the human-review extraction's id.
    """
    with conn.cursor() as cur:
        # created_at is also bumped on conflict -- there's no separate
        # updated_at column on this table, and without this the QC page's
        # "Last edited" display would keep showing the time of the first
        # correction forever, no matter how many times it's since been
        # re-edited.
        cur.execute(
            """
            INSERT INTO llm_extractions (page_id, model, model_tag, status)
            VALUES (%(page_id)s, 'human', %(model_tag)s, 'success')
            ON CONFLICT (page_id, model_tag) DO UPDATE SET status = 'success', created_at = now()
            RETURNING id
            """,
            {"page_id": page_id, "model_tag": HUMAN_MODEL_TAG},
        )
        (extraction_id,) = cur.fetchone()

        cur.execute("DELETE FROM catalogue_entries WHERE extraction_id = %(extraction_id)s", {"extraction_id": extraction_id})

        cols = ["extraction_id", "entry_index"] + CATALOGUE_ENTRY_FIELDS
        insert_sql = psycopg2.sql.SQL("INSERT INTO catalogue_entries ({}) VALUES ({})").format(
            psycopg2.sql.SQL(", ").join(psycopg2.sql.Identifier(c) for c in cols),
            psycopg2.sql.SQL(", ").join(psycopg2.sql.Placeholder() for _ in cols),
        )
        for index, entry in enumerate(entries):
            values = [extraction_id, index]
            for field in CATALOGUE_ENTRY_FIELDS:
                value = entry.get(field)
                if field in CATALOGUE_ENTRY_JSON_FIELDS and value is not None:
                    value = Json(value)
                values.append(value)
            cur.execute(insert_sql, values)
    conn.commit()
    return extraction_id
