"""Queries backing the processing dashboard, the generic table browser, and
the QC review page. The dashboard functions below are plain SELECT/
aggregate and never write; fetch_table_page() is also read-only. Only the
QC section (apply_qc_verdict, save_human_edit) writes to the database, and
each does so narrowly: apply_qc_verdict() logs a verdict and, on
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
    SELECT count(*) FROM pages WHERE image_uploaded_at IS NOT NULL AND excluded_at IS NULL
"""

# Mirrors extract_with_llm.py's CLAIM_TIMEOUT_SECONDS/MAX_ATTEMPTS_PER_PAGE
# (not imported from there directly, same rationale as
# scripts/extraction_status.py: that module does real work at import time
# -- connecting to B2, requiring OLLAMA_MODEL -- that this read-only
# dashboard has no reason to need). Keep in sync if either changes.
CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60
MAX_ATTEMPTS_PER_PAGE = 2

# model/model_tag a human correction is stored under (see save_human_edit()
# further down) -- defined up here too since the dashboard queries below
# need to exclude it: a correction is not a model attempt, and without this
# exclusion it would show up as its own "model" row on the processing
# dashboard, with a fake 100%-success rate and its own entry count.
#
# The colon is load-bearing, not stylistic: extract_with_llm.py derives
# every real model_tag by slugifying OLLAMA_MODEL through
# `re.sub(r"[^A-Za-z0-9._-]", "-", OLLAMA_MODEL)`, which replaces any
# character outside A-Za-z0-9._- with a dash -- so no real pipeline run,
# whatever OLLAMA_MODEL is set to, can ever produce a model_tag containing
# a colon. Using "human-review" (all slugify-legal characters) would have
# meant a real model actually named "human-review" could collide with
# this reserved tag: its extraction would upsert into the same
# (page_id, model_tag) row save_human_edit() treats as a correction,
# silently mixing a person's edits with real model output -- exactly what
# this feature exists to prevent. The colon closes that off structurally
# rather than by convention.
HUMAN_MODEL_TAG = "human:review"

# The model whose output counts as a page's extraction when no human
# correction exists yet -- see fetch_corpus_stats() below, which needs
# exactly one row-set per page (not one per model that happened to run
# against it) to report a real entry count. Matches MODEL_ROLE_LABELS'
# "purpose-built document OCR · default" tag on the Progress page in
# api/index.py -- same model, formalized here as the one this query treats
# as canonical rather than just a cosmetic label.
PRIMARY_MODEL_TAG = "glm-ocr"

# Mirrors scripts/extract_with_gemini.py's GEMMA_OCR_ONLY_TAGS -- not
# imported, same rationale as this module's other duplicated constants
# (that script does real work at import time -- requiring GEMINI_API_KEY --
# this read-only dashboard has no reason to need). These two tags' 'success'
# rows are an OCR-completion marker with zero catalogue_entries, never a
# real extraction verdict: excluded below from PAGES_ANY_EXTRACTED_SQL (a
# page Gemma merely OCR'd shouldn't count as "has data extracted" until
# something has actually extracted structured data from it) and from the
# QC page's needs_review queries further down (an empty entry list has
# nothing for a person to review). Keep in sync if either script's own set
# changes.
GEMMA_OCR_ONLY_TAGS = ("gemma-4-31b-it", "gemma-4-26b-a4b-it")

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
# Every query below that groups llm_extractions/page_ocr_text by model_tag
# joins back to `pages` and filters p.excluded_at IS NULL, matching
# TOTAL_PAGES_SQL above -- so an excluded page's rows (if it had already
# been attempted before someone excluded it) drop out of every numerator
# the same way the page itself drops out of the denominator. Without this,
# excluding an already-attempted page would leave its old rows still
# counted in e.g. extraction_attempted while total_pages shrank underneath
# it, so "remaining" (total_pages - total_attempted + ...) could go
# negative and a done_pct bar could read over 100%.
EXTRACTION_SUMMARY_SQL = """
    SELECT
        le.model_tag,
        count(*) AS total_attempted,
        count(*) FILTER (WHERE le.status = 'success') AS success,
        count(*) FILTER (WHERE le.status = 'failed' AND le.content_failure) AS content_failed,
        count(*) FILTER (WHERE le.status = 'failed' AND NOT le.content_failure) AS transient_failed,
        count(*) FILTER (
            WHERE le.status = 'claimed' AND le.claimed_at >= now() - %(claim_timeout)s * interval '1 second'
        ) AS claimed_active,
        count(*) FILTER (
            WHERE le.status = 'claimed' AND le.claimed_at < now() - %(claim_timeout)s * interval '1 second'
        ) AS claimed_stale,
        -- Subset of content_failed that claim_next_page() will still
        -- reclaim on its own (attempt_count hasn't hit the cap yet) --
        -- see MAX_ATTEMPTS_PER_PAGE. Needed to compute "remaining" below:
        -- a *capped* content failure needs a person or a different model,
        -- not another automatic retry, so it must not count as remaining
        -- work the pipeline will still get to on its own.
        count(*) FILTER (
            WHERE le.status = 'failed' AND le.content_failure AND le.attempt_count < %(max_attempts)s
        ) AS content_failed_retryable
    FROM llm_extractions le
    JOIN pages p ON p.id = le.page_id
    WHERE le.model_tag <> %(human_tag)s AND p.excluded_at IS NULL
    GROUP BY le.model_tag
    ORDER BY le.model_tag
"""

# Total catalogue_entries rows produced per model -- the actual extracted
# table data, as opposed to how many *pages* succeeded above. Excludes
# HUMAN_MODEL_TAG for the same reason as EXTRACTION_SUMMARY_SQL above --
# a human correction's entries aren't a model's output.
ENTRIES_PER_MODEL_SQL = """
    SELECT le.model_tag, count(ce.id) AS total_entries
    FROM catalogue_entries ce
    JOIN llm_extractions le ON le.id = ce.extraction_id
    JOIN pages p ON p.id = le.page_id
    WHERE le.model_tag <> %(human_tag)s AND p.excluded_at IS NULL
    GROUP BY le.model_tag
"""

# One row per model that has run the independent full-page OCR pass (see
# scripts/extract_with_llm.py's ocr_full_page() and
# supabase/migrations/20260912160000_add_page_ocr_text.sql). Fully separate
# from EXTRACTION_SUMMARY_SQL above -- a page can succeed at one and fail
# the other.
OCR_SUMMARY_SQL = """
    SELECT
        pot.model_tag,
        count(*) AS total_attempted,
        count(*) FILTER (WHERE pot.status = 'success') AS success,
        count(*) FILTER (WHERE pot.status = 'failed') AS failed
    FROM page_ocr_text pot
    JOIN pages p ON p.id = pot.page_id
    WHERE p.excluded_at IS NULL
    GROUP BY pot.model_tag
    ORDER BY pot.model_tag
"""

# One row per model, counting how many of its extractions have a human
# qc_reviews verdict recorded against their *current* output -- backs the
# research site's Progress page ("Human-reviewed" column and, via the
# approved count, the completion bar's green segment).
#
# qc_reviews.extraction_id points at the stable (page_id, model_tag) row in
# llm_extractions, but a 'needs_reprocessing' verdict resets that same row
# for the existing claim loop to retry (see apply_qc_verdict()), and a
# retry's own db_save_extraction_success()/db_save_extraction_failure()
# overwrite it in place rather than inserting a new id -- so without a
# version boundary, a review of the *old* (now-replaced) output would go on
# counting that extraction_id as reviewed/approved forever. The
# qr.created_at >= le.created_at join closes that: both save functions bump
# created_at on every write (see their own comments), so a review only
# counts once it postdates the extraction's current output. Excludes
# HUMAN_MODEL_TAG rows for the same reason as EXTRACTION_SUMMARY_SQL above
# -- a human correction's own row is never what a qc_reviews verdict is
# recorded against (see apply_qc_verdict()'s own WHERE model_tag <>
# %(human_tag)s).
REVIEWED_PER_MODEL_SQL = """
    SELECT
        le.model_tag,
        count(DISTINCT le.id) AS reviewed,
        count(DISTINCT le.id) FILTER (WHERE qr.verdict = 'approved') AS approved
    FROM llm_extractions le
    JOIN pages p ON p.id = le.page_id
    JOIN qc_reviews qr ON qr.extraction_id = le.id AND qr.created_at >= le.created_at
    WHERE le.model_tag <> %(human_tag)s AND p.excluded_at IS NULL
    GROUP BY le.model_tag
"""

# The actual top-line goal -- "is there data extracted from this page at
# all" -- as opposed to every query above, which is deliberately sliced per
# model_tag to track which model is doing the work and how well. This one
# collapses all of that: a page counts the moment *any* source (any vision
# model bake-off candidate, the textparse:* OCR-text backfill, or a human
# correction) has a successful llm_extractions row for it, full stop, with
# no per-model breakdown and no review/approval requirement -- a
# successfully extracted-but-not-yet-reviewed page still counts here, since
# the question this answers is narrower than the per-model "done_pct"
# (which requires review) on the Progress page. HUMAN_MODEL_TAG is
# deliberately included (not excluded like the per-model queries above) --
# a page a human corrected by hand, with no model ever succeeding on it,
# is still a page with data extracted from it.
PAGES_ANY_EXTRACTED_SQL = """
    SELECT count(DISTINCT le.page_id)
    FROM llm_extractions le
    JOIN pages p ON p.id = le.page_id
    WHERE le.status = 'success'
      AND le.model_tag <> ALL(%(gemma_ocr_only_tags)s)
      AND p.image_uploaded_at IS NOT NULL
      AND p.excluded_at IS NULL
"""


def fetch_dashboard_data(conn):
    """Run every query above and merge extraction/entries/OCR stats into one
    per-model_tag dict, so the template only has to iterate one structure."""
    with conn.cursor() as cur:
        cur.execute(TOTAL_PAGES_SQL)
        (total_pages,) = cur.fetchone()

        cur.execute(PAGES_ANY_EXTRACTED_SQL, {"gemma_ocr_only_tags": list(GEMMA_OCR_ONLY_TAGS)})
        (any_extracted_pages,) = cur.fetchone()

        cur.execute(
            EXTRACTION_SUMMARY_SQL,
            {"claim_timeout": CLAIM_TIMEOUT_SECONDS, "human_tag": HUMAN_MODEL_TAG, "max_attempts": MAX_ATTEMPTS_PER_PAGE},
        )
        extraction_cols = [d.name for d in cur.description]
        extraction_rows = [dict(zip(extraction_cols, row)) for row in cur.fetchall()]

        cur.execute(ENTRIES_PER_MODEL_SQL, {"human_tag": HUMAN_MODEL_TAG})
        entries_by_model = dict(cur.fetchall())

        cur.execute(OCR_SUMMARY_SQL)
        ocr_cols = [d.name for d in cur.description]
        ocr_rows = [dict(zip(ocr_cols, row)) for row in cur.fetchall()]

        cur.execute(REVIEWED_PER_MODEL_SQL, {"human_tag": HUMAN_MODEL_TAG})
        reviewed_by_model = {}
        approved_by_model = {}
        for tag, reviewed, approved in cur.fetchall():
            reviewed_by_model[tag] = reviewed
            approved_by_model[tag] = approved

    models = {}
    for row in extraction_rows:
        tag = row["model_tag"]
        models[tag] = {
            "model_tag": tag,
            "extraction_attempted": row["total_attempted"],
            "extraction_success": row["success"],
            "extraction_content_failed": row["content_failed"],
            # The subset of content_failed that has hit MAX_ATTEMPTS_PER_PAGE
            # and so will never be reclaimed automatically again -- the
            # "needs a person" bucket the Progress page's bar highlights
            # separately from success, as opposed to content failures still
            # under the cap (which are still "remaining", not stuck).
            "content_failed_capped": row["content_failed"] - row["content_failed_retryable"],
            "extraction_transient_failed": row["transient_failed"],
            "extraction_claimed_active": row["claimed_active"],
            "extraction_claimed_stale": row["claimed_stale"],
            "reviewed": reviewed_by_model.get(tag, 0),
            "approved": approved_by_model.get(tag, 0),
            # Mirrors scripts/extraction_status.py's own "remaining"
            # definition: never-attempted pages (assuming this model will
            # eventually run against every uploaded page) + stale claims +
            # transient failures + under-cap content failures -- every
            # page still eligible to be picked up and retried
            # automatically, either by a fresh claim or the pipeline's
            # next run, with no person needing to do anything first. A
            # *capped* content failure is deliberately excluded: it won't
            # move again on its own (see extraction_content_failed's own
            # legend entry), so it isn't "remaining" in that sense.
            "remaining": (
                (total_pages - row["total_attempted"])
                + row["claimed_stale"]
                + row["transient_failed"]
                + row["content_failed_retryable"]
            ),
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
                "content_failed_capped": 0,
                "extraction_transient_failed": 0,
                "extraction_claimed_active": 0,
                "extraction_claimed_stale": 0,
                "reviewed": reviewed_by_model.get(tag, 0),
                "approved": approved_by_model.get(tag, 0),
                # No extraction attempted at all yet for this model (it
                # only shows up here via the OCR pass) -- every uploaded
                # page is still remaining work for it.
                "remaining": total_pages,
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
        "any_extracted_pages": any_extracted_pages,
        # Fewest pages remaining first -- the Progress page's own framing
        # is "how much is left," so the model closest to done (or already
        # there, at 0) leads, not alphabetical order.
        "models": sorted(models.values(), key=lambda m: m["remaining"]),
    }


# ---------------------------------------------------------------------------
# Public overview page
# ---------------------------------------------------------------------------

# One row-set per page, not one per model attempt: a page can have a
# successful extraction from several different models (they're run
# independently to bake off against each other -- see
# scripts/extract_with_llm.py's module docstring) plus, once reviewed, a
# human correction. Counting every one of those would inflate "catalogue
# entries" by however many models happened to run, so this treats a human
# correction as authoritative where one exists, and PRIMARY_MODEL_TAG's own
# successful extraction otherwise -- `DISTINCT ON (page_id)` with
# HUMAN_MODEL_TAG sorted first picks exactly one extraction_id per page
# under that rule.
_CANONICAL_EXTRACTION_PER_PAGE_SQL = """
    SELECT DISTINCT ON (page_id) page_id, id AS extraction_id
    FROM llm_extractions
    WHERE status = 'success' AND model_tag IN (%(human_tag)s, %(primary_tag)s)
    ORDER BY page_id, (model_tag = %(human_tag)s) DESC
"""

# copies is stored as free text, not a number (see supabase/migrations'
# init_processing_schema.sql) -- the source column mixes plain digits with
# a thousands separator ("12.7% carry a thousands separator" -- see
# analysis/integrity/INTEGRITY_SWEEP.md), and a page can leave it blank or
# carry a non-numeric annotation the schema doesn't rule out. The `~
# '^[0-9]+(,[0-9]+)*$'` guard requires at least one digit (not just
# `[0-9,]+`, which also matches a comma-only value like "," -- stripping the
# comma from that leaves an empty string, and ''::bigint raises rather than
# being excluded, taking this whole request down), so stripping the comma
# and summing can't itself throw on the other cases -- anything else is
# silently excluded from the sum rather than raising, same as sum() already
# does for a NULL/blank copies value.
#
# source_pdf is set by extract_with_llm.py's process_page() on every entry
# it produces (the PDF filename stem, not part of the model's own JSON
# output -- see that function's own comment) -- one of the bound volumes
# downloaded from the British Library Research Repository (see the Sources
# page), so this counts how many of those volumes have contributed at
# least one canonical entry so far.
CORPUS_LIVE_STATS_SQL = f"""
    WITH canonical AS ({_CANONICAL_EXTRACTION_PER_PAGE_SQL})
    SELECT
        count(*) AS total_entries,
        sum(
            CASE WHEN ce.copies ~ '^[0-9]+(,[0-9]+)*$' THEN replace(ce.copies, ',', '')::bigint ELSE NULL END
        ) AS total_copies,
        count(DISTINCT nullif(ce.printer, '')) AS total_printers,
        count(DISTINCT nullif(ce.publisher, '')) AS total_publishers,
        count(DISTINCT nullif(ce.quarter, '')) AS total_quarters,
        count(DISTINCT nullif(ce.source_pdf, '')) AS total_source_pdfs
    FROM canonical c
    JOIN catalogue_entries ce ON ce.extraction_id = c.extraction_id
"""


def fetch_corpus_stats(conn):
    """One row of corpus-wide totals for the Overview page's stats card --
    see CORPUS_LIVE_STATS_SQL for what counts as "the" extraction for a
    page. Every total is 0/None over an empty table (no pipeline run yet),
    so callers get a well-formed all-zero dict rather than needing to
    special-case a NULL row."""
    with conn.cursor() as cur:
        cur.execute(CORPUS_LIVE_STATS_SQL, {"human_tag": HUMAN_MODEL_TAG, "primary_tag": PRIMARY_MODEL_TAG})
        cols = [d.name for d in cur.description]
        row = dict(zip(cols, cur.fetchone()))
    return {
        "total_entries": row["total_entries"] or 0,
        "total_copies": row["total_copies"] or 0,
        "total_printers": row["total_printers"] or 0,
        "total_publishers": row["total_publishers"] or 0,
        "total_quarters": row["total_quarters"] or 0,
        "total_source_pdfs": row["total_source_pdfs"] or 0,
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

    # LIMIT/OFFSET with a non-unique ORDER BY key isn't guaranteed stable
    # between the separate page-1 and page-2 requests that make up one
    # browsing session -- Postgres is free to return same-key rows in
    # whatever order a given query plan happens to produce, which can
    # duplicate or skip rows across pages. That's true whether there's an
    # explicit sort_col or not, so every other column is always appended
    # as a full-row tiebreak rather than assuming a primary key column
    # name (this schema doesn't use one consistently -- pcloud_files uses
    # pcloud_fileid, not id).
    if sort_col:
        order_parts = [
            psycopg2.sql.SQL("{} {}").format(
                psycopg2.sql.Identifier(sort_col),
                psycopg2.sql.SQL("DESC" if sort_dir == "desc" else "ASC"),
            )
        ]
        order_parts += [psycopg2.sql.Identifier(c) for c in columns if c != sort_col]
    else:
        order_parts = [psycopg2.sql.Identifier(c) for c in columns]
    order_sql = psycopg2.sql.SQL("ORDER BY {}").format(psycopg2.sql.SQL(", ").join(order_parts))

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

# The five queries below all embed the same "needs review" clause -- true
# when a page has at least one non-human extraction that's either a
# successful extraction with no qc_reviews verdict yet, or a capped
# content failure (the same "needs a person" bucket fetch_dashboard_data()
# reports as content_failed_capped) -- spliced in as `NOT %(needs_review)s
# OR EXISTS (...)`, the same short-circuit-on-a-bind-parameter idiom
# extract_with_gemini.py's CLAIM_NEXT_PAGE_SQL uses for
# allow_already_extracted, so needs_review=False always matches. Each
# query below is its own fully static string literal repeating that
# clause verbatim, rather than one shared fragment joined in with `+` --
# extract_with_gemini.py's own CLAIM_NEXT_PAGE_SQL comment explains why:
# an earlier version of that file's queries built this same way (first
# .format()-based, then plain-concatenation-based) tripped a code-
# scanning Bandit rule (B608) that pattern-matches *any* dynamic string
# construction flowing into SQL-keyword-shaped text, concatenation
# included, regardless of whether what's spliced in is actually request/
# user-controlled -- confirmed again here (PR #76 review comments) when
# NEEDS_REVIEW_EXISTS_SQL was first written as a `+`-joined fragment.
QC_FIRST_ID_SQL = """
    SELECT min(id) FROM pages p
    WHERE image_uploaded_at IS NOT NULL AND excluded_at IS NULL
      AND (NOT %(needs_review)s OR EXISTS (
        SELECT 1 FROM llm_extractions le
        WHERE le.page_id = p.id
          AND le.model_tag <> %(human_tag)s
          AND (
            (le.status = 'success' AND le.model_tag <> ALL(%(gemma_ocr_only_tags)s) AND NOT EXISTS (
                SELECT 1 FROM qc_reviews qr WHERE qr.extraction_id = le.id
            ))
            OR (le.status = 'failed' AND le.content_failure AND le.attempt_count >= %(max_attempts)s)
          )
      ))
"""

QC_NEXT_ID_SQL = """
    SELECT min(id) FROM pages p
    WHERE id > %(page_id)s AND image_uploaded_at IS NOT NULL AND excluded_at IS NULL
      AND (NOT %(needs_review)s OR EXISTS (
        SELECT 1 FROM llm_extractions le
        WHERE le.page_id = p.id
          AND le.model_tag <> %(human_tag)s
          AND (
            (le.status = 'success' AND le.model_tag <> ALL(%(gemma_ocr_only_tags)s) AND NOT EXISTS (
                SELECT 1 FROM qc_reviews qr WHERE qr.extraction_id = le.id
            ))
            OR (le.status = 'failed' AND le.content_failure AND le.attempt_count >= %(max_attempts)s)
          )
      ))
"""

QC_PREV_ID_SQL = """
    SELECT max(id) FROM pages p
    WHERE id < %(page_id)s AND image_uploaded_at IS NOT NULL AND excluded_at IS NULL
      AND (NOT %(needs_review)s OR EXISTS (
        SELECT 1 FROM llm_extractions le
        WHERE le.page_id = p.id
          AND le.model_tag <> %(human_tag)s
          AND (
            (le.status = 'success' AND le.model_tag <> ALL(%(gemma_ocr_only_tags)s) AND NOT EXISTS (
                SELECT 1 FROM qc_reviews qr WHERE qr.extraction_id = le.id
            ))
            OR (le.status = 'failed' AND le.content_failure AND le.attempt_count >= %(max_attempts)s)
          )
      ))
"""

# rank/total in one round trip (rank counts pages with id <= page_id,
# same definition fetch_qc_position()'s own docstring already gives) --
# both share the exact same WHERE clause, including the needs_review
# short-circuit, so they can't silently drift out of sync with each
# other the way two separate queries could.
QC_POSITION_SQL = """
    SELECT
        count(*) FILTER (WHERE id <= %(page_id)s) AS rank,
        count(*) AS total
    FROM pages p
    WHERE image_uploaded_at IS NOT NULL AND excluded_at IS NULL
      AND (NOT %(needs_review)s OR EXISTS (
        SELECT 1 FROM llm_extractions le
        WHERE le.page_id = p.id
          AND le.model_tag <> %(human_tag)s
          AND (
            (le.status = 'success' AND le.model_tag <> ALL(%(gemma_ocr_only_tags)s) AND NOT EXISTS (
                SELECT 1 FROM qc_reviews qr WHERE qr.extraction_id = le.id
            ))
            OR (le.status = 'failed' AND le.content_failure AND le.attempt_count >= %(max_attempts)s)
          )
      ))
"""

# Backs the "go to page N" jump: N is the same 1-indexed rank
# QC_POSITION_SQL computes and the template shows as "page N of M", so
# OFFSET N-1 under the identical id ordering and WHERE clause lands on
# exactly the page a reviewer typing N would expect.
QC_ID_AT_RANK_SQL = """
    SELECT id FROM pages p
    WHERE image_uploaded_at IS NOT NULL AND excluded_at IS NULL
      AND (NOT %(needs_review)s OR EXISTS (
        SELECT 1 FROM llm_extractions le
        WHERE le.page_id = p.id
          AND le.model_tag <> %(human_tag)s
          AND (
            (le.status = 'success' AND le.model_tag <> ALL(%(gemma_ocr_only_tags)s) AND NOT EXISTS (
                SELECT 1 FROM qc_reviews qr WHERE qr.extraction_id = le.id
            ))
            OR (le.status = 'failed' AND le.content_failure AND le.attempt_count >= %(max_attempts)s)
          )
      ))
    ORDER BY id
    OFFSET %(offset)s LIMIT 1
"""


def fetch_qc_page(conn, page_id, model_tag=None, needs_review=False):
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
                   p.excluded_at, p.excluded_note,
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

        # Backs the exclude control's own claimed-guard message: mirrors
        # apply_page_exclusion()'s rejection (any model 'claimed', any
        # staleness) so the template can explain up front why the button
        # isn't offered, instead of only finding out via a 409 after
        # submitting.
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM llm_extractions WHERE page_id = %(page_id)s AND status = 'claimed')",
            {"page_id": page_id},
        )
        (any_claimed,) = cur.fetchone()

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
            "SELECT model_tag, status, raw_text, error_message FROM page_ocr_text "
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

        # Excluded pages are skipped by prev/next (not just by the counts
        # above) -- there's nothing left for a reviewer to do on one until
        # it's re-included, so walking past it here keeps Prev/Next moving
        # through pages that still need attention. Landing on one directly
        # (via URL, or because it was excluded while it was the current
        # page) still works -- see the page lookup above, which doesn't
        # filter on excluded_at -- so re-including it is always reachable.
        # needs_review, when set, additionally skips past any page that's
        # already been reviewed (or has nothing yet needing a verdict) --
        # see QC_NEXT_ID_SQL/QC_PREV_ID_SQL's own "needs review" clause.
        nav_params = {
            "page_id": page_id,
            "needs_review": needs_review,
            "human_tag": HUMAN_MODEL_TAG,
            "max_attempts": MAX_ATTEMPTS_PER_PAGE,
            "gemma_ocr_only_tags": list(GEMMA_OCR_ONLY_TAGS),
        }
        cur.execute(QC_NEXT_ID_SQL, nav_params)
        (next_id,) = cur.fetchone()
        cur.execute(QC_PREV_ID_SQL, nav_params)
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
        "any_claimed": any_claimed,
        "prev_id": prev_id,
        "next_id": next_id,
    }


def fetch_qc_first_id(conn, needs_review=False):
    """id of the first image-available, non-excluded page (optionally
    restricted to needs_review, see QC_FIRST_ID_SQL) -- backs
    qc_index()'s redirect to a real page to land on. Returns None if
    nothing matches (an empty backlog, or an all-caught-up
    needs_review=True filter)."""
    with conn.cursor() as cur:
        cur.execute(
            QC_FIRST_ID_SQL,
            {
                "needs_review": needs_review,
                "human_tag": HUMAN_MODEL_TAG,
                "max_attempts": MAX_ATTEMPTS_PER_PAGE,
                "gemma_ocr_only_tags": list(GEMMA_OCR_ONLY_TAGS),
            },
        )
        (first_id,) = cur.fetchone()
    return first_id


def fetch_qc_position(conn, page_id, needs_review=False):
    """(rank, total) of page_id among every image-available, non-excluded
    page, ordered by id -- backs the QC page's "page N of M" counter. rank
    counts pages with id <= page_id, so it's meaningful even though prev/next
    (see fetch_qc_page()) walk the same id ordering rather than a queue of
    specifically-unreviewed pages. If page_id itself is excluded, it isn't
    counted in rank either (TOTAL_PAGES_SQL excludes it from total the same
    way) -- a harmless cosmetic wrinkle while sitting on an excluded page,
    not a data problem.

    needs_review, when set, restricts both rank and total to the same
    "needs a person" subset prev/next walk (see QC_POSITION_SQL's own
    "needs review" clause) -- computed in one round trip so they can't
    drift out of sync with each other."""
    with conn.cursor() as cur:
        cur.execute(
            QC_POSITION_SQL,
            {
                "page_id": page_id,
                "needs_review": needs_review,
                "human_tag": HUMAN_MODEL_TAG,
                "max_attempts": MAX_ATTEMPTS_PER_PAGE,
                "gemma_ocr_only_tags": list(GEMMA_OCR_ONLY_TAGS),
            },
        )
        (rank, total) = cur.fetchone()
    return rank, total


def fetch_qc_id_at_rank(conn, rank, needs_review=False):
    """The page id at 1-indexed rank among the same ordered set
    fetch_qc_position() counts -- backs the QC page's "go to page N" jump.
    Returns None if rank is out of range (including rank < 1, since
    OFFSET rejects a negative value)."""
    if rank < 1:
        return None
    with conn.cursor() as cur:
        cur.execute(
            QC_ID_AT_RANK_SQL,
            {
                "needs_review": needs_review,
                "human_tag": HUMAN_MODEL_TAG,
                "max_attempts": MAX_ATTEMPTS_PER_PAGE,
                "gemma_ocr_only_tags": list(GEMMA_OCR_ONLY_TAGS),
                "offset": rank - 1,
            },
        )
        row = cur.fetchone()
    return row[0] if row else None


def apply_qc_verdict(conn, extraction_id, verdict, note):
    """Atomically validate and apply a QC verdict against extraction_id.
    Returns ("ok" | "not_found" | "claimed" | "not_success", page_id,
    model_tag) -- page_id/model_tag are None unless the row was found.

    A separate "check, then write" (an earlier version of this function
    split across qc_verdict_target()/save_qc_verdict()) has a real race:
    between the check and the write, claim_next_page() could claim the
    row, and that worker's own later success/failure write -- itself
    unconditional -- would silently overwrite whatever this function had
    just set, discarding the reviewer's verdict with no trace. Locking the
    row with SELECT ... FOR UPDATE for the rest of this transaction closes
    that window: claim_next_page()'s own claiming step is an
    `INSERT ... ON CONFLICT (page_id, model_tag) DO UPDATE`, and taking
    the UPDATE side of that conflict on this same row requires the same
    row lock this function is already holding, so a concurrent claim
    attempt simply blocks until this transaction commits (or rolls back)
    rather than interleaving with it.

    Any status='claimed' row is rejected outright, regardless of how old
    claimed_at is -- an earlier version of this check only blocked a claim
    younger than CLAIM_TIMEOUT_SECONDS, but that staleness cutoff is a
    heuristic claim_next_page() uses to decide reclaimability, not proof
    the original worker actually died; a slow-but-still-running worker
    past the timeout is exactly what claim_next_page() would then also
    reclaim, and either one's unconditional result write could still
    overwrite this transaction's reset the moment it lands. Blocking any
    claimed row costs nothing real: a genuinely stale one doesn't need a
    QC nudge to get reclaimed, claim_next_page() already treats it as
    fair game on its own next run.

    Excludes HUMAN_MODEL_TAG rows entirely -- the QC form never renders a
    verdict control for a human correction, so an id resolving to one
    here only happens via a crafted request, and there's no model attempt
    behind it to judge. 'approved' additionally requires status='success'
    -- a failed or claimed row has no output worth signing off on.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT page_id, model_tag, status
            FROM llm_extractions
            WHERE id = %(extraction_id)s AND model_tag <> %(human_tag)s
            FOR UPDATE
            """,
            {"extraction_id": extraction_id, "human_tag": HUMAN_MODEL_TAG},
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return "not_found", None, None
        page_id, model_tag, status = row
        if status == "claimed":
            conn.rollback()
            return "claimed", page_id, model_tag
        if verdict == "approved" and status != "success":
            conn.rollback()
            return "not_success", page_id, model_tag

        cur.execute(
            "INSERT INTO qc_reviews (extraction_id, verdict, note) VALUES (%(extraction_id)s, %(verdict)s, %(note)s)",
            {"extraction_id": extraction_id, "verdict": verdict, "note": note or None},
        )
        if verdict == "needs_reprocessing":
            # Reset to look like a stale, non-content (i.e. unconditionally
            # reclaimable) failure: status='failed', content_failure=false,
            # claimed_at pushed further into the past than
            # CLAIM_TIMEOUT_SECONDS (the same constant claim_next_page()
            # itself uses for staleness, rather than a separately hardcoded
            # duration that could silently drift out of sync with it).
            # That's exactly the shape claim_next_page()'s
            # CLAIM_NEXT_PAGE_SQL already treats as immediately reclaimable
            # -- see scripts/extract_with_llm.py -- so the existing worker
            # loop picks the page back up and retries on its own next
            # scheduled run; no new pipeline logic needed.
            # raw_response/raw_text/attempt_count are left untouched so the
            # previous (wrong) output stays visible for comparison once
            # the retry completes.
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
    return "ok", page_id, model_tag


def apply_page_exclusion(conn, page_id, excluded, note):
    """Toggle whether page_id is excluded from future processing. Returns
    "ok", "not_found", or "claimed" (excluding only -- see below).

    excluded=True sets excluded_at to now() and stores note (if any); this
    is what CLAIM_NEXT_PAGE_SQL/PENDING_EXISTS_SQL (scripts/
    extract_with_llm.py) and TOTAL_PAGES_SQL/EXTRACTION_SUMMARY_SQL etc.
    (above) check to keep the page from ever being claimed again or counted
    in dashboard totals. excluded=False clears both columns -- re-included
    is re-included clean, with no stale note left over from whatever
    justified the exclusion last time.

    Deliberately narrow: only pages.excluded_at/excluded_note change. Any
    llm_extractions/catalogue_entries rows already on this page (from
    before it was excluded) are left exactly as they were -- this isn't a
    verdict on their content, just a switch for whether the pipeline should
    keep trying.

    Excluding locks the pages row FOR UPDATE first and rejects outright if
    any model currently has this page 'claimed' (any staleness) -- mirrors
    apply_qc_verdict()'s same guard, for the same reason: process_page()
    commits its claim up front and only writes the OCR/extraction result
    once the whole attempt finishes, unconditionally, with no re-check
    against excluded_at in between. Without this, excluding a page with a
    claim in flight wouldn't stop that attempt's result from landing right
    after -- reappearing in the pipeline's output the moment the reviewer
    was told they'd removed it. The row lock also closes the narrower race
    where a *new* claim attempt is concurrently evaluating this exact page:
    CLAIM_NEXT_PAGE_SQL takes the same `FOR UPDATE OF p SKIP LOCKED` lock,
    so it either skips this page while we hold the lock (and finds it
    excluded on its next attempt) or claims it first and this call then
    sees that claim and rejects. Re-including skips the guard entirely --
    nothing is racing to protect there, a concurrent claim succeeding
    alongside a re-include is just two independent, harmless writes.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pages WHERE id = %(page_id)s FOR UPDATE", {"page_id": page_id})
        if cur.fetchone() is None:
            conn.rollback()
            return "not_found"

        if excluded:
            cur.execute(
                "SELECT 1 FROM llm_extractions WHERE page_id = %(page_id)s AND status = 'claimed'",
                {"page_id": page_id},
            )
            if cur.fetchone() is not None:
                conn.rollback()
                return "claimed"

        cur.execute(
            """
            UPDATE pages
            SET excluded_at = CASE WHEN %(excluded)s THEN now() ELSE NULL END,
                excluded_note = CASE WHEN %(excluded)s THEN %(note)s ELSE NULL END
            WHERE id = %(page_id)s
            """,
            {"page_id": page_id, "excluded": excluded, "note": note or None},
        )
    conn.commit()
    return "ok"


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
