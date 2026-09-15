-- Lets a QC reviewer pull a page out of future processing entirely --
-- distinct from a qc_reviews 'needs_reprocessing' verdict, which targets one
-- model's extraction and asks the pipeline to retry it. Excluding a page
-- means no model should ever be claimed against it again (see
-- CLAIM_NEXT_PAGE_SQL/PENDING_EXISTS_SQL in scripts/extract_with_llm.py) and
-- it should stop counting toward the dashboard's totals/remaining-work
-- figures (see TOTAL_PAGES_SQL in api/queries.py) -- for a page that turns
-- out to be a duplicate, a blank, or otherwise not worth extracting.
--
-- Deliberately just two columns on `pages`, not an append-only log table
-- like qc_reviews: this is one page-level on/off switch (with room for one
-- explanatory note), not a history of per-model verdicts. Neither the B2
-- image nor the pages row itself is touched -- toggling excluded_at back to
-- NULL (see api/queries.py's apply_page_exclusion()) makes the page
-- eligible for processing again with nothing to restore.
alter table pages
    add column excluded_at timestamptz,
    add column excluded_note text;
