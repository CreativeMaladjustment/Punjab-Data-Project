-- Backs the dashboard's new QC review page (see api/queries.py's
-- apply_qc_verdict()): one row per human verdict on a specific model's
-- extraction of a page -- "does this look right, or does it need
-- reprocessing". Kept as its own append-only log rather than a column on
-- llm_extractions so re-reviewing the same extraction (e.g. after it's
-- been reprocessed) keeps every prior verdict instead of overwriting it.
--
-- A 'needs_reprocessing' verdict also resets the reviewed llm_extractions
-- row itself (status='failed', content_failure=false, claimed_at pushed
-- into the past) so the existing extract_with_llm.py claim loop picks it
-- back up and retries automatically on its next scheduled run -- see
-- CLAIM_NEXT_PAGE_SQL in that script. This table only records that the
-- reset happened and why; it has no effect on claim_next_page() itself.
create table qc_reviews (
    id bigint generated always as identity primary key,
    extraction_id bigint not null references llm_extractions (id) on delete cascade,
    verdict text not null check (verdict in ('approved', 'needs_reprocessing')),
    note text,
    created_at timestamptz not null default now()
);

create index qc_reviews_extraction_id_idx on qc_reviews (extraction_id);
