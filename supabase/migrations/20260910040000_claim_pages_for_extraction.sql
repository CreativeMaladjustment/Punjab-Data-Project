-- Supports claiming a page for extraction before the attempt completes, so
-- multiple parallel workers (see .github/workflows/extract-pages.yml's
-- `worker` matrix) can each atomically pick a different page instead of
-- colliding on the same one -- see scripts/extract_with_llm.py's
-- claim_next_page(). A worker that dies or is killed mid-extraction leaves
-- its claim behind; any worker (the same one on its next run, or another)
-- picks the page back up once the claim is older than CLAIM_TIMEOUT_SECONDS.
-- No separate cleanup step needed: the claiming query itself treats a stale
-- claim as available again, every time it runs.
alter table llm_extractions
    add column claimed_at timestamptz;

-- Rows from before this migration (in practice, only 'failed' ones -- the
-- 'claimed' status didn't exist yet) come out of the ADD COLUMN above with
-- claimed_at NULL. claim_next_page()'s staleness gate is `claimed_at < now()
-- - ...`, which is never true against NULL, so without this backfill every
-- pre-existing failure would become permanently unclaimable instead of
-- eligible for retry. created_at is the closest stand-in for "last
-- touched" available on rows that predate real claim tracking.
update llm_extractions
    set claimed_at = created_at
    where status = 'failed' and claimed_at is null;

alter table llm_extractions
    drop constraint llm_extractions_status_check;

-- A 'claimed' row without claimed_at would make the staleness check in
-- claim_next_page() ambiguous. Scoped to 'claimed' only (not 'failed') --
-- db_save_extraction_failure()'s INSERT ... ON CONFLICT DO UPDATE never
-- lists claimed_at, and Postgres validates CHECK constraints against that
-- pre-conflict insert-attempt tuple (claimed_at NULL) even when the
-- conflict path will actually preserve the existing row's value, so a
-- 'failed' entry here would make every recorded failure violate this
-- constraint.
alter table llm_extractions
    add constraint llm_extractions_status_check
    check (status in ('claimed', 'success', 'failed')),
    add constraint llm_extractions_claimed_at_check
    check (status <> 'claimed' or claimed_at is not null);

-- Every claim attempt filters on (model_tag, status); this is the query the
-- claiming transaction runs most, and most often.
create index llm_extractions_model_tag_status_idx
    on llm_extractions (model_tag, status);
