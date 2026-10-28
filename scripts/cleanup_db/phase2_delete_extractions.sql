-- Phase 2: delete extraction rows the canonical layer does not need.
--
-- KEPT:  every extraction that is a page's canonical winner, every human:review
--        extraction, and every extraction that has a qc_reviews verdict.
-- GONE:  all other llm_extractions rows (non-winning successes, plus failed and
--        claimed attempts) and, via ON DELETE CASCADE, their catalogue_entries.
--
-- The canonical tables are self-contained copies, but deleting catalogue_entries
-- fires the triggers that re-sync the canonical copy -- which would wipe it. So
-- user triggers are disabled for the delete and the canonical tables' row counts
-- are checked before commit (the transaction aborts if they changed).
-- psql variables: dry_run (true|false)

SET default_transaction_read_only = off;
SET statement_timeout = 0;

create temp table keep_extractions on commit preserve rows as
    select extraction_id as id from page_extractions_canonical where extraction_id is not null
    union
    select id from llm_extractions where model_tag = 'human:review'
    union
    select extraction_id from qc_reviews;
create unique index on keep_extractions (id);

\echo '== Phase 2: what would be kept / deleted =='
select
  (select count(*) from llm_extractions) as extractions_total,
  (select count(*) from keep_extractions) as extractions_kept,
  (select count(*) from llm_extractions le where not exists (select 1 from keep_extractions k where k.id = le.id)) as extractions_deleted,
  (select count(*) from catalogue_entries) as entries_total,
  (select count(*) from catalogue_entries ce where not exists (select 1 from keep_extractions k where k.id = ce.extraction_id)) as entries_deleted;

select le.model_tag, le.status, count(*) as deleted
from llm_extractions le
where not exists (select 1 from keep_extractions k where k.id = le.id)
group by 1, 2 order by 3 desc limit 15;

\if :dry_run
  \echo 'DRY RUN - nothing deleted'
\else
  begin;
  set local lock_timeout = '60s';

  create temp table canonical_before on commit drop as
    select (select count(*) from page_extractions_canonical) as pec,
           (select count(*) from catalogue_entries_canonical) as cec,
           (select count(*) from canonical_entry_notes) as notes,
           (select count(*) from llm_extractions where model_tag = 'human:review') as humans;

  alter table catalogue_entries disable trigger user;
  alter table llm_extractions disable trigger user;

  delete from llm_extractions le
  where not exists (select 1 from keep_extractions k where k.id = le.id);

  alter table catalogue_entries enable trigger user;
  alter table llm_extractions enable trigger user;

  do $$
  declare b record; a record;
  begin
    select * into b from canonical_before;
    select (select count(*) from page_extractions_canonical) as pec,
           (select count(*) from catalogue_entries_canonical) as cec,
           (select count(*) from canonical_entry_notes) as notes,
           (select count(*) from llm_extractions where model_tag = 'human:review') as humans
      into a;
    if a.pec <> b.pec or a.cec <> b.cec or a.notes <> b.notes or a.humans <> b.humans then
      raise exception 'canonical/human row counts changed (% -> %); aborting', b, a;
    end if;
  end $$;

  commit;
  \echo 'Deleted non-canonical extractions and their entries. Run phase 3 to return the space to the OS.'
\endif
