-- Free the space held by llm_extractions.raw_text: the model's raw response string,
-- stored per attempt (added in 20260911120000). It is not copied into the canonical
-- tables (they keep the parsed entries) and is only used for display on
-- the QC review page and by scripts/audit_ditto_marks.py.
--
-- Sets raw_text to NULL (rows, statuses and entries are untouched),
-- then VACUUM FULL rewrites llm_extractions so the space is returned to the OS.
-- psql variables:
--   dry_run  true|false
--   mode     non_winners  (keep raw_text of canonical winners, human:review and
--                          extractions with a QC verdict)   |   all
--
-- llm_extractions has an AFTER UPDATE trigger that re-runs the canonical refresh for
-- every updated row; it is disabled for the UPDATE (and the canonical row counts are
-- checked before commit).

SET default_transaction_read_only = off;
SET statement_timeout = 0;

create temp table keep_extractions on commit preserve rows as
    select extraction_id as id from page_extractions_canonical where extraction_id is not null
    union
    select id from llm_extractions where model_tag = 'human:review'
    union
    select extraction_id from qc_reviews;
create unique index on keep_extractions (id);

\echo '== llm_extractions.raw_text by group =='
select case when k.id is not null then 'kept (winner / human / QC verdict)' else 'not kept' end as grp,
       le.status,
       count(*) as rows,
       count(le.raw_text) as with_raw_text,
       pg_size_pretty(coalesce(sum(pg_column_size(le.raw_text)), 0)) as raw_text_size
from llm_extractions le
left join keep_extractions k on k.id = le.id
group by 1, 2
order by coalesce(sum(pg_column_size(le.raw_text)), 0) desc;

select pg_size_pretty(pg_total_relation_size('llm_extractions')) as llm_extractions_size_now;

\echo 'mode =' :mode

\if :dry_run
  \echo 'DRY RUN - nothing changed'
\else
  begin;
  set local lock_timeout = '60s';

  create temp table canonical_before on commit drop as
    select (select count(*) from page_extractions_canonical) as pec,
           (select count(*) from catalogue_entries_canonical) as cec,
           (select count(*) from llm_extractions) as ext,
           (select count(*) from catalogue_entries) as ent;

  alter table llm_extractions disable trigger user;

  \if :trim_all
    update llm_extractions set raw_text = null where raw_text is not null;
  \else
    update llm_extractions le set raw_text = null
    where le.raw_text is not null
      and not exists (select 1 from keep_extractions k where k.id = le.id);
  \endif

  alter table llm_extractions enable trigger user;

  do $$
  declare b record; a record;
  begin
    select * into b from canonical_before;
    select (select count(*) from page_extractions_canonical) as pec,
           (select count(*) from catalogue_entries_canonical) as cec,
           (select count(*) from llm_extractions) as ext,
           (select count(*) from catalogue_entries) as ent
      into a;
    if a.pec <> b.pec or a.cec <> b.cec or a.ext <> b.ext or a.ent <> b.ent then
      raise exception 'row counts changed (% -> %); aborting', b, a;
    end if;
  end $$;

  commit;
  \echo 'raw_text cleared. Rewriting llm_extractions to return the space ...'
  VACUUM (FULL, ANALYZE) llm_extractions;
  select pg_size_pretty(pg_total_relation_size('llm_extractions')) as llm_extractions_size_after;
\endif
