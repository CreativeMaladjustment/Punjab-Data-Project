-- Read-only inventory: where the database space is going, and how much of it is
-- NOT needed to keep the canonical layer. Safe to run on a full / read-only database.

\echo '== Largest tables (total = table + indexes + TOAST) =='
select c.relname as table,
       pg_size_pretty(pg_total_relation_size(c.oid)) as total,
       pg_size_pretty(pg_relation_size(c.oid)) as heap,
       pg_size_pretty(pg_indexes_size(c.oid)) as indexes,
       pg_size_pretty(coalesce(pg_total_relation_size(nullif(c.reltoastrelid, 0)), 0)) as toast,
       c.reltuples::bigint as approx_rows
from pg_class c
where c.relkind = 'r' and c.relnamespace = 'public'::regnamespace
order by pg_total_relation_size(c.oid) desc;

\echo '== Whole public schema =='
select pg_size_pretty(sum(pg_total_relation_size(c.oid))) as public_total
from pg_class c where c.relkind = 'r' and c.relnamespace = 'public'::regnamespace;

\echo '== Extractions by model: rows, entries, and raw_response bytes =='
select le.model_tag, le.status, count(*) as extractions,
       pg_size_pretty(coalesce(sum(pg_column_size(le.raw_response)), 0)) as raw_response_size,
       count(*) filter (where le.id in (select extraction_id from page_extractions_canonical where extraction_id is not null)) as canonical_winners
from llm_extractions le
group by 1, 2
order by coalesce(sum(pg_column_size(le.raw_response)), 0) desc;

\echo '== human:review rows (human corrections - must be kept) =='
select count(*) as human_extractions,
       (select count(*) from catalogue_entries ce join llm_extractions l on l.id = ce.extraction_id where l.model_tag = 'human:review') as human_entries
from llm_extractions where model_tag = 'human:review';

\echo '== Space held by data the canonical layer does not need =='
select 'llm_extractions.raw_response (non-winners)' as what,
       pg_size_pretty(coalesce(sum(pg_column_size(raw_response)), 0)) as size
from llm_extractions
where id not in (select extraction_id from page_extractions_canonical where extraction_id is not null)
  and model_tag <> 'human:review'
union all
select 'catalogue_entries (non-winners, non-human)',
       pg_size_pretty(coalesce(sum(pg_column_size(ce.*)), 0))
from catalogue_entries ce
join llm_extractions l on l.id = ce.extraction_id
where l.id not in (select extraction_id from page_extractions_canonical where extraction_id is not null)
  and l.model_tag <> 'human:review'
union all
select 'page_ocr_text.raw_text', pg_size_pretty(coalesce(sum(pg_column_size(raw_text)), 0)) from page_ocr_text
union all
select 'page_translations', pg_size_pretty(coalesce(sum(pg_column_size(t.*)), 0)) from page_translations t
union all
select 'page_extractions_canonical.raw_response (the canonical copy)', pg_size_pretty(coalesce(sum(pg_column_size(raw_response)), 0)) from page_extractions_canonical
union all
select 'catalogue_entries_canonical (the canonical copy)', pg_size_pretty(coalesce(sum(pg_column_size(c.*)), 0)) from catalogue_entries_canonical c;
