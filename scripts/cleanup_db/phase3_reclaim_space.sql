-- Phase 3: give dead space back to the OS. Deleted/updated rows leave dead tuples
-- (and dead TOAST) that count toward the database size until the table is
-- rewritten. VACUUM FULL rewrites a table into a new, compact file: it takes an
-- exclusive lock and needs transient free space about the size of the table's LIVE
-- data, so tables are rewritten smallest-first -- each one frees room for the next.
-- Stop the web app's writers first (they are blocked anyway while the DB is read-only).
-- psql variables: dry_run (true|false)

SET default_transaction_read_only = off;
SET statement_timeout = 0;
SET lock_timeout = '60s';

\echo '== Phase 3: tables (physical size, live vs dead rows) =='
select relname as tbl,
       pg_size_pretty(pg_total_relation_size(relid)) as total,
       n_live_tup as live_rows, n_dead_tup as dead_rows
from pg_stat_user_tables
where schemaname = 'public'
order by pg_total_relation_size(relid) desc;

\if :dry_run
  \echo 'DRY RUN - nothing rewritten'
\else
  \echo 'Rewriting (smallest first) ...'
  VACUUM (FULL, ANALYZE) qc_reviews;
  VACUUM (FULL, ANALYZE) pages;
  VACUUM (FULL, ANALYZE) title_translations;
  VACUUM (FULL, ANALYZE) field_value_map;
  VACUUM (FULL, ANALYZE) page_extractions_canonical;
  VACUUM (FULL, ANALYZE) catalogue_entries_canonical;
  VACUUM (FULL, ANALYZE) catalogue_entries;
  VACUUM (FULL, ANALYZE) llm_extractions;
  VACUUM (FULL, ANALYZE) page_ocr_text;
  \echo 'Done. Sizes now:'
  select relname as tbl, pg_size_pretty(pg_total_relation_size(relid)) as total
  from pg_stat_user_tables where schemaname = 'public'
  order by pg_total_relation_size(relid) desc;
\endif
