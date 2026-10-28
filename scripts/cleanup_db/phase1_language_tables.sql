-- Phase 1: empty the language-detection / translation tables (not needed by the
-- canonical layer; restorable from the backup). TRUNCATE returns the space to the
-- OS immediately, which also creates headroom for the later rewrites.
-- psql variables: dry_run (true|false)

SET default_transaction_read_only = off;
SET statement_timeout = 0;

\echo '== Phase 1: language / translation tables =='
select 'page_translations' as tbl, count(*) as rows, pg_size_pretty(pg_total_relation_size('page_translations')) as size from page_translations
union all
select 'page_detected_languages', count(*), pg_size_pretty(pg_total_relation_size('page_detected_languages')) from page_detected_languages;

\if :dry_run
  \echo 'DRY RUN - nothing changed'
\else
  begin;
  set local lock_timeout = '30s';
  truncate page_translations, page_detected_languages;
  commit;
  \echo 'Truncated page_translations and page_detected_languages'
\endif
