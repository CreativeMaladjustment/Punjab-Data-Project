-- Phase 1b (OPTIONAL, emergency headroom): empty page_ocr_text. Only needed if the
-- database is too full for the phase 3 rewrites. Costs the "Full-page OCR" text
-- shown on the QC review page; restore it from the backup afterwards with
--   pg_restore --data-only --table=page_ocr_text -d <db> <dump>
-- psql variables: dry_run (true|false)

SET default_transaction_read_only = off;
SET statement_timeout = 0;

\echo '== Phase 1b: page_ocr_text =='
select count(*) as rows, pg_size_pretty(pg_total_relation_size('page_ocr_text')) as size from page_ocr_text;

\if :dry_run
  \echo 'DRY RUN - nothing changed'
\else
  begin;
  set local lock_timeout = '30s';
  truncate page_ocr_text;
  commit;
  \echo 'Truncated page_ocr_text'
\endif
