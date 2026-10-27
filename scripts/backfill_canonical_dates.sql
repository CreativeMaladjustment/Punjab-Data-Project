-- One-time backfill of the derived date columns for existing canonical entries.
-- Run after the 20261027110000 migration has applied: via the "Backfill canonical
-- dates" GitHub workflow, or paste into the Supabase SQL editor.
-- (New/updated pages are dated automatically by sync_canonical_entries().)
-- Safe to re-run: it recomputes the date columns from the as-extracted `date`.
--
-- One explicit transaction with SET LOCAL, so the timeout override also holds
-- through Supabase's transaction-mode pooler (a plain SET would not).

begin;

set local statement_timeout = 0;

select apply_catalogue_dates();
analyze catalogue_entries_canonical;   -- lets the planner use the PDF-order index
select infer_catalogue_dates();

commit;

-- sanity check
select date_status, date_basis, count(*)
from catalogue_entries_canonical
group by 1, 2
order by 3 desc;
