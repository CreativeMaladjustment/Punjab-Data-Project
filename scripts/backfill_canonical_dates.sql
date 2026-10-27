-- One-time backfill of the derived date columns for existing canonical entries.
-- Run in the Supabase SQL editor after the 20261027110000 migration has applied.
-- (New/updated pages are dated automatically by sync_canonical_entries().)
-- Takes a few seconds to a couple of minutes; disable the editor's timeout first.

set statement_timeout = 0;

select apply_catalogue_dates();
analyze catalogue_entries_canonical;   -- lets the planner use the PDF-order index
select infer_catalogue_dates();

-- sanity check
select date_status, date_basis, count(*)
from catalogue_entries_canonical
group by 1, 2
order by 3 desc;
