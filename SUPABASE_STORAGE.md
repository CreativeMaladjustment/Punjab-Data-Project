# Keeping the database under Supabase's free-tier limit

The processing database (Supabase Postgres) hit the free tier's database size limit in
October 2026, after which Supabase stops writes. This document records what we found, what
we changed to use less space, the workflows that do it, and how to resume processing
without hitting the wall again. Figures are from the public schema
(`select pg_total_relation_size` over the 13 tables), as of 28 Oct 2026.

## 1. What happened

- The public schema had grown to **670 MB**; the free tier allows 500 MB.
- Processing was stopped first: every scheduled workflow that writes to the database had
  its `schedule:` block commented out (`extract-pages`, `extract-pages-gemini`,
  `parse-ocr-text`, `detect-languages`, `translate-canonical-titles`). Re-enable by
  uncommenting them — but see §6 first.

## 2. Where the space was

| Table | Size | Notes |
|---|---|---|
| `llm_extractions` | 230 MB | most of it TOAST: `raw_text` (the model's raw reply, ~140 MB live) and `raw_response` (parsed entries as JSON, ~43 MB) |
| `page_ocr_text` | 173 MB | full-page OCR text; **input** to the text-parsing step |
| `catalogue_entries` | 75 MB | 166k parsed entries, all models |
| `page_extractions_canonical` | 73 MB | canonical layer (mostly dead rows + a redundant `raw_response` copy) |
| `catalogue_entries_canonical` | 70 MB | canonical layer (mostly dead rows) |
| `page_translations`, `field_value_map`, `title_translations`, `pages`, `page_detected_languages`, the rest | ~50 MB | |

Two findings drove everything below:

1. **A lot of it was dead space, not data.** Updating a row in Postgres writes a new copy and
   leaves the old one until the table is rewritten; autovacuum frees that space for reuse
   but never shrinks the file, and Supabase counts file size. The canonical tables had been
   updated row-by-row by several backfills (date parsing, value mapping), so ~60% of
   them was dead space.
2. **Several columns were redundant or never read.** `raw_response` (a JSON copy of the
   entries already stored in `catalogue_entries`) existed twice — in `llm_extractions` and
   copied into `page_extractions_canonical` — and was read only by one status report.

## 3. What we did

| # | Change | Space | Status |
|---|---|---|---|
| 1 | Disabled all writing cron schedules | stops growth | done |
| 2 | **Backup** — `pg_dump` of the public schema as a workflow artifact | — | done 28 Oct (`punjab-db-20261028T142428Z.dump`, 152 MB, kept to 2027-01-26) |
| 3 | **`VACUUM FULL`** every table (workflow) | **670 → 472 MB** (−198 MB): `page_ocr_text` −109, `page_extractions_canonical` −46, `catalogue_entries_canonical` −41 | done |
| 4 | Dropped `page_extractions_canonical.raw_response` (migration `20261028120000`) and stopped syncing it | ~20 MB after a vacuum | applies when merged to `main` |
| 5 | Dropped `llm_extractions.raw_response` (migration `20261028130000`); extraction scripts no longer write it; `extraction_status.py` counts `catalogue_entries` rows instead | ~40 MB after a vacuum | applies when merged to `main` |
| 6 | **Trim `raw_text`** workflow — clears `llm_extractions.raw_text` (non-winners, or all) then vacuums | est. ~70 MB (`non_winners`) to ~145 MB (`all`) | built, **not yet run** |

Estimated end state after 4–6 and a final vacuum: roughly **265–410 MB** depending on the
`raw_text` choice (estimates from column sizes, not measurements — the workflow's dry run
prints the real numbers).

### Deliberately not done

- **Deleting non-winning extractions** (and their entries): a script for it was written and
  removed. It would free the most but throws away per-model results and the markers
  workers use to know what has been attempted.
- **Emptying `page_ocr_text`**: it is the input to `parse_ocr_text.py`, the only way to
  regenerate `textparse:*` results, and feeds the QC page's "Full-page OCR" panel.
- **Emptying `page_translations` / `page_detected_languages`**: we want to keep translations.

## 4. Workflows and scripts

| Workflow (manual) | What it does |
|---|---|
| **Back up database (pg_dump)** | full compressed dump → artifact (90 days). Run before anything destructive. |
| **Database inventory (read-only)** | table sizes, extractions by model, sizes of selected columns (`scripts/db_inventory.sql`). Safe at any size. |
| **Vacuum full (reclaim dead space)** | rewrites every table smallest-first (`scripts/cleanup_db/phase3_reclaim_space.sql`). Dry run by default; type `VACUUM` to run. |
| **Trim llm_extractions.raw_text (free space)** | clears `raw_text` then vacuums `llm_extractions` (`scripts/cleanup_db/trim_raw_text.sql`). Dry run by default; needs backup ticked and `TRIM` typed. |
| **Backfill canonical dates** | recomputes the derived date columns (not a space tool; see `CANONICAL.md`). |

All of them connect through the **session** pooler (port 5432); a transaction-pooler URL
(6543) is switched automatically. Run order after merging: *backup → inventory → trim
`raw_text` (optional) → vacuum full*. Drops from migrations only free space after the
vacuum.

### Restoring from the backup

```bash
createdb restore_test
pg_restore --no-owner --no-privileges -d restore_test punjab-db-20261028T142428Z.dump
# one table only, e.g. the OCR text:
pg_restore --data-only --table=page_ocr_text -d <db> punjab-db-20261028T142428Z.dump
```

## 5. Lessons — how we got here, and how to avoid it

- **Avoid whole-table `UPDATE` backfills.** Each one doubles the table until it is rewritten.
  If a backfill is needed, plan a `VACUUM FULL` right after (the vacuum workflow).
- **Don't copy large blobs into derived tables.** The canonical layer's page-level
  `raw_response` copy cost ~20 MB of live data plus all its dead rows for nothing.
- **Keep heavy backfills out of migrations.** Supabase's migration runner has a statement
  timeout (~2 min); the date backfill is a separate workflow for that reason, and the
  neighbour lookups it needs have a matching index.
- **Autovacuum is not enough.** It recycles space inside a file; only `VACUUM FULL` (or a
  table rewrite) returns it to the OS. A rewrite needs transient free space about the size
  of the table's live data, so don't wait until the disk is completely full.
- **Measure live data before assuming bloat.** `llm_extractions` looked bloated but was live
  `raw_text` — checking column sizes (`pg_column_size`) is what found it.
- **Supabase read-only mode:** when over the limit the project goes read-only. The cleanup
  scripts run `SET default_transaction_read_only = off` for their session; we believe this
  is how you get past it, but it has not been verified — see Supabase's docs on disk size.

## 6. Before turning processing back on

- Run the inventory and check the headroom. At ~470 MB there is only ~30 MB of room;
  at ~300 MB there is room for tens of thousands of new entries.
- New extraction output now stores `raw_text` per attempt, which is the biggest column.
  Decide whether to keep that (QC page "raw output" panel, `audit_ditto_marks.py`) or
  stop writing it; if kept, re-run the **Trim raw_text** workflow periodically.
- Re-enable schedules one at a time (uncomment the `schedule:` block), and watch the
  size after the first few runs.
- Schedule the **Database inventory** workflow (or run it weekly by hand) so growth is seen
  before the limit.

See also: `CANONICAL.md` (the canonical layer these tables feed), `ARCHITECTURE.md` /
`DECISIONS.md` (D-021 on the free-tier infrastructure choice).
