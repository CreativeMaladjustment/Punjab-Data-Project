# The canonical layer — one winning record per page, normalized for review

This document describes the *canonical* tables and view, how a page's winning
extraction is chosen, and how the normalization layers on top of it (value
mapping, title translation, **date parsing**) work. It is written for discussion:
the last section lists the open questions and known limits.

It sits on top of the per-model extraction tables (`llm_extractions`,
`catalogue_entries`), which are never modified by anything here. Following
[D-001](DECISIONS.md), the as-extracted text stays queryable; every normalized
value is a separate, inspectable column or lookup row. See D-022 and D-023.

## 1. Why a canonical layer

Several models (and `textparse:*` runs over full-page OCR) can extract the same
page, so `catalogue_entries` holds many competing versions of the same entries.
Analysis and review need **one** version per page. The canonical layer picks it
and keeps its own **copy** of the winning text, so the record shown to reviewers
doesn't depend on rows that may be re-run or deleted.

## 2. Tables and view

| Object | What it holds |
|---|---|
| `page_extractions_canonical` | One row per page: the winning `model_tag`, `priority`, `entry_count`, a copy of the winning `raw_response`, and `extraction_id` (a *provenance hint only* — `ON DELETE SET NULL`, so deleting the source never deletes the canonical copy). |
| `catalogue_entries_canonical` | A full copy of every entry of the winning extraction (same content columns as `catalogue_entries`) plus `page_id`, `source_extraction_id`, `model_tag`, `priority`, and the derived `date_*` columns (§5). Unique on `(page_id, entry_index)`. |
| `field_value_map` | Generic value-normalization lookup (§4.1): `field`, `raw_value` → `canonical_value`, one table for every field. |
| `title_translations` | Original title → English translation, or `is_english = true` (§4.2). |
| `canonical_entry_notes` | Human reviewer notes per `(page_id, entry_index)`, with a snapshot of the record they were written about (§6). |
| `canonical_entries_view` | The flat, human-readable row per entry used by the Canonical review table (§7). Normalized spellings and the English title are joined in; the as-extracted values stay in `catalogue_entries_canonical`. |

## 3. Which extraction wins

`select_canonical_extraction()` ranks successful extractions per page:

1. `human:review` (a human correction)
2. `textparse:*` (text parsing after full-page OCR)
3. the first successful attempt of any other model

Within a priority the earliest successful attempt wins. Priority numbers are
stored 1/2/3 in that order.

**Keeping it current.** Triggers re-run the selection when an extraction is
inserted/updated, and re-copy the entries (statement-level triggers on
`catalogue_entries`) when the winner's entries change. `sync_canonical_entries(page)`
does, in order: copy page-level data → delete + re-insert the page's canonical
entries → `map_new_field_values` → `apply_catalogue_dates` → `infer_catalogue_dates`.
`refresh_canonical_extractions()` re-ranks (all pages, or one) and then syncs.

**Consequences worth knowing**
- Canonical rows are rebuilt on every change — **never edit them directly.**
  Corrections go through a `human:review` extraction (which wins), or through the
  lookup tables (value map, title translation), which are re-applied by join.
- The ranking is by *kind of source*, not quality: a `textparse:*` run that found
  1 entry beats a model that found 15 (this happened on page 2224). See §8.
- There is no delete trigger: if a winning extraction is deleted, the canonical
  row keeps its last copy until a manual `refresh_canonical_extractions()`.

## 4. Normalization layers

### 4.1 Value mapping (`field_value_map`)

Variant spellings of the same value ("Urdu", "Urdú", "URDU", "Urdū.") are
grouped under one canonical value. Covered fields: `lang`, `pubcity`, `pcity`,
`publisher`, `printer` (add a field by adding it to `map_new_field_values()`).

- `norm_key` = the value with accents, punctuation and case removed; values with
  the same `(field, norm_key)` share a canonical value. The most frequent
  spelling wins when a group is first created; later spellings join it.
- Rows are never overwritten by the auto-mapper. A human edit sets
  `status = 'approved'` and sticks.
- The raw value is never replaced; the review table shows the canonical value.
- Edited on the **Bulk errors** page (rename/merge a whole group, or move one
  spelling).

### 4.2 Title translation (`title_translations`)

`scripts/translate_canonical_titles.py` (workflow *Translate canonical titles to
English*, hourly at :30) sends distinct untranslated titles to Gemini in batches
and records, per title, whether it is 100% English and — if not — its language
and English translation. English titles are recorded too, so they aren't
re-checked. The review table shows `title_en` next to `title`; the detail page
shows "original (English)".

## 5. Dates

The as-extracted `date` text is mixed: ISO dates, English-style text dates in
many forms, numeric d-m-y, Hijri/B.E./Fasli, ranges, partial dates with no year,
"ditto", and misplaced non-dates ("1st edition", "REMARKS."). About 59% of
canonical entries have no date at all (profile of Oct 2026, ~58k entries).

`parse_catalogue_date(text, year_from, year_to)` is a pure SQL function; the
derived columns are stored on `catalogue_entries_canonical` and surfaced in the
view.

### 5.1 Derived columns

| Column | Meaning |
|---|---|
| `date_start`, `date_end` | First and last day covered. A day → both the same; a month → its first/last day; a year → Jan 1/Dec 31; a **range → first day of the range .. last day**. |
| `date_iso` | `date_start` written at its precision: `YYYY-MM-DD`, `YYYY-MM` or `YYYY`. |
| `date_precision` | `day`, `month`, `year` or `range`. |
| `date_era` | Hijri / B.E. / Fasli etc. kept as printed. Era years are *not* converted. |
| `date_status` | see below |
| `date_basis` | where the date/estimate came from: `printed`, `neighbors`, `ditto`, `year_window`, `quarter` |
| `date_est_from`, `date_est_to` | **Estimated** window for entries with no usable date. Not a date. |
| `date_note` | short reason ("year taken from neighbors", "century from pdf", "unparsed" …) |

`date_status`:

| Status | Meaning |
|---|---|
| `ok` | parsed from the printed text |
| `inferred` | day/month printed, **year (or the whole date, for ditto) filled in from context** |
| `needs_year` | day/month printed but the year couldn't be resolved |
| `ditto` | "ditto"/"Do." that couldn't be resolved |
| `needs_review` | unparsed, OCR damage ("98th January 1880"), year outside range, era date only |
| `not_a_date` | edition / size / header text that landed in the date field |
| `empty` | nothing printed |

### 5.2 Parsing rules

- Recognised forms include `1882-11-17`, `1882-11`, `1882`, `10th April, 1911`,
  `[10th April 1914.]`, `April 11, 1932`, `April, 1906`, `1904. March 25`,
  `1908.Dec.4`, `10-3-1942`, `10-2-45`, month lists (`July, August and September,
  1940`), `1934-35`, two ISO dates, and typo months (`Augt.`, `Octr.`, `Jany.`,
  `Feby.`). Brackets and "Issued in" are stripped.
- Combined forms use the Gregorian part: `1290 [16th May 1920]` → 1920-05-16
  with era `1290`; `1909-02-10 (1270 B.E.)` → keeps the ISO date, era `1270 B.E.`.
- Impossible days/months and years outside the allowed range → `needs_review`,
  no date stored.

### 5.3 What the PDF file contributes

`catalogue_pdf_year_window(source_pdf)` reads the year span from the PDF name
(`SV_412_44_1881-84` → 1881–1884). The name gives years, **not quarters**.

- **Allowed years** = the window ±1 (publication can precede registration); with
  no window, 1867–1946. Anything else is `needs_review`.
- **Two-digit years** (`10-2-45`) become 18yy or 19yy by whichever falls in the
  window; if neither does (or both), `needs_review`. With no window the century
  is guessed and flagged.
- **Partial dates** ("April 10"): the year is taken from the nearest *dated*
  neighbour before/after in the same PDF (ordered by `pdf_page`, `entry_index`) —
  the candidate year closest to the neighbour — or from the window if it is a
  single year. Status `inferred`.
- **Ditto**: copies the previous dated entry (a printed day keeps that month/year).
- **No date at all**: an *estimated window* — the entry's own `quarter` if it has
  one (`1883Q2`), else the span of the neighbouring dated entries (±3 months if
  only one side), else the PDF's year span. These are estimates for sorting and
  filtering, never written into `date_start`.

### 5.4 Running it

- New or re-synced pages are dated automatically by `sync_canonical_entries()`.
- Existing rows: after the migrations apply, run the **Backfill canonical dates**
  workflow (or `scripts/backfill_canonical_dates.sql` in the SQL editor). It is
  safe to re-run, e.g. after changing the parser. The backfill is deliberately
  *not* in a migration: inside Supabase's migration runner it hit the statement
  timeout (the neighbour lookup needed the `…_pdf_order_idx` index and an
  `ANALYZE`; with them, 60k rows take seconds).

## 6. Reviewer notes

`canonical_entry_notes` stores a free-text note per `(page_id, entry_index)`.
It is *not* a foreign key into the canonical copy (which is rebuilt); each save
snapshots `source_extraction_id`, `model_tag`, `priority`, `serial`, `reg` and
`title` so the note can be traced back. The Canonical review table autosaves a
note when the box loses focus, plus a "Save all notes on this page" button.
Notes record no author (the dashboard has a single shared password).
Caveat: if a different extraction wins later, `entry_index` may point at a
different entry — the snapshot title/serial is how to spot that.

## 7. Where to see it (web app)

| Page | Purpose |
|---|---|
| **Canonical review** (`/qc-canonical`) | One row per entry from `canonical_entries_view`; filter and sort on any column; page id → QC review (image + extraction) in a new tab, "image" → the page scan in a new tab; notes column. `/qc-canonical/<page_id>` is a per-page detail view with all fields, original title with its translation, and prev/next/go-to. |
| **Bulk errors** (`/bulk-errors`) | Tabs for `lang`, `pubcity`, `pcity`, `publisher`, `printer`: spelling groups with counts, drill-down to entries (links to the page image/QC), rename/merge a group. Tabs for **title errors** (title starts with "Price"/"Copies", "Ditto", no letters), **price errors** (price starts with "Price") and **date errors** (by `date_status`, listing raw date strings with counts, estimated window shown in the drill-down). |

## 8. Operations

- **Migrations** are applied by the Supabase integration on every push to `main`.
  Each runs under Supabase's statement timeout — keep heavy backfills out of
  migrations (see §5.4) and keep migrations idempotent where cheap.
- **Workflows**: *Translate canonical titles to English* (hourly), *Backfill
  canonical dates* (manual). Both use the `b2-upload` environment's secrets.
- Scheduled workflows only run from the default branch.

## 9. Open questions and known limits (for discussion)

1. **Ranking ignores quality.** `textparse:*` beats a fuller extraction from
   another model (page 2224: 1 entry vs 15). Options: require the textparse
   entry count to be within ~70% of the best alternative; or score by filled
   fields. `human:review` stays on top either way.
2. **59% of entries have no printed date.** Is that extractor coverage (some
   models/pages never capture dates), or the register? Worth grouping empties by
   `model_tag` and by PDF before trusting the estimates.
3. **Neighbour inference assumes entries run roughly in date order within a PDF.**
   If a volume is ordered by language section the estimated windows get wide and
   a partial date's year can be wrong. `inferred` rows deserve a spot-check.
4. **Era dates are kept, not converted.** `1333 Hijri` stays as text with no
   Gregorian date; Hijri/Fasli/Samvat conversion is not attempted.
5. **"Ditto" in titles** ("Ditto Part VIII. for September 1882") is not resolved
   to the full title — only dates and the other fields are.
6. **Out-of-range years** (e.g. 1971–74, or 1804–1866) are flagged
   `needs_review` rather than corrected; some may be genuine late issues.
7. **Notes and entry_index** can drift if the winning extraction changes (§6).
8. **Value-map auto-grouping for `publisher`/`printer`** removes punctuation and
   case only, so it can merge two genuinely different names that differ just by
   punctuation; check groups with several spellings.
9. **Misplaced values** (a price in the title, an edition in the date) are
   detected and listed but not yet moved to the right field automatically.
