# Punjab Data Project

**A computational sociological analysis of British Punjab through the imperial print register, 1867–1942.**
Thomas Graves, with Prof. Emmett Davis.

## The live explorer

**→ [https://tgraves719.github.io/Punjab-Data-Project/](https://tgraves719.github.io/Punjab-Data-Project/)** *(GitHub Pages, served from `docs/`)*

An interactive explorer of three complete years, **1910–1912**: all twelve quarterly
*Catalogues of Books registered in the Punjab* under Act XXV of 1867 and Act X of 1890 —
**4,502 entries, 6,944,051 registered copies**, 350 printers, 1,726 publishers, 59 printing
cities (India Office Records SV 412/44, Punjab, Vol 13). Filterable entry table,
printer–publisher network, script-market analysis, curated exhibits, and a built-in scan
viewer that opens every record's source page image.

## What this repo contains

| Path | Contents |
|---|---|
| `docs/` | The published site: the self-contained explorer (`index.html`) plus the rendered page scans (`pages/<quarter>/`) it links to |
| `pipeline/` | Extraction pipeline: `render.py` → per-page extraction JSONs → `extract_api.py` (Batch API) → `postprocess.py` (normalization, SQLite) → `validate.py` (sequence checks, adjudication queues); `schema.md`, `aliases.json`, per-quarter manifests |
| `pipeline/constraints.py` | Independent quality check built on the register's own grammar and entity vocabulary; ranks the adjudication queue (D-018) |
| `pipeline/localize.py`, `lines.py`, `crops.py` | Native-script title localization: finds the vernacular title within each entry and pairs it with its printed romanization |
| `pipeline/data/<quarter>/extractions/` | The verbatim record layer: one JSON per catalog page, the catalog's own words preserved (misprints, editorializing and all) |
| `pipeline/data/<quarter>/out/` | Derived open data: `entries.csv`, `adjudication_queue.csv`, `validation_report.md` |
| `pipeline/data/<quarter>/marginalia_*.md` | Documentation of the handwritten verso indexes found in the bound volumes |
| `scripts/process_pcloud.py`, `.github/workflows/process-pdfs.yml` | On-demand pipeline: pCloud source PDFs → LLM-vision-ready images (optionally single-page PDFs too) → Backblaze B2, status tracked in Postgres (see below) |
| `scripts/extract_with_llm.py`, `.github/workflows/extract-pages.yml` | On-demand pipeline: B2 page images → catalogue entries via a local vision LLM (Ollama, on-runner) → Postgres (see below) |
| `scripts/audit_b2_pages.py`, `scripts/resolve_duplicate_pages.py`, `scripts/remove_unlinked_images.py`, `scripts/remove_stale_pages.py`, `.github/workflows/audit-and-clean-b2.yml` | Audits B2 page images against `pages`, then fixes what it finds: cross-account duplicates, unlinked images, and stale `pages` rows (see below) |
| `supabase/migrations/` | Postgres schema for the above: `pcloud_files`, `pages`, `llm_extractions`, `catalogue_entries` (see below) |
| `analysis/slice_1910/` | Analysis over the corpus: `build_network.py`, `script_market.py`, `build_site.py` (regenerates `docs/index.html`) |
| `analysis/ocr_lab/` | The native-script workstream: legibility measurements (`E0B_RESULTS.md`), localization results, and `REIMAGING_PILOT.md` — the 21-page experiment that decides whether re-imaging the volumes is worth buying |
| `analysis/integrity/` | Sweeps testing whether the stored record matches its own specification (`INTEGRITY_SWEEP.md`) |
| `dialectic/` | The method dialogue behind the August decisions. `dead_ends.md` first |
| `OCR_RESEARCH_AGENDA.md` | Governing document for transcription and extraction |
| `PLAN.md` / `DECISIONS.md` | Plan of record and the numbered decision log (D-001…) governing every normalization fold and method choice |

## Method in one paragraph

Three layers, kept separate: **page image → verbatim record → normalized layer.**
Every entry carries full provenance (printed page + PDF page); the catalog's wording is not
silently corrected — uncertain readings are flagged into per-quarter adjudication queues with
stated reasoning. Registration numbers run as one annual sequence and serial numbers chain
across quarters within each language–topic section; both are used as validation instruments.
The verbatim layer has been swept against the extractor's own flags and holds with five
identified exceptions (`analysis/integrity/INTEGRITY_SWEEP.md`).

## Two things to know before querying the data

- **`copies` is a TEXT column and 12.7% of its values carry a thousands separator.**
  `sum(cast(copies as integer))` silently undercounts by 16.7%. Use
  `sum(cast(replace(copies, ',', '') as integer))`. See `analysis/integrity/INTEGRITY_SWEEP.md`.
- **The register's own completeness is stratified by language.** `method` is blank for 3.3%
  of Urdu entries but 71.5% of Punjabi and 88.2% of Hindi; `char` is annotated for 28.9% of
  Punjabi entries against 0.7% of Urdu. Cross-language comparison on sparse fields needs a
  recorded-versus-blank control. This is a property of the imperial record, not of the
  extraction. See `analysis/ocr_lab/E0B_RESULTS.md` §3.

## What is deliberately not here

- The bound-volume scans (~25 GB of India Office PDFs) — only the per-page renders needed by
  the explorer are published, under `docs/pages/`.
- The SQLite database (`punjab.db`) — regenerable, see below.
- Private working material (interpretive memos, correspondence, collaborator transcription
  files).

## Rebuilding the site

```
cd pipeline
python postprocess.py manifest_1910Q1.json   # repeat for all twelve quarters: rebuilds punjab.db
cd ../analysis/slice_1910
python build_network.py && python script_market.py
python build_site.py --public                # web build: no local-path PDF links
cp out/explore_1910_1912.html ../../docs/index.html
python build_site.py                         # local build (with PDF deep-links)
```

## pCloud → B2 scan pipeline

`scripts/process_pcloud.py`, run on demand via the `.github/workflows/process-pdfs.yml`
GitHub Actions workflow, pulls the source volume PDFs from a public pCloud folder, renders
each page as a 200 DPI WebP image for LLM vision input, and uploads it to a Backblaze B2
bucket (via B2's S3-compatible API) — the image is rendered directly from the source PDF, no
intermediate per-page PDF needed. **Postgres (Supabase) is the resumability ledger**, not B2:
every page's upload is recorded as a row in the `pages` table (see "Processing database"
below), checked before anything is redone, so a run that hits the 5-hour GitHub Actions runner
ceiling exits `42`, flags this in the run's job summary, and stops — a human re-runs the
workflow (Actions → *Process pCloud PDFs to B2* → **Run workflow**) to pick up where it left
off; nothing needs re-checking or re-configuring first.

A single-page PDF per page isn't currently used by anything downstream (only the WebP images
feed the LLM extraction stage), so it's **not** split out or uploaded by default. Tick
**Also split and upload a single-page PDF per page** when running the workflow (or set
`UPLOAD_PAGE_PDFS=true` if running the script directly) to bring it back — output goes to
`pages/{folder}/{stem}/page_XXXX.pdf`, same as before.

The job runs against a GitHub **environment** named `b2-upload` (Settings → Environments →
New environment) rather than plain repository secrets, so a run can be gated behind manual
approval before any secret is exposed:

1. Create the `b2-upload` environment (rename it in `.github/workflows/process-pdfs.yml`'s
   `environment:` key if you'd rather call it something else).
2. Add **Required reviewers** under that environment's protection rules — every run of this
   workflow will then pause at "Waiting for review" until one of the listed reviewers
   approves it, before the job (and its secrets) starts.
3. Add these secrets to the *environment* (not the repository's plain Actions secrets):

   | Secret | Purpose |
   |---|---|
   | `B2_ENDPOINT` | The bucket's B2 S3-compatible endpoint, e.g. `https://s3.us-west-004.backblazeb2.com` (find it on the bucket's details page) |
   | `B2_KEY_ID` / `B2_APPLICATION_KEY` | A B2 application key scoped to the destination bucket (Account → App Keys) |
   | `B2_BUCKET_NAME` | Destination B2 bucket |
   | `SUPABASE_DB_URL` | Postgres connection string for the processing database — see "Processing database" below for the exact connection string to use (not the default one Supabase shows first) |
4. Set the pCloud share link's code as a **repository variable** named `PCLOUD_CODE`
   (Settings → Secrets and variables → Actions → *Variables* tab — not *Secrets*, since it's
   just the public share-link identifier, not a credential). The script reads it from the
   `PCLOUD_CODE` environment variable and fails fast if it isn't set, so the source link is
   explicit and changeable without editing code.

## Local-LLM catalogue extraction

`.github/workflows/extract-pages.yml`, run on demand, is the next stage after the pCloud → B2
pipeline above: it reads the page images already uploaded to B2 — looked up via Postgres, not
by listing B2 — and runs each one through a vision LLM to transcribe catalogue entries,
writing one row per entry into the `catalogue_entries` table (see "Processing database"
below), following the same per-entry schema as the existing extraction pipeline
(`pipeline/schema.md`) so it's compatible with `pipeline/postprocess.py` once assigned a
`quarter`. This workflow never writes to B2 at all — B2 is read-only from its point of view.

The model runs **locally on the GitHub Actions runner** via [Ollama](https://ollama.com) — no
external API, no API key, nothing sent off-runner except to B2. GitHub-hosted runners have no
GPU, so this is CPU inference and will be slow per page; the workflow uses the same
runtime-guard-and-manual-resume pattern as `process-pdfs.yml` (exits `42` after ~5 hours, a job
summary notice tells you to re-run it) rather than trying to finish in one run.

`workflow_dispatch` takes a `model` choice — a shortlist of small (1B-8B), non-cloud-gated
vision models pulled from Ollama's current vision listing (`minicpm-v4.6`, `qwen3-vl:2b`,
`qwen3-vl:4b`, `gemma4:e2b`, `glm-ocr`, `minicpm-v4.5`), or `all` to fan them out as a parallel
matrix so you can bake off quality/speed across models on the same page images. `glm-ocr` is
included because it's purpose-built for document OCR — exactly this task. Default is
**`minicpm-v4.5`** (8B): the largest model in this CPU-feasible set, and MiniCPM-V's line has a
well-established OCR/document-understanding benchmark track record combined with being a full
general-purpose model, so it should follow the 25-field schema more reliably than a narrower or
smaller model — reasoned from published model positioning, not benchmarked against this
project's actual pages, so treat an `all` bake-off as the real source of truth once you can
eyeball output quality yourself.
Each model's output is namespaced by `model_tag` (the `model` value slugified — `:` and other
non-alphanumeric characters replaced with `-`, e.g. `qwen3-vl:2b` becomes `qwen3-vl-2b`) via a
unique `(page_id, model_tag)` constraint on `llm_extractions`, so different models' runs never
clobber each other and can be compared side by side with a plain SQL query. Ollama's library
can rename or drop model tags over time — if `ollama pull` fails for one of these, check
[ollama.com/library](https://ollama.com/library) for the current tag and update the `options`
list in the workflow.

Uses the same `b2-upload` environment as `process-pdfs.yml` (for reading page images) plus
`SUPABASE_DB_URL` (for writing results) — no other secrets specific to this workflow. Fetching
each page's image bytes to send to Ollama still costs one real download from B2 per
not-yet-extracted page (unavoidable — the model needs the actual pixels), so a corpus with more
than ~2,500 not-yet-extracted pages may need multiple days/resumed runs against a free-tier B2
account's daily transaction cap; that's expected, not a bug.

## Second B2 account

Both B2-backed workflows (`process-pdfs.yml`, `extract-pages.yml`) take a **B2 account** choice
(`1` or `2`, default `1`) on `workflow_dispatch` — useful once a bucket fills up (storage or a
daily transaction cap) and you've created a second bucket, possibly under a whole separate
Backblaze account, to keep going.

Add a second full set of secrets to the same `b2-upload` environment, suffixed `_2`:

| Secret | Purpose |
|---|---|
| `B2_ENDPOINT_2` | The second bucket's B2 S3-compatible endpoint |
| `B2_KEY_ID_2` / `B2_APPLICATION_KEY_2` | An application key scoped to the second bucket |
| `B2_BUCKET_NAME_2` | The second bucket's name |

The original `B2_ENDPOINT`/`B2_KEY_ID`/`B2_APPLICATION_KEY`/`B2_BUCKET_NAME` secrets remain
account `1` — nothing to rename. Account `2` is entirely optional: leave its four secrets unset
and both workflows behave exactly as if there were only ever one account.

**The dropdown only controls where *new* work is written — it isn't a plain swap, and it isn't
a blind switch either:**

- **`process_pcloud.py`** checks the `processed/...` completion markers in *every* configured
  account before touching a PDF. A PDF already fully done in account `1` is skipped even when
  you're running with account `2` selected — it is never redone or re-split just because the
  active account changed. A PDF not yet done in *either* account is (re)processed entirely into
  whichever account is currently active; it's never resumed part-way from a different account,
  which would leave its pages split across two buckets.
- **`extract_with_llm.py`** doesn't take a `b2_account` choice at all — it doesn't write to B2,
  and Postgres already records exactly which account/bucket holds each page's image (`pages.
  b2_account`/`b2_bucket`), so it always fetches from the right place regardless of which
  account was active when that image was uploaded. Extraction dedup (has this page already
  been processed by this model?) is tracked per page in `llm_extractions`, independent of B2
  accounts entirely.
- For `process_pcloud.py`, all *new* uploads for a run go to the account you picked — nothing
  is copied or merged between buckets, and the workflow never writes to a non-active account.

## Processing database (Supabase)

A Supabase Postgres project (linked to this repo via Supabase's GitHub integration, which
deploys `supabase/migrations/*.sql` automatically) is the source of truth for pipeline status
and LLM output. B2 holds every actual byte (page images, optionally page PDFs); Postgres tracks
what's been uploaded where, and holds every extracted catalogue entry for analysis.

Schema (`supabase/migrations/20260909140000_init_processing_schema.sql`):

| Table | One row per... |
|---|---|
| `pcloud_files` | pCloud PDF (keyed by pCloud's own file id — stable, no surrogate key needed) |
| `pages` | page of a PDF — which B2 account/bucket/key holds its image, and — tracked separately, since a later run can upload it under a different active account — the account/bucket/key of its optional single-page PDF, and when each was uploaded |
| `llm_extractions` | (page, model) extraction attempt — status, the model's raw JSON response, error message if it failed |
| `catalogue_entries` | extracted catalogue entry — the same ~30 typed fields as `pipeline/schema.md`, one row per entry (a page can hold several) |

A `catalogue_entries_full` view joins all four tables, so a catalogue entry can always be
traced back to its exact image (`b2_account`/`b2_bucket`/`image_key`) and its original pCloud
file (`pcloud_fileid`/`pcloud_name`/`pcloud_folder`) in one query — that's "linked to the image
and the original PDF file in pCloud." Both `process_pcloud.py` and `extract_with_llm.py` query
these tables directly to decide what's already done; neither lists B2 objects for status
anymore (only for the byte data itself).

**Required secret:** `SUPABASE_DB_URL` in the `b2-upload` environment — a Postgres connection
string. **Use the connection *pooler* string, not Supabase's default "direct connection" one:**
Supabase's direct connection (port 5432, `db.<ref>.supabase.co`) is IPv6-only unless you've
bought the IPv4 add-on, and GitHub Actions runners are IPv4-only — a direct-connection string
will just hang and time out from CI. In the Supabase dashboard's "Connect" panel, copy the
**Session pooler** or **Transaction pooler** string instead (hostname like
`aws-0-<region>.pooler.supabase.com`) — both support IPv4.

**Audit and clean up B2/Postgres**: `.github/workflows/audit-and-clean-b2.yml` runs one audit
step and three cleanup scripts in sequence, then a final audit if anything actually changed:

1. **Audit** (`scripts/audit_b2_pages.py`) — read-only (never writes to B2 or Postgres), lists
   every `images/**/*.webp` object in each configured B2 account/bucket and cross-references it
   against `pages`, reporting three things in the job summary: images with no matching `pages`
   row at all, images whose `pages` row points at a different account/bucket/key than where the
   image actually is, and any page whose image was uploaded to more than one B2 account (most
   likely from switching `B2_ACTIVE_ACCOUNT` without `process_pcloud.py` knowing the other
   account already had that page). Exits non-zero if it finds anything — this step is allowed
   to "fail" without failing the job, since that's expected going into the cleanup steps below.
2. **Resolve duplicate pages** (`scripts/resolve_duplicate_pages.py`) — lists every
   `images/*.webp` object in both accounts and, for every page found in *either* one, works out
   where it should be recorded: account 1 (authoritative, never touched) if it's there,
   otherwise account 2, wherever it actually is. The `pages` row for that page is created or
   corrected to match — whether it was missing entirely, pointing at the wrong
   account/bucket/key, or already right — and only once the row is confirmed to point at
   account 1 is the redundant account-2 copy deleted (only when the page exists in both; an
   account-2-only page is left alone, since it's simply not processed under account 1 yet, not
   a duplicate). The row is always fixed before the account-2 object is deleted, so an
   interrupted run never leaves a row pointing at a just-deleted object, and a page that can't
   be mapped to a `pcloud_files` row is skipped rather than acted on blindly.
3. **Remove unlinked images** (`scripts/remove_unlinked_images.py`) — deletes every
   `images/*.webp` object, in either account, that has no matching `pcloud_files` row at all.
   Such an image isn't tracked by anything, and `process_pcloud.py` doesn't check what's already
   in B2 before it (re)uploads a PDF's pages anyway — it decides purely from
   `pcloud_files`/`pages` state — so keeping it around preserves nothing. A later
   `process_pcloud.py` run reprocesses that PDF (if it's still on pCloud) completely fresh. This
   leaves alone any image whose PDF already has a `pcloud_files` row (even if a specific page's
   `pages` row is missing — step 2 fixes that by creating the row, not by deleting anything) and
   never touches page PDFs or extraction output.
4. **Remove stale pages** (`scripts/remove_stale_pages.py`) — the mirror of step 3: deletes
   every `pages` row whose (folder, stem, page number) has no backing image in *either* B2
   account. Such a row has nothing to extract from, link, or serve. "No backing image" is
   checked against both accounts, not just the account/bucket the row happens to currently
   record, so a row step 2 would instead repoint (its image exists, just under the other
   account) is never mistaken for stale. Deleting a `pages` row cascades to its
   `llm_extractions` and `catalogue_entries` rows via the schema's own `ON DELETE CASCADE`.
5. **Final audit** — only runs if step 2, 3, or 4 actually changed something; its exit code (0
   clean, 1 issues remain) becomes this job's exit code, so a still-dirty state after cleanup
   shows up as a failed run instead of a silently-skipped one.

Steps 2-4 default to a dry run (each reports what it would do, changes nothing) every time you
*don't* tick **Actually commit changes** on the workflow's dispatch form; the audits are always
read-only regardless. All three writes are upserts/idempotent, so re-running is always safe.

## Security scanning

`.github/workflows/security-scans.yml` runs on every pull request against `main`, weekly
(Mondays), and on demand — all with free/open-source tools, no paid service or license:

| Job | Tool | Checks |
|---|---|---|
| CodeQL | [`github/codeql-action`](https://github.com/github/codeql-action) | Python SAST (free for public repos) |
| Bandit + Semgrep | `bandit`, `semgrep` | Python-specific and general-purpose SAST (`p/security-audit`, `p/secrets`, `p/owasp-top-ten` rulesets) |
| Gitleaks | [`gitleaks`](https://github.com/gitleaks/gitleaks) | Secret scanning across the working tree |
| pip-audit | [`pip-audit`](https://github.com/pypa/pip-audit) | Known CVEs in `scripts/requirements.txt` |
| ZAP baseline | [OWASP ZAP](https://www.zaproxy.org/) | Passive DAST against the live explorer |

Findings from the SARIF-emitting scanners (CodeQL, Bandit, Semgrep, Gitleaks) land in the
repo's **Security → Code scanning alerts** tab; pip-audit's output goes to the run's job
summary. None of these jobs currently block merges — they're wired up to build visibility
first — so tighten branch protection around them once the initial signal has been triaged.

**DAST caveat:** this repo's only deployed surface is the static GitHub Pages explorer
(`docs/`) — there's no backend/API and no per-PR preview deployment. The ZAP baseline job
always scans the live production URL, so a PR run is a drift/regression check against
production, not a test of that PR's own changes. If preview deployments are added later,
point the `target` input at the preview URL for PR runs instead.

## Source

*Catalogue of Books registered in the Punjab under Act XXV of 1867 and Act X of 1890*,
quarterly, British Library India Office Records **SV 412/44** (Punjab; 26 bound volumes,
1867–1942 — the years here are Vol 13). Public-domain government record. Print runs measure
publisher supply decisions under a legal-deposit regime — not readership, not literacy.

## Licence and citation

Three licences, because this repository holds three different kinds of thing: **code** is
GPL-3.0-or-later, **data** is CC0 (it is a transcription of a public-domain government
record, and mostly not ours to license), and **prose** is CC BY 4.0. Full statement and
reasoning in [`LICENSING.md`](LICENSING.md); machine-readable citation in `CITATION.cff`.

Citation is requested, not required. If you use the data, please cite the source record
too — and tell us what you find wrong in it.
