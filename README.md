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
| `scripts/process_pcloud.py`, `.github/workflows/process-pdfs.yml` | On-demand pipeline: pCloud source PDFs → single-page PDFs + LLM-vision-ready images → Backblaze B2 (see below) |
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
GitHub Actions workflow, pulls the source volume PDFs from a public pCloud folder, splits
each into single-page PDFs, renders each page as a 200 DPI WebP image for LLM vision input,
and uploads both to a Backblaze B2 bucket (via B2's S3-compatible API). B2 itself is the
resumability ledger: every page PDF, page image, and per-PDF `processed/.../<stem>.done`
marker is checked against the bucket before being redone, so a run that hits the 5-hour
GitHub Actions runner ceiling exits `42`, flags this in the run's job summary, and stops —
a human re-runs the workflow (Actions → *Process pCloud PDFs to B2* → **Run workflow**) to
pick up where it left off; nothing needs re-checking or re-configuring first.

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
