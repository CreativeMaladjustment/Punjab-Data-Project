# Gemini catalogue extraction

`.github/workflows/extract-pages-gemini.yml` runs the same structured-extraction task as
`.github/workflows/extract-pages.yml` (see README.md's "Local-LLM catalogue extraction"), but
against Google's hosted **Gemini API** instead of a local Ollama model. It's another peer in
the same model bake-off — its output is namespaced under its own `model_tag` in
`llm_extractions`, exactly like `glm-ocr`, `minicpm-v4.6`, etc. — not a replacement for the
Ollama pipeline. `scripts/extract_with_gemini.py` is the script; this document is about
running and operating it. For the code-level reasoning (rate-limit handling, why this is a
separate script, why it's single-worker), see that script's own module docstring — it's kept
current there, not duplicated here.

## Setup

1. Create a Gemini API key at [aistudio.google.com/apikey](https://aistudio.google.com/apikey)
   (or via Google Cloud Console, for a paid-tier key). The free tier is enough to run this
   workflow as configured below.
2. Add it as `GEMINI_API_KEY` in the repo's `b2-upload` GitHub environment (Settings →
   Environments → `b2-upload` → Environment secrets) — the same environment the Ollama pipeline
   and `SUPABASE_DB_URL`/B2 secrets already live in, so this workflow is gated behind the same
   manual-approval rule if one is configured there.

No other setup is needed: this workflow never touches B2 for writing (same as the Ollama
pipeline, it only *reads* page images from B2) and doesn't install or cache anything
model-sized locally — there's no Ollama server here, just Python calling a hosted API, so the
job itself is much lighter/faster to start than `extract-pages.yml`'s.

## Running it

**Scheduled**: every 6 hours total, alternating between two models via two `cron:` entries —
`0 4,16 * * *` runs `gemini-3.1-flash-lite`, `0 10,22 * * *` runs `gemini-3.5-flash-lite` (both
UTC), each landing two of the four daily slots. The workflow's `env:` block picks the model by
matching `github.event.schedule` (the exact cron string GitHub reports for whichever entry
fired) against these two strings. Both models otherwise run with `source_model=(none)`,
`allow_already_extracted=false` — a normal run against whatever's still missing a successful
extraction from any source. Splitting the four daily slots across two models' independent
free-tier daily quotas means roughly twice the pages/day get attempted before either model's
own quota caps out for the day, versus one model spending its single quota bucket across all
four slots. Like every workflow in this repo that uses the `b2-upload` environment, a scheduled
run still queues for manual approval if required reviewers are configured there — the schedule
makes runs *regularly requested*, not unattended.

**Manual** (Actions → *Extract catalogue entries with Gemini* → Run workflow) takes three
inputs:

| Input | Default | Meaning |
|---|---|---|
| `model` | `gemini-3.1-flash-lite` | Which Gemini model to run (see "Model choices" below) |
| `source_model` | `(none)` | Rescue mode — see below |
| `allow_already_extracted` | off | Check this only for a deliberate model comparison run |

## Model choices

| Model | Free-tier pace this workflow uses | Notes |
|---|---|---|
| `gemini-3.1-flash-lite` | ~4s between requests | Materially higher free-tier RPM/RPD than flash, per every source describing it. Scheduled twice a day (see above) |
| `gemini-3.5-flash-lite` | ~4s between requests | Same free-tier ceiling as `3.1-flash-lite` per every source describing it. Scheduled the other two times a day, on its own independent daily quota |
| `gemini-3.6-flash` | ~4s between requests | Offered for comparison, not currently scheduled; its free tier turned out much tighter than assumed — a full 5-hour run only cleared ~24 pages, almost entirely stuck retrying 429s |
| `gemini-2.5-flash-lite` | fallback pace (unlisted in `GEMINI_MODEL_PACING`) | RPD 20, same tight tier as `3.6-flash`; manual/rescue only. Google has it slated to shut down 2026-10-16 |
| `gemini-3.5-flash` | fallback pace | RPD 20; manual/rescue only |
| `gemini-3.7-flash` | fallback pace | RPD 20; manual/rescue only |
| `gemini-3.8-flash` | fallback pace | RPD 20; manual/rescue only |
| `gemini-3-flash-preview` | fallback pace | RPD 20; manual/rescue only; a preview model, so expect more volatility than the non-preview tags |

The five RPD-20 models above aren't in `GEMINI_MODEL_PACING`, so they use the module's
conservative unlisted-model fallback pace rather than an explicit entry — deliberate, not an
oversight, since a 20-request daily cap would exhaust in the first page or two of a normal
run regardless of request pacing; they're offered for a one-off manual dispatch or rescue run,
never the scheduled backlog. `gemini-2.5-flash` (no `-lite`) is *not* offered at all despite
Google's own usage dashboard still showing it a nonzero quota row — it's the same generation
already confirmed retired via the live 404 below; the dashboard quota row apparently persists
after the model itself stops serving requests.

Google retired the generation this workflow originally shipped with
(`gemini-2.5-flash`/`-pro`, `gemini-1.5-flash`/`-pro`) — confirmed via a live 404 from the API
itself ("This model ... is no longer available to new users"). `gemini-3.6-flash` was tried
next as the confirmed replacement, but its free-tier daily quota turned out to be far
stricter than the old flash tier this pacing was originally sized for — see the note above.
`gemini-3.1-flash-lite` is the current default instead. Numbers still vary noticeably by
source, so treat the pacing above as a conservative starting floor, not a guarantee. Check
[ai.google.dev/gemini-api/docs/rate-limits](https://ai.google.dev/gemini-api/docs/rate-limits)
for what Google currently publishes before assuming the pacing above is still accurate; the
pacing in `scripts/extract_with_gemini.py`'s `GEMINI_MODEL_PACING` is a floor, not the real
protection — the script also retries a real `429` **or `5xx`** with backoff (honoring
`Retry-After` when Google sends one), so a stale pacing number doesn't break the run outright,
just makes it less efficient in either direction. If Google retires `gemini-3.1-flash-lite`
too, the API's own error message names its replacement — that's how this workflow's default
was fixed last time.

This workflow deliberately runs **one worker, not a matrix**: every worker would pace against
the *same* shared per-project Gemini quota, so more workers here means more `429`s, not more
throughput — unlike the Ollama pipeline's CPU inference, where more runners really do mean
more parallel work.

## Rescue mode (`source_model`)

Set `source_model` to point Gemini at exactly the pages some *other* model already gave up on
— a *capped* content failure (`status='failed'`, `content_failure`, `attempt_count >= 2`) under
that model's own tag — instead of running `model` against the whole backlog. This works
**across providers**: `source_model` can name an Ollama model (`glm-ocr`, `minicpm-v4.6`, …)
just as easily as another Gemini model, since what makes a page rescuable is entirely about its
`model_tag` history in Postgres, not which script produced it. For example, `source_model:
glm-ocr` with `model: gemini-3.1-flash-lite` sends exactly the pages `glm-ocr` capped out on to
Gemini, without also re-running Gemini against everything `glm-ocr` already succeeded on.

## Coverage vs. comparison (`allow_already_extracted`)

By default, this workflow — like `extract-pages.yml` — skips any page that already has a
successful extraction from **any** source: another vision model, this same Gemini pass under
a different run, the `textparse:*` OCR-text backfill (`parse-ocr-text.yml`), or a human
correction. The goal is coverage of the whole backlog, not spending Gemini's quota
re-extracting a page that already has data. Check `allow_already_extracted` only when you
specifically want to compare Gemini's output against another model's on the *same* pages —
this restores the old "run against everything, regardless of what already succeeded"
behavior for that one run.

## What gets written

Two things per page, both namespaced under `model_tag` = the slugified `model` input (e.g.
`gemini-3.1-flash-lite`, unchanged since it's already tag-safe):

- **`llm_extractions`** / **`catalogue_entries`** — the structured extraction, same shape as
  every other model's output, reviewable on the QC page and counted on the Progress page
  exactly like `glm-ocr`'s or any Ollama model's.
- **`page_ocr_text`** — a full-page verbatim transcription, independent of and unaffected by
  whether the structured extraction above succeeded or failed. This costs a second API call per
  page (see "Model choices" above for what that does to daily throughput) but means the
  transcription is already in Postgres rather than needing a separate image-based pass later.

## Runtime guard

Same 5-hour cap as every other extraction workflow in this repo: the script checks its own
elapsed time every iteration and exits `42` (flagged in the job's summary) rather than risking
a mid-page cutoff from the GitHub Actions runner's own hard limit. Nothing needs manual
cleanup after this — the next scheduled run (or a manual re-run) picks up exactly where the
last one left off, since every claimed-but-unfinished page becomes reclaimable again once its
claim goes stale (see `CLAIM_TIMEOUT_SECONDS` in the script).

## Checking progress

The Progress page (`/progress`) shows this workflow's `model_tag` as its own row, same as every
other model, plus the "pages with data extracted" summary at the top, which counts a success
from *any* source (Gemini included) toward the real goal. For a throughput/ETA estimate specific
to one model, run a query like:

```sql
select count(*) filter (where status in ('success', 'failed')) as processed_last_24h
from llm_extractions
where model_tag = 'gemini-3.1-flash-lite'
  and created_at >= now() - interval '24 hours';
```

(swap the `model_tag` for whichever Gemini model you're tracking).

## Troubleshooting

- **Every page fails immediately with a 401/403** — `GEMINI_API_KEY` is missing, wrong, or
  the environment approval was declined; check Settings → Environments → `b2-upload`.
- **Frequent `429`/`503` lines in the log, run still finishes** — expected occasionally; the
  script retries both with backoff. If it's *constant* — every page taking several minutes of
  retries, only a handful of pages clearing in a full 5-hour run — the model's actual free-tier
  quota is likely much lower than `GEMINI_MODEL_PACING` assumes (this happened with
  `gemini-3.6-flash`; see "Model choices" above). Check Google's current published numbers and
  consider switching `model` to a more generous tier rather than just raising the pacing
  constant.
- **A page keeps failing with the same error every run** — check `llm_extractions.error_message`
  for that `(page_id, model_tag)`; a capped content failure (`content_failure=true`,
  `attempt_count >= 2`) needs a person to look at it (QC page) or a different model
  (`source_model` rescue), not another automatic retry.
