# Hugging Face catalogue extraction

`.github/workflows/extract-pages-hf.yml` runs the same structured-extraction task as
`.github/workflows/extract-pages.yml` and `extract-pages-gemini.yml` (see README.md's
"Local-LLM catalogue extraction" and `GEMINI_EXTRACTION.md`), but against **Hugging Face's
Inference Providers router** instead of a local Ollama model or Google's Gemini API. It's
another peer in the same model bake-off — its output is namespaced under its own `model_tag`
in `llm_extractions`, exactly like `glm-ocr`, `gemini-3.1-flash-lite`, etc. — not a
replacement for either of the other two pipelines. `scripts/extract_with_hf.py` is the
script; this document is about running and operating it. For the code-level reasoning, see
that script's own module docstring — it's kept current there, not duplicated here.

## The free tier is not like Gemini's

This is the single most important thing to know before running this workflow: **Hugging
Face's "free" access is a small monthly dollar credit, not a request quota that resets
daily.** A free HF account currently gets **$0.10/month** of Inference Providers credit (Pro
accounts get $2/month); once it's spent, every call returns HTTP 402 until the credit renews
next month. That's enough for a handful to a few dozen pages a month, not a meaningful
addition to the bulk pipeline the way `glm-ocr` (free local compute) or Gemini's actual
per-day quota are. This workflow was built anyway on the understanding that even a small,
occasional trickle of comparison data has value for the bake-off — see the "What to expect"
section below before assuming it'll process any real fraction of the backlog.

## Setup

1. Create a Hugging Face account at [huggingface.co](https://huggingface.co) if you don't
   have one.
2. Go to **Settings → Access Tokens** (`huggingface.co/settings/tokens`) → **Create new
   token**. Pick **Fine-grained** and check **"Make calls to Inference Providers"** (or use
   the simpler **Read** token type, which covers this too).
3. Copy the token (starts with `hf_...`) — it's shown only once.
4. Add it as `HUGGING_FACE_API_KEY` in the repo's `b2-upload` GitHub environment (Settings →
   Environments → `b2-upload` → Environment secrets) — the same environment the Ollama
   pipeline, Gemini workflow, and `SUPABASE_DB_URL`/B2 secrets already live in, so this
   workflow is gated behind the same manual-approval rule if one is configured there.

No other setup is needed: this workflow never touches B2 for writing (same as the other two
extraction pipelines, it only *reads* page images from B2) and doesn't install or cache
anything model-sized locally.

## Running it

**Scheduled**: every 6 hours (`0 4,10,16,22 * * *` UTC), same anchor as the other two
extraction workflows. Most scheduled runs after the monthly credit is spent exit within
seconds on the first 402 — see "What to expect" below — so this doesn't cost meaningful
runner time even with nothing left to spend.

**Manual** (Actions → *Extract catalogue entries with Hugging Face* → Run workflow) takes
three inputs:

| Input | Default | Meaning |
|---|---|---|
| `model` | `Qwen/Qwen2.5-VL-7B-Instruct` | Which HF model to run |
| `source_model` | `(none)` | Rescue mode — see below |
| `allow_already_extracted` | off | Check this only for a deliberate model comparison run |

## Model choice

The default is **`Qwen/Qwen2.5-VL-7B-Instruct`**: a well-established vision-language model
with strong published document/OCR benchmarks, actively hosted by multiple Inference
Providers (DeepInfra, Together, and others) rather than a brand-new or niche release. That
matters here specifically because this project already hit a model getting pulled out from
under it once (Gemini's `gemini-2.5-flash` retirement — see `GEMINI_EXTRACTION.md`); a
well-established, multiply-hosted model is less likely to disappear the same way.

The full catalogue of `image-text-to-text` models on Hugging Face is much larger (300+) —
[huggingface.co/models?pipeline_tag=image-text-to-text&inference_provider=all](https://huggingface.co/models?pipeline_tag=image-text-to-text&inference_provider=all) —
but most aren't actually deployed by any Inference Provider (a model page existing doesn't
mean it's callable this way; check for provider badges on the model's page, or that calling it
doesn't 404). `HF_MODEL` is an env var override if you want to try a different one — update
the workflow's `model` choice list to match.

## Rescue mode (`source_model`)

Set `source_model` to point this model at exactly the pages some *other* model already gave
up on — a *capped* content failure (`status='failed'`, `content_failure`, `attempt_count >=
2`) under that model's own tag — instead of running `model` against the whole backlog. This
works **across providers**: `source_model` can name an Ollama model (`glm-ocr`,
`minicpm-v4.6`, …) or a Gemini model tag just as easily, since what makes a page rescuable is
entirely about its `model_tag` history in Postgres, not which script produced it. Given how
small the monthly credit is, rescue mode is arguably the *better* default use of it — spending
a handful of calls specifically on pages that failed elsewhere is worth more than a handful of
calls on pages nothing has tried yet.

## Coverage vs. comparison (`allow_already_extracted`)

By default, this workflow — like the other two — skips any page that already has a successful
extraction from **any** source. Given the tiny credit, this default matters more here than
anywhere else in the pipeline: there's no reason to spend a scarce monthly dollar on a page
that already has data. Check `allow_already_extracted` only for a deliberate model comparison
run on pages another model has already succeeded on.

## What gets written

Two things per page, both namespaced under `model_tag` = the slugified `model` input (e.g.
`Qwen/Qwen2.5-VL-7B-Instruct` → `Qwen-Qwen2.5-VL-7B-Instruct`):

- **`llm_extractions`** / **`catalogue_entries`** — the structured extraction, same shape as
  every other model's output, reviewable on the QC page and counted on the Progress page.
- **`page_ocr_text`** — a full-page verbatim transcription, independent of and unaffected by
  whether the structured extraction above succeeded or failed. This costs a second API call
  per page, which — given the tiny budget here — roughly halves how many pages the monthly
  credit reaches. Accepted deliberately, same tradeoff already made for Gemini.

## What to expect: the credit runs out fast

Once the month's `$0.10` (or `$2` on Pro) is spent, the Inference Providers router returns
HTTP 402. Unlike a 429 (rate limit, worth retrying) this won't clear up by waiting, so
`extract_with_hf.py` stops the run immediately instead of retrying or burning the rest of the
5-hour window — this is expected behavior, not a bug, and the workflow's job summary says so
explicitly (exit code 43, distinct from the runtime guard's 42) rather than showing as a
failure. The in-flight page's claim just goes stale (same `CLAIM_TIMEOUT_SECONDS` mechanism
every extraction script uses) and gets retried automatically once the credit renews next
month — nothing needs manual cleanup.

If you want more headroom, a **Pro** account ($9/month as of this writing) gets $2/month of
Inference Providers credit instead of $0.10 — 20x more, though still modest next to Gemini's
actual per-day request quota.

## Runtime guard

Same 5-hour cap as every other extraction workflow in this repo, for the (unlikely, given the
credit constraint) case where the credit is large enough for a run to actually approach it: the
script checks its own elapsed time every iteration and exits `42` (flagged in the job's
summary) rather than risking a mid-page cutoff from the GitHub Actions runner's own hard
limit.

## Checking progress

The Progress page (`/progress`) shows this workflow's `model_tag` as its own row, same as
every other model, plus the "pages with data extracted" summary at the top, which counts a
success from *any* source (this one included) toward the real goal. For a throughput estimate
specific to this model, run a query like:

```sql
select count(*) filter (where status in ('success', 'failed')) as processed_last_30d
from llm_extractions
where model_tag = 'Qwen-Qwen2.5-VL-7B-Instruct'
  and created_at >= now() - interval '30 days';
```

## Troubleshooting

- **Every page fails immediately with a 401/403** — `HUGGING_FACE_API_KEY` is missing, wrong,
  or lacks the "Make calls to Inference Providers" permission; check Settings → Environments →
  `b2-upload`, and the token's scopes at `huggingface.co/settings/tokens`.
- **Job summary shows "credit exhausted"** — expected, not a bug; see "What to expect" above.
  No action needed unless you want it to resume sooner (upgrade to Pro, or wait for next
  month's renewal).
- **A page keeps failing with the same error every run** — check `llm_extractions.error_message`
  for that `(page_id, model_tag)`; a capped content failure (`content_failure=true`,
  `attempt_count >= 2`) needs a person to look at it (QC page) or a different model
  (`source_model` rescue), not another automatic retry.
- **`model` fails every page with `HF router returned 400: ... "not supported by any provider
  you have enabled"`** — this happened in production with the original default,
  `Qwen/Qwen2.5-VL-7B-Instruct` (see `.github/workflows/list-hf-models.yml` below). Two
  different causes produce the exact same message: the model genuinely isn't deployed by any
  Inference Provider right now, *or* it is, but your account hasn't enabled that provider (check
  `huggingface.co/settings/inference-providers`) — a model page existing on Hugging Face
  doesn't guarantee either. Run **Actions → *List available Hugging Face models*** (manual
  dispatch, `scripts/list_hf_models.py`) to get an authoritative answer instead of guessing from
  the model's page: it live-tests a batch of trending vision-language models against this
  account's actual key through the same router endpoint the extraction pipeline uses, and
  reports which ones genuinely work right now. Set `HF_MODEL` in `extract-pages-hf.yml` to
  whichever one it confirms.
