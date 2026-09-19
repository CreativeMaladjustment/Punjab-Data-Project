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

**Scheduled**: once a week, Mondays at 04:00 UTC (`0 4 * * 1`) — same anchor hour as the
other two extraction workflows, but weekly rather than every 6 hours, since the monthly
credit is small enough that running more often just means more runs finding no credit
left, not more pages processed. Most scheduled runs after the monthly credit is spent
exit within seconds on the first 402 — see "What to expect" below — so this doesn't cost
meaningful runner time even with nothing left to spend.

**Manual** (Actions → *Extract catalogue entries with Hugging Face* → Run workflow) takes
three inputs:

| Input | Default | Meaning |
|---|---|---|
| `model` | `google/gemma-4-31B-it` | Which HF model to run |
| `source_model` | `(none)` | Rescue mode — see below |
| `allow_already_extracted` | off | Check this only for a deliberate model comparison run |

## Model choice

**Don't pick a model from its Hugging Face page or a third-party writeup and trust it'll
work.** This pipeline's original default, `Qwen/Qwen2.5-VL-7B-Instruct`, looked reasonable by
every indirect signal available (well-established, benchmarked, "actively hosted by multiple
providers" per third-party pricing writeups) and still failed on every page in production with
`"not supported by any provider you have enabled"`. A model's page existing on Hugging Face
does not mean your account's Inference Providers configuration can actually call it.

Instead, run **Actions → *List available Hugging Face models*** (`scripts/list_hf_models.py`)
whenever you need to pick or re-verify a model — it live-tests real candidates against this
account's actual key through the same router endpoint the extraction pipeline uses, and
reports which ones genuinely work right now. The current default, **`google/gemma-4-31B-it`**,
was chosen this way (run 35446611937): of 6 models that passed the live test, it's the one that
also returned real, non-empty response content under the probe, not just a bare 200. The
workflow's `model` choice list offers all 6 confirmed-working candidates from that run —
`google/gemma-4-31B-it`, `google/gemma-4-26B-A4B-it`, `Qwen/Qwen3.6-27B`,
`Qwen/Qwen3.6-35B-A3B`, `zai-org/GLM-5.3-Flash`, `moonshotai/Kimi-K3` — see
`scripts/hf_config.py`'s docstring for the full detail on why the other 5 are offered as
alternates rather than the default.

`HF_MODEL` is an env var override (falls back to `scripts/hf_config.py`'s `DEFAULT_HF_MODEL`
when unset) if you want to try a model not in the dropdown — update the workflow's `model`
choice list to match once you've confirmed it actually works.

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
`google/gemma-4-31B-it` → `google-gemma-4-31B-it`):

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
2-hour window — this is expected behavior, not a bug, and the workflow's job summary says so
explicitly (exit code 43, distinct from the runtime guard's 42) rather than showing as a
failure. The in-flight page's claim just goes stale (same `CLAIM_TIMEOUT_SECONDS` mechanism
every extraction script uses) and gets retried automatically once the credit renews next
month — nothing needs manual cleanup.

If you want more headroom, a **Pro** account ($9/month as of this writing) gets $2/month of
Inference Providers credit instead of $0.10 — 20x more, though still modest next to Gemini's
actual per-day request quota.

## Runtime guard

A 2-hour cap — shorter than the other extraction workflows' 5-hour guard, since this
pipeline's tiny monthly credit exhausts in well under an hour once it's actually spent, so
there's no benefit to a longer window. For the (unlikely, given the credit constraint) case
where the credit is large enough for a run to actually approach it: the
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
where model_tag = 'google-gemma-4-31B-it'
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
  `Qwen/Qwen2.5-VL-7B-Instruct`, and every one of the current dropdown's options was verified
  by actually clearing this exact check, not by reading a model's page. Two different causes
  produce the exact same message: the model genuinely isn't deployed by any Inference Provider
  right now, *or* it is, but your account hasn't enabled that provider (check
  `huggingface.co/settings/inference-providers`) — a model page existing on Hugging Face
  doesn't guarantee either. If this happens again (Inference Providers can stop hosting a model
  with no more notice than Google gave for `gemini-2.5-flash`), run **Actions → *List available
  Hugging Face models*** (manual dispatch, `scripts/list_hf_models.py`) to get an authoritative
  answer instead of guessing from the model's page: it live-tests real candidates against this
  account's actual key through the same router endpoint the extraction pipeline uses, and
  reports which ones genuinely work right now. Set `HF_MODEL` in `extract-pages-hf.yml` (and
  `scripts/hf_config.py`'s `DEFAULT_HF_MODEL`) to whichever one it confirms.
