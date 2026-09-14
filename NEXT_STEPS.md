# Next steps — additional free-tier compute accounts

*A personal to-do list, not a decision record (see [DECISIONS.md](DECISIONS.md) for
those) or a status report (see [ARCHITECTURE.md](ARCHITECTURE.md)). This tracks
signing up for more free-tier inference providers so extraction throughput isn't
capped by any single quota.*

## Why

[D-021](DECISIONS.md) picked "self-hosted Ollama on GitHub Actions runners" as the
entire compute layer specifically to stay at zero infra cost — and the same entry
records the tradeoff biting immediately: a single provider's free quota (B2's, that
time) became the real ceiling on throughput, not the pipeline logic. `extract-pages.yml`
already fans out across up to 18 GitHub Actions workers, but they all still funnel
through the same Ollama-on-a-CPU-runner path — no amount of matrix width buys past
that. Accounts on a handful of *other* free-tier inference providers, run alongside
Ollama rather than instead of it, spread the load across several independent quotas
that reset on different clocks, and some of them are real hosted GPU inference (not
CPU-only), so are worth having even at small scale.

The [rescue-mode workflow](.github/workflows/extract-pages.yml) already gives a
second model a narrow, well-defined job (glm-ocr's capped content failures) — a
natural first task to point a new provider at once one is wired up, before trusting it
with the main backlog.

## Providers to add

| Provider | Reset cycle | Free allowance |
|---|---|---|
| Cloudflare Workers AI | Daily | 100,000 Neurons/day (~thousands of light vision tokens), resets 00:00 UTC daily. |
| Groq | Daily / rolling | 30 RPM + a daily token/request budget (~1,000–7,000 requests/day, model-dependent), resets every 24h. |
| Hugging Face Serverless | Rolling | Rate-limited on a rolling compute-pool window (~1,000 requests/day). |
| Google AI Studio (Gemini Flash) | Daily / per-minute | 15 RPM + a perpetual 1,500 requests/day quota, resets daily, no end date. |
| OpenRouter (`:free` endpoints) | Rolling | Per-minute/rolling rate limits, no fixed lifetime expiration. |

## Per-provider checklist

- [ ] **Cloudflare Workers AI** — sign up, create an API token, confirm which of its
      hosted models are actually vision-capable (not all are) before counting on it
      for page-image extraction.
- [ ] **Groq** — sign up, get an API key, confirm a vision-capable model is on the
      free tier (their catalog skews text-only; check before relying on it here).
- [ ] **Hugging Face Serverless Inference** — create a token, confirm a vision model
      is served on the free rolling pool (availability varies by model).
- [ ] **Google AI Studio** — get a Gemini API key; Gemini Flash is natively
      multimodal, so this is the most likely of the five to be immediately useful.
- [ ] **OpenRouter** — sign up, get an API key, pick a `:free`-suffixed vision model
      from their list (verify it stays free and vision-capable — their free lineup
      changes over time).

## Once keys exist

- [ ] Store each as a repo secret (`environment: b2-upload`, matching the pattern
      `B2_KEY_ID` etc. already use) — one secret per provider, not shared.
- [ ] `extract_with_llm.py` currently only speaks Ollama's `/api/generate` request/
      response shape (see `extract_page()`/`ocr_full_page()`). Each of these is a
      genuinely different HTTP API (OpenAI-compatible chat completions for some,
      provider-specific for others) — this needs a small adapter layer (one function
      per provider producing the same `(entries, raw_text)` shape `extract_page()`
      returns today) before any of them can actually run pages, not just a new env
      var. Model-tag namespacing (`MODEL_TAG` slugification, `llm_extractions`
      unique on `(page_id, model_tag)`) already generalizes to this with no schema
      change — only the request/response layer is Ollama-specific.
- [ ] Confirm each provider's free tier permits this use case in its ToS (some
      free/eval tiers restrict commercial or high-volume batch use).
- [ ] Start with rescue mode (see `SOURCE_MODEL` in `extract-pages.yml`) pointed at
      glm-ocr's capped content failures as the first real test of a new provider,
      before trusting it with the full backlog.
