# Architecture Decision Record — Cloud-Native, CI/CD-as-Compute Pipeline

**Status:** Accepted, in production use.
**Date:** 2026-09-10 (written up retroactively; the underlying decisions were made incrementally
from project start through PRs #13–#17).
**Related:** `DECISIONS.md` D-021 (short cross-reference entry). This document covers
infrastructure/engineering decisions; `DECISIONS.md` covers data and research-methodology
decisions. Different questions, different audiences — kept separate rather than merged.

---

## Context

The project needs to move ~tens of thousands of scanned register pages (bound PDF volumes,
1867–1942) through three stages — render each page to an image, store it durably, and run a
vision-LLM extraction pass over it into a structured schema (`pipeline/schema.md`) — at a
scale and cadence that will run for months as more volumes are added, with **no dedicated
server, no ops team, and no infrastructure budget**. This is an independent research project,
not a funded lab with cloud credits.

That constraint is the actual design driver. Every choice below follows from "what can this
project run entirely on free or near-free managed services, coordinated by nothing more than
what's already checked into the repo."

## Decision

Build the whole pipeline on **GitHub Actions as the compute layer**, coordinating a handful of
**free/cheap-tier managed cloud services** for everything stateful, with **Postgres as the only
shared coordination point** between otherwise-stateless, ephemeral jobs.

### Components

| Concern | Service | Why |
|---|---|---|
| Source volumes | pCloud (public share link) | Already where the scans lived; no migration needed; free tier serves public downloads. |
| Compute | GitHub Actions (`ubuntu-latest` runners) | Free minutes on a public repo; ephemeral — nothing to patch, nothing idling between runs; `strategy.matrix` gives horizontal parallelism for free. |
| Object storage | Backblaze B2 (two accounts, round-robin assigned) | Cheapest S3-compatible storage available; two accounts split load and give a fallback path (`upload_with_fallback` in `process_pcloud.py`) if one account errors. |
| Database | Supabase-hosted Postgres | Free-tier managed Postgres; single source of truth for upload state and extraction results (`pages.image_uploaded_at`, `llm_extractions.status`) — see `supabase/migrations/`. PDF-stage render/upload failures aren't persisted as a status anywhere; a failed page just stays absent from `pages`, with the failure itself only visible in that run's Actions log. |
| LLM inference | Ollama, self-hosted **on the runner itself** | GitHub-hosted runners have no GPU, so this is CPU inference — slow per page, but it costs nothing beyond runner-minutes. No API key, no per-token billing, no external vendor for the actual OCR/extraction work. |
| Orchestration | None (deliberately) | No Celery, no SQS, no Redis, no K8s. Coordination between parallel jobs is a handful of SQL statements against Postgres (see below), not a service. |

### The pipeline *is* the CI/CD system, not a thing CI/CD deploys

`process-pdfs.yml` and `extract-pages.yml` are `workflow_dispatch`-triggered — manually run,
not push-triggered — because they **are** the production data pipeline, invoked as needed,
not a test suite that gates a deploy. There is no separate "deploy" step: the workflow YAML
in the repo *is* the infrastructure, and a `git push` to it changes production behavior on the
next run. This is deliberate: it means the entire compute and orchestration definition is
version-controlled, reviewable, and reproducible by anyone who forks the repo and supplies
their own credentials — there is no server whose state can drift from what's in git.

### Parallelism without a queue service

Both workflows fan work out across GitHub Actions matrix jobs, but the two use different
coordination strategies depending on the shape of the work:

- **`extract-pages.yml`**: a fixed matrix of 9 workers, each running an independent claim
  loop (`claim_next_page()` in `scripts/extract_with_llm.py`) against `llm_extractions`.
  The claim query uses `FOR UPDATE ... SKIP LOCKED` plus a conditional
  `ON CONFLICT DO UPDATE ... WHERE ...` so that two workers racing for the same page can
  never both believe they claimed it — verified under genuine concurrent load (16 real
  threads, separate connections, forced-overlap stress test) during development. A worker
  that dies mid-page leaves a stale claim that any worker (including itself, next run)
  reclaims automatically once `CLAIM_TIMEOUT_SECONDS` (3h) has passed — no separate cleanup
  job, no dead-letter queue, just a `WHERE claimed_at < now() - interval` in the same query.
- **`process-pdfs.yml`**: a `list-remaining` job computes the full backlog once, up front,
  and chunks it round-robin into at most `MAX_PARALLEL_PDF_WORKERS` (10) slices — a static
  partition, not a live claim loop, because the unit of work (one PDF) is naturally
  divisible in advance and doesn't need runtime contention-resolution *within that run*.
  This also sidesteps a real GitHub Actions platform limit (a job's matrix is capped at
  256 combinations) that a naive one-matrix-entry-per-PDF design would eventually have hit
  as the backlog grew. **This safety is scoped to a single run**, not to arbitrary
  concurrency: the workflow has no `concurrency:` group and there is no atomic PDF-level
  claim, so two manually-dispatched runs overlapping in time could both snapshot the same
  not-yet-uploaded PDFs before either writes a page row, and duplicate the download/upload
  work for the overlap window. `process_pdf()`'s per-page upsert keeps the final Postgres
  row coherent (`ON CONFLICT DO UPDATE` means whichever write lands last wins cleanly), but
  it does **not** make the B2 side idempotent: `upload_with_fallback()` tries the assigned
  account first, then falls back to any other configured account on error, so two racing
  runs can genuinely succeed on *different* accounts for the same page — leaving one
  physical object in B2 that no `pages` row ever points to, an orphan invisible to anything
  that only audits Postgres. Wasteful and untracked, not corrupting — but a real gap this
  design accepts rather than closes. `extract-pages.yml`'s live claim, by contrast,
  is safe under *arbitrary* concurrency, including two separate workflow runs racing each
  other, because the guarantee lives in the database transaction itself rather than in
  a one-time, run-scoped snapshot.

Postgres is doing the job a message queue would normally do, at zero additional
infrastructure cost, because the coordination need (five-ish SQL predicates) doesn't justify
running a queue service.

## Alternatives considered

- **A dedicated VM or server.** Rejected: fixed cost whether or not it's doing anything, and
  this workload is bursty (manually triggered, runs for hours, then idle for days) — paying
  for 24/7 uptime for an intermittent job is the wrong shape. Also means someone has to patch
  and monitor it; nobody is staffed to do that here.
- **A paid hosted LLM API** (rather than local Ollama on the runner) for extraction. Rejected
  on cost: tens of thousands of vision-LLM calls at API pricing is real money for a
  no-budget project, versus free CPU-minutes already available from GitHub Actions. The
  tradeoff is speed (CPU inference is slow) and model quality (only small, non-gated models
  fit), which the project accepted.
- **A real task queue** (Celery/Redis, SQS, or similar) for coordinating parallel workers.
  Rejected as disproportionate: the actual coordination need is "don't let two workers grab
  the same row," which a `SELECT ... FOR UPDATE SKIP LOCKED` already solves without adding
  a service to run, monitor, and pay for.
- **Kubernetes / autoscaling compute.** Never seriously considered — wildly disproportionate
  to the scale (a few thousand jobs total, not a continuously-running service) and adds
  exactly the operational burden this whole approach exists to avoid.

## Consequences

### What this buys

- **Near-zero marginal infrastructure cost**, bounded by free-tier limits, for a project with
  no budget line for infrastructure.
- **Nothing to patch or monitor as a running service** — every compute unit is an ephemeral
  GitHub Actions job that exists for the duration of one run.
- **Infrastructure-as-code by construction**: the workflow YAML and the claiming SQL *are*
  the orchestration layer, checked into the same repo as the application code, reviewed the
  same way, with the same history.
- **Reproducible and forkable**: anyone with their own pCloud link, B2 buckets, and a Supabase
  project can run the identical pipeline from a clean checkout.
- **Demonstrated horizontal scaling** at the scale this project needs it: 9 parallel
  extraction workers, up to 10 parallel PDF-processing workers. The extraction claim loop
  is verified to prevent double-processing under *arbitrary* concurrency (including across
  separate runs); the PDF-processing chunking prevents double-processing *within a single
  run* only — see the accepted cross-run duplication gap above.

### What it costs

- **Free-tier caps become the actual operational bottleneck**, and they are *external* limits
  the pipeline cannot negotiate around by writing better code. This is not hypothetical: on
  2026-09-10, three consecutive `extract-pages.yml` runs, spanning roughly 05:00–16:12 UTC
  (~11 hours), produced, out of 22,398 `llm_extractions` rows, 22,387 `failed`, 6 `claimed`
  (each either an active claim or one gone stale and not yet reclaimed — a single snapshot
  query can't distinguish the two), and 5 `success` — a **99.95% failure rate** — because
  both configured B2
  accounts hit the exact error `AccessDenied: ... download bandwidth or transaction
  (Class B) cap exceeded` and it had not recovered across the entire window. That message
  names two distinct B2 quotas (a download-bandwidth allowance and a separate "Class B"
  transaction-count cap) without saying which one actually tripped; telling them apart, and
  therefore the correct remedy, needs a direct look at the Backblaze account's Caps & Alerts
  dashboard, not just the error string. The claiming and retry logic behaved exactly as
  designed throughout (no double-claims, no hot-looping, clean backoff) — the bottleneck was
  entirely the external quota, and no amount of application-level fixing moves that number
  until the actual cap in question is raised or resets.
- **Runner limits shape the code, not just the ops — unevenly.** Both workflows set
  `timeout-minutes: 350` (a repo choice, kept under GitHub Actions' own platform ceiling for
  a job — not itself a platform-inherent number, so it'll drift if that setting ever
  changes). That configured limit is why `extract-pages.yml`'s per-worker claim loop checks
  `MAX_RUNTIME_SECONDS` every iteration and exits with code 42 ("resume needed, not a
  failure") before the timeout would otherwise kill it mid-run. `process_pcloud.py` has the
  equivalent guard too, but currently only in
  its legacy full-scan mode — the `PCLOUD_PDFS_JSON` slice loop that `process-pdfs.yml`'s
  matrix jobs actually run has no internal runtime check, so a slice with an unlucky mix of
  large PDFs can run until GitHub's hard timeout kills it mid-item, with no graceful
  "resume" signal. Worth closing, not yet done.
- **No persistent compute state.** Every matrix job starts from nothing; any
  *application/source-of-truth* state that needs to survive between jobs or runs — what's
  been uploaded, what's been extracted — must be written to Postgres or B2 explicitly. This
  is why the whole design centers on Postgres as source of truth for what's done; it's not
  optional, it's the only place that state can live. (Incidental build/dependency caches are
  a separate thing and do survive elsewhere on purpose — `extract-pages.yml` caches the
  pulled Ollama model under `~/.ollama` via `actions/cache`, and `setup-python` caches pip
  packages — but losing either just costs a slower re-download next run, not correctness.)
- **CPU-only LLM inference is slow.** No GPU on standard GitHub-hosted runners bounds
  per-page throughput regardless of parallelism; this was an accepted tradeoff against the
  cost of GPU compute or a paid API.
- **Vendor sprawl as an operational surface.** Four external providers (GitHub, Supabase,
  Backblaze, pCloud) plus Ollama's model registry, each with independent auth, quotas, and
  failure modes. Diagnosing a stall means checking across all of them — the B2 incident above
  took correlating GitHub Actions job logs with a direct Postgres query to pin down, because
  neither GitHub's UI nor a cursory log read said "B2 quota" on its own; the actual root cause
  was one specific error string repeated in the logs and only obvious once counted.
- **No dedicated monitoring or alerting.** There is no dashboard; the state of a run is
  whatever's in the GitHub Actions log or whatever a hand-written SQL query against
  `llm_extractions`/`pages` says at the moment someone looks. This is adequate at current
  scale and would need revisiting if the pipeline needed to run unattended for long stretches.

## When to revisit

- If the project ever has a budget line, whichever B2 quota is actually behind the 2026-09-10
  incident (download bandwidth or Class B transaction count — confirm via Caps & Alerts
  before assuming) is the most likely forcing function to reconsider paid storage tiers or a
  different provider; the two have different remedies, so pin down which one it is before
  paying to fix it.
- If GPU-accelerated inference becomes affordable or necessary for extraction quality, the
  "Ollama on a CPU runner" choice should be revisited — it was a cost decision, not a belief
  that CPU inference is the right long-term answer.
- If the pipeline needs to run unattended for long stretches without a person watching Actions
  logs, the "no monitoring" gap becomes the priority, not compute or storage choice.
