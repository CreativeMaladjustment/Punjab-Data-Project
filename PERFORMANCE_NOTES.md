# Extraction performance notes

Point-in-time measurements of `extract_with_llm.py`'s per-page throughput
across the two runner types it currently supports (`extract-pages.yml`'s
GitHub-hosted CPU matrix, and `extract-pages-local.yml`'s self-hosted
runner). Kept as a reference for future capacity/cost decisions — not a
guarantee either number holds after model, workflow, or hardware changes.

## 2026-09-12: GitHub-hosted CPU vs. self-hosted Apple Silicon (M5)

**Setup**: both runs used `glm-ocr` via Ollama, no code differences beyond
the runner itself (GitHub-hosted `ubuntu-latest`, 4 vCPU, CPU-only
inference, vs. a MacBook Air M5 / 24GB RAM self-hosted runner, Metal-
accelerated inference through Ollama).

**Sources**:
- GitHub-hosted: [run 34671103351](https://github.com/CreativeMaladjustment/Punjab-Data-Project/actions/runs/34671103351), worker 6 — 38 pages processed in one ~5-hour session (stopped by `MAX_RUNTIME_SECONDS`, not exhaustion).
- Self-hosted (Mac): [run 34692134487](https://github.com/CreativeMaladjustment/Punjab-Data-Project/actions/runs/34692134487) — 135 pages processed over ~1h47m before the run was cancelled (not a natural stop; still a large enough sample for a rate estimate). 82 of those 135 pages had their per-page timing captured in the retrieved log window.

### Per-page latency

| | GitHub-hosted (CPU) | Self-hosted (M5, Metal) |
|---|---|---|
| Mean | 481.5s (~8.0 min) | 49.4s (~50s) |
| Median | ~350s (individual samples ranged 318–800s) | 32s |
| Range observed | 318–800s | 17–502s (two outliers: 233s, 502s — likely unusually dense pages or transient contention on the laptop) |

**The Mac ran roughly 10–15x faster per page** than a GitHub-hosted CPU
runner on this model. Larger than a naive "GPU vs. 4 CPU cores" guess would
suggest — worth re-checking if a different model or page mix is used.

### Aggregate throughput (at each side's worker count on this date)

| | Per-worker | Worker count | Aggregate |
|---|---|---|---|
| GitHub-hosted | ~7.5 pages/hour | 18 (`extract-pages.yml`'s matrix as of PR #25) | ~135 pages/hour |
| Self-hosted (Mac) | ~75.6 pages/hour | 1 | ~75.6 pages/hour |

At these worker counts, the 18-runner GitHub-hosted fleet still edges out
one Mac in raw aggregate throughput (~1.8x). But since one Mac does
roughly the work of 10 CPU runners, **two machines like this one would
already beat the entire 18-runner fleet** — with none of the 5-hour
resume cycles, no queueing against GitHub's 20-concurrent-job account
cap (see `ARCHITECTURE.md`), and no GitHub Actions minutes spent.

### Caveats

- Both samples are from a single run each, not averaged across multiple
  runs — page-to-page variance (dense multi-column tables vs. sparse
  pages) is significant on both sides.
- The self-hosted run was cancelled mid-run, not exhausted; its rate is a
  reasonable estimate but not a full-session average.
- Neither number accounts for time spent on setup steps (Ollama
  install/cache, model pull, Python setup) — this is purely the
  `extract_page()` inference cost, which dominates total runtime either
  way.
