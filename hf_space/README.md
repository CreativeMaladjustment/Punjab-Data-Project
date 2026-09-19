---
title: Punjab Data Project Extraction
emoji: 📚
colorFrom: blue
colorTo: indigo
sdk: gradio
app_file: app.py
pinned: false
---

Internal catalogue-extraction endpoint for the [Punjab Data
Project](https://github.com/CreativeMaladjustment/Punjab-Data-Project) — a
computational sociological analysis of British colonial Punjab print registers,
1867–1942.

Takes one scanned page image, runs it through `Qwen/Qwen2.5-VL-7B-Instruct` on
this Space's free ZeroGPU hardware, and returns structured catalogue entries
following the project's `pipeline/schema.md`. Not a public demo — this exists to
be called from the project's own GitHub Actions pipeline (via `gradio_client`),
as another peer alongside its local-Ollama, Gemini, and Hugging Face Inference
Providers extraction passes.

This Space's code is mirrored from `hf_space/` in the main GitHub repo by
`.github/workflows/sync-hf-space.yml` on every relevant push — don't edit files
here directly, they'll be overwritten by the next sync. See `HF_EXTRACTION.md`
in the main repo (and this app's own module docstring) for the full reasoning.
