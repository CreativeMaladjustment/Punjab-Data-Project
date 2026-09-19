"""Diagnostic tool: report which vision-language ("image-text-to-text")
models are actually callable through Hugging Face's Inference Providers
router right now, for the account that owns HUGGING_FACE_API_KEY.

Why this exists: extract_with_hf.py's default model (Qwen/Qwen2.5-VL-7B-
Instruct, picked from third-party pricing/benchmark writeups since
huggingface.co itself isn't reachable from wherever this was researched)
turned out to 400 in production with "not supported by any provider you
have enabled." That error has two different possible causes -- the model
genuinely isn't deployed by any provider right now, or it is, but the
account's Inference Providers settings don't have that provider enabled --
and indirect, secondhand sources can't distinguish between them or catch
either kind of drift. This script asks the actual API, with the actual
key that actually runs the pipeline, instead of guessing from writeups
again.

Two checks, in order:

1. Hub API listing (huggingface.co/api/models) for image-text-to-text
   models, filtered to ones with at least one Inference Provider mapping,
   sorted by download count (most-downloaded first -- the API rejects
   "trending" as a sort key), with each model's inferenceProviderMapping
   expanded -- this is catalog metadata: which providers Hugging Face's
   own listing says serve a model. This alone isn't sufficient (see
   above), but it's a reasonable source of candidate model names.
2. A live, minimal chat-completion call through the same
   router.huggingface.co endpoint and the same multimodal content shape
   (an image_url data URI plus text, not a plain text message)
   extract_with_hf.py actually sends -- this is the authoritative check:
   if this succeeds, that (model, this account's key) combination
   genuinely works right now for the same request shape the real
   extraction calls use. A text-only probe isn't enough here -- a model/
   provider can accept a plain-text request while rejecting the
   image-bearing one the pipeline actually sends, which would make this
   script recommend a model that then fails extraction anyway. If it
   fails, the exact error is reported (a 400 "not supported" means step
   1's listing doesn't reflect what's actually usable for this account;
   other errors -- 401/403 in particular -- point at the token itself, or
   at needing to enable a provider at
   huggingface.co/settings/inference-providers). Calls are paced and a
   429/5xx is retried a bounded number of times, same reasoning as
   extract_with_hf.py's own pace()/_hf_post() -- back-to-back calls across
   a batch of candidates could otherwise draw a rate limit that has
   nothing to do with whether a given model actually works.

Run manually via .github/workflows/list-hf-models.yml (workflow_dispatch
only -- this is an on-demand diagnostic, not a pipeline stage, so it
never runs on a schedule and never touches B2 or Postgres). Output goes
to the job's log and step summary; nothing is written anywhere.
"""
import json
import os
import sys
import time

import requests

from hf_config import DEFAULT_HF_MODEL

HUGGING_FACE_API_KEY = os.environ["HUGGING_FACE_API_KEY"]

HF_API_TIMEOUT_SECONDS = 30
HF_ROUTER_URL = "https://router.huggingface.co/v1/chat/completions"
HF_MODELS_API_URL = "https://huggingface.co/api/models"

# How many most-downloaded image-text-to-text models (with at least one
# inference-provider mapping per the Hub's own listing) to pull as
# candidates for the live test below.
CANDIDATE_LIMIT = int(os.environ.get("HF_CANDIDATE_LIMIT", "15"))

# A 1x1 transparent PNG, used only to exercise the same multimodal
# image_url + text content shape extract_with_hf.py's real calls use (see
# hf_generate()/hf_ocr_full_page() there) -- the pixel data itself is
# irrelevant, what matters is the request shape matching, per the module
# docstring's "why a text-only probe isn't enough" note.
PROBE_IMAGE_DATA_URL = (
    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII="
)

# Same pace()/retry reasoning as extract_with_hf.py's own -- see there --
# just with shorter backoff since this is a quick diagnostic scan across
# a batch of candidates, not a long-running extraction run.
PROBE_PACE_SECONDS = float(os.environ.get("HF_PROBE_PACE_SECONDS", "2.0"))
PROBE_MAX_RETRIES = 3
PROBE_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_last_probe_at = [0.0]  # mutable single-element box, same pattern as
# extract_with_hf.py's _LAST_CALL_AT -- this script's calls are all
# sequential (no threads/workers), so a plain module global would need a
# `global` statement at every call site otherwise.


def _probe_pace():
    wait = PROBE_PACE_SECONDS - (time.time() - _last_probe_at[0])
    if wait > 0:
        time.sleep(wait)


def fetch_candidates():
    """Query the Hub API for image-text-to-text models with an inference
    provider mapping, sorted by download count (most-downloaded first).
    Returns a list of (model_id, providers) tuples: providers is whatever
    the Hub's own inferenceProviderMapping says currently serves each
    model (may be empty even for a model returned by this filter -- the
    field's exact shape isn't guaranteed stable, so this degrades to an
    empty list rather than raising if it's missing or shaped differently
    than expected).

    "downloads", not "trending": the Hub API rejects "trending" with a 400
    ("Invalid sort parameter") -- it's a website-only sort the raw API
    doesn't expose the same way. "downloads" is a documented sort key on
    huggingface_hub's ModelInfo and a reasonable proxy for "widely used,
    likely still actively hosted"."""
    headers = {"Authorization": f"Bearer {HUGGING_FACE_API_KEY}"}
    params = {
        "pipeline_tag": "image-text-to-text",
        "inference_provider": "all",
        "sort": "downloads",
        "direction": "-1",
        "limit": str(CANDIDATE_LIMIT),
        "expand[]": "inferenceProviderMapping",
    }
    try:
        resp = requests.get(HF_MODELS_API_URL, headers=headers, params=params, timeout=HF_API_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        # A timeout/connection failure here must not prevent main() from
        # still live-testing the pipeline's configured model below -- this
        # step is catalog metadata only, not required for the
        # authoritative check.
        print(f"WARNING: Hub API model listing request failed: {exc}")
        print("Falling back to no catalog candidates -- the live test below still runs against "
              "the model this pipeline is currently configured for (HF_MODEL, if set).")
        return []
    if not resp.ok:
        print(f"WARNING: Hub API model listing returned {resp.status_code}: {resp.text[:1000]}")
        print("Falling back to no catalog candidates -- the live test below still runs against "
              "the model this pipeline is currently configured for (HF_MODEL, if set).")
        return []

    models = resp.json()
    candidates = []
    for entry in models:
        model_id = entry.get("id") or entry.get("modelId")
        if not model_id:
            continue
        mapping = entry.get("inferenceProviderMapping")
        if isinstance(mapping, dict):
            providers = sorted(mapping.keys())
        elif isinstance(mapping, list):
            providers = sorted({m.get("provider") for m in mapping if isinstance(m, dict) and m.get("provider")})
        else:
            providers = []
        candidates.append((model_id, providers))
    return candidates


def test_model_live(model_id):
    """The authoritative check: a minimal chat completion through the
    exact router endpoint AND the exact multimodal content shape (image_url
    + text) extract_with_hf.py's real calls use -- see the module
    docstring for why a text-only probe isn't enough. Returns (ok, detail)
    -- detail is the response's message content on success, or the error
    text (truncated) on failure. Paced against PROBE_PACE_SECONDS and
    retries a 429/5xx a bounded number of times before reporting it as a
    (possibly false) failure -- see PROBE_MAX_RETRIES above."""
    headers = {
        "Authorization": f"Bearer {HUGGING_FACE_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model_id,
        "max_tokens": 5,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": PROBE_IMAGE_DATA_URL}},
                {"type": "text", "text": "Say OK."},
            ],
        }],
    }

    backoff = 5
    for attempt in range(1, PROBE_MAX_RETRIES + 1):
        _probe_pace()
        try:
            resp = requests.post(HF_ROUTER_URL, json=body, headers=headers, timeout=HF_API_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            return False, f"request failed: {exc}"
        _last_probe_at[0] = time.time()
        if resp.status_code in PROBE_RETRYABLE_STATUS_CODES:
            if attempt < PROBE_MAX_RETRIES:
                print(f"    {resp.status_code} from HF router for {model_id} "
                      f"(attempt {attempt}/{PROBE_MAX_RETRIES}); retrying in {backoff}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            return False, (
                f"{resp.status_code} after {PROBE_MAX_RETRIES} attempts -- transient, "
                f"inconclusive rather than a confirmed non-working model: {resp.text[:300]}"
            )
        break

    if not resp.ok:
        return False, f"{resp.status_code}: {resp.text[:300]}"

    try:
        content = resp.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        return False, f"200 but unexpected response shape ({exc}): {resp.text[:300]}"

    return True, content


def main():
    print("Step 1: querying Hugging Face's model catalog for the most-downloaded "
          f"image-text-to-text models with an inference-provider mapping (limit {CANDIDATE_LIMIT})...")
    candidates = fetch_candidates()

    # Defaults to hf_config.DEFAULT_HF_MODEL -- the same single source of
    # truth extract_with_hf.py's own HF_MODEL default reads from -- rather
    # than requiring the workflow YAML to pass its own copy of that
    # literal (a Copilot review finding on PR #57: a hardcoded value here
    # could silently drift from extract-pages-hf.yml's real default the
    # next time it changes). An explicit HF_MODEL env var still overrides,
    # same as extract_with_hf.py's own.
    configured_model = os.environ.get("HF_MODEL", DEFAULT_HF_MODEL).strip()
    seen = {model_id for model_id, _ in candidates}
    if configured_model and configured_model not in seen:
        # Always test the pipeline's actual currently-configured model
        # too, even if the catalog query above didn't happen to surface
        # it (e.g. it's not among the most-downloaded, or the listing call failed).
        candidates.append((configured_model, []))

    if not candidates:
        print("No candidates found from the catalog and no configured model to fall back to; nothing to test.")
        sys.exit(1)

    print(f"\nFound {len(candidates)} candidate(s). Catalog-reported providers (informational only -- "
          "see the module docstring for why this alone isn't reliable):")
    for model_id, providers in candidates:
        providers_str = ", ".join(providers) if providers else "(none listed)"
        marker = "  <- currently configured HF_MODEL" if model_id == configured_model else ""
        print(f"  {model_id}: {providers_str}{marker}")

    print(f"\nStep 2: live-testing each candidate against {HF_ROUTER_URL} "
          "(the same endpoint extract_with_hf.py uses)...")
    results = []
    for model_id, providers in candidates:
        ok, detail = test_model_live(model_id)
        status = "OK" if ok else "FAILED"
        print(f"  {model_id}: {status} -- {detail!r}")
        results.append((model_id, ok, detail))

    working = [model_id for model_id, ok, _ in results if ok]
    print(f"\n{len(working)}/{len(results)} candidate(s) actually work with this account's key right now.")
    if working:
        print("Working model(s), usable as HF_MODEL in extract-pages-hf.yml:")
        for model_id in working:
            print(f"  - {model_id}")
    else:
        print(
            "None of the tested candidates work. If every candidate failed with the same "
            "\"not supported by any provider you have enabled\" message, check "
            "https://huggingface.co/settings/inference-providers and enable at least one "
            "provider for this account -- that's an account-level setting this script "
            "can't check or change on your behalf."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
