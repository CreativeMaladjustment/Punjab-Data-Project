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
   sorted by trending, with each model's inferenceProviderMapping
   expanded -- this is catalog metadata: which providers Hugging Face's
   own listing says serve a model. This alone isn't sufficient (see
   above), but it's a reasonable source of candidate model names.
2. A live, minimal (text-only, no image, small max_tokens) chat-
   completion call through the same router.huggingface.co endpoint
   extract_with_hf.py actually uses, for each candidate from step 1 --
   this is the authoritative check: if this succeeds, that (model, this
   account's key) combination genuinely works right now. If it fails, the
   exact error is reported (a 400 "not supported" means step 1's listing
   doesn't reflect what's actually usable for this account; other errors
   -- 401/403 in particular -- point at the token itself, or at needing
   to enable a provider at huggingface.co/settings/inference-providers).

Run manually via .github/workflows/list-hf-models.yml (workflow_dispatch
only -- this is an on-demand diagnostic, not a pipeline stage, so it
never runs on a schedule and never touches B2 or Postgres). Output goes
to the job's log and step summary; nothing is written anywhere.
"""
import json
import os
import sys

import requests

HUGGING_FACE_API_KEY = os.environ["HUGGING_FACE_API_KEY"]

HF_API_TIMEOUT_SECONDS = 30
HF_ROUTER_URL = "https://router.huggingface.co/v1/chat/completions"
HF_MODELS_API_URL = "https://huggingface.co/api/models"

# How many trending image-text-to-text models (with at least one
# inference-provider mapping per the Hub's own listing) to pull as
# candidates for the live test below.
CANDIDATE_LIMIT = int(os.environ.get("HF_CANDIDATE_LIMIT", "15"))


def fetch_candidates():
    """Query the Hub API for image-text-to-text models with an inference
    provider mapping, sorted by trending. Returns a list of
    (model_id, providers) tuples: providers is whatever the Hub's own
    inferenceProviderMapping says currently serves each model (may be
    empty even for a model returned by this filter -- the field's exact
    shape isn't guaranteed stable, so this degrades to an empty list
    rather than raising if it's missing or shaped differently than
    expected)."""
    headers = {"Authorization": f"Bearer {HUGGING_FACE_API_KEY}"}
    params = {
        "pipeline_tag": "image-text-to-text",
        "inference_provider": "all",
        "sort": "trending",
        "limit": str(CANDIDATE_LIMIT),
        "expand[]": "inferenceProviderMapping",
    }
    resp = requests.get(HF_MODELS_API_URL, headers=headers, params=params, timeout=HF_API_TIMEOUT_SECONDS)
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
    """The authoritative check: a minimal, text-only, no-image chat
    completion through the exact router endpoint extract_with_hf.py uses.
    Returns (ok, detail) -- detail is the response's message content on
    success, or the error text (truncated) on failure. This only confirms
    the router accepts (model_id, this account's key) for a basic text
    call; it doesn't confirm vision support specifically -- a model
    passing this check still needs to actually be a vision-capable model
    (which the pipeline_tag filter above already selected for)."""
    headers = {
        "Authorization": f"Bearer {HUGGING_FACE_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model_id,
        "max_tokens": 5,
        "messages": [{"role": "user", "content": "Say OK."}],
    }
    try:
        resp = requests.post(HF_ROUTER_URL, json=body, headers=headers, timeout=HF_API_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        return False, f"request failed: {exc}"

    if not resp.ok:
        return False, f"{resp.status_code}: {resp.text[:300]}"

    try:
        content = resp.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        return False, f"200 but unexpected response shape ({exc}): {resp.text[:300]}"

    return True, content


def main():
    print("Step 1: querying Hugging Face's model catalog for trending "
          f"image-text-to-text models with an inference-provider mapping (limit {CANDIDATE_LIMIT})...")
    candidates = fetch_candidates()

    configured_model = os.environ.get("HF_MODEL", "").strip()
    seen = {model_id for model_id, _ in candidates}
    if configured_model and configured_model not in seen:
        # Always test the pipeline's actual currently-configured model
        # too, even if the catalog query above didn't happen to surface
        # it (e.g. it's not "trending", or the listing call failed).
        candidates.append((configured_model, []))

    if not candidates:
        print("No candidates found from the catalog and no HF_MODEL configured to fall back to; nothing to test.")
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
