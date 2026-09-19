"""Gradio app for the Hugging Face Space at floutenvy/Punjab-Data-Project,
run on HF's free ZeroGPU hardware (dynamic RTX Pro 6000 Blackwell
allocation, up to 96GB VRAM). Extracts catalogue entries from a scanned
page image of a British colonial-era "Catalogue of Books registered"
print register, running Qwen2.5-VL-7B-Instruct locally on the Space's own
GPU -- no per-token billing, no third-party Inference Providers routing,
just this Space's own free ZeroGPU quota.

This file lives at hf_space/app.py in the main GitHub repo, not directly
in the HF Space's own git repo -- a GitHub Actions workflow
(.github/workflows/sync-hf-space.yml) mirrors this folder's contents
(plus a copy of pipeline/schema.md) into the Space's separate git repo on
every push that touches either. Don't edit the Space's repo directly;
changes made there will be overwritten by the next sync.

Why a local model here instead of routing through HF's own Inference
Providers (like scripts/extract_with_hf.py does): ZeroGPU's daily quota is
measured in actual GPU-seconds (a free account gets ~5 minutes/day,
resetting 24h after first use), not a dollar credit -- a completely
different, and for a 7B model doing a handful of pages, likely more
generous, constraint shape than Inference Providers' $0.10/month. See
HF_EXTRACTION.md in the main repo for that pipeline's own tradeoffs.

Deliberately NOT running a second full-page-OCR call per image the way
extract_with_gemini.py/extract_with_hf.py do -- given how small the daily
GPU-second budget is, doubling the calls per page would roughly halve how
many pages fit in it, and there's no established real-world timing data
yet for how many seconds one call actually costs. Revisit once that's
measured.

Model loading and the `.to("cuda")` call happen at module level (Space
startup), NOT inside the @spaces.GPU-decorated function -- this matches
the pattern HF's own ZeroGPU example Spaces use; the `spaces` package
handles making this safe to do before any GPU is actually attached to the
process. Only the actual forward pass (extract() below) is decorated, so
only its GPU time counts against the daily quota, not model loading.

UNVERIFIED as of this writing: this hasn't been run against a live
ZeroGPU allocation yet, so the exact per-call GPU-seconds cost, and
whether the transformers/qwen_vl_utils API calls below are exactly
correct for the installed library versions, are both things to confirm
once the Space actually builds and a real call is made through it (e.g.
via the Space's own Gradio UI, or gradio_client).
"""
import json
import pathlib
import re

import gradio as gr
import spaces
import torch
from PIL import Image
from qwen_vl_utils import process_vision_info
from transformers import AutoProcessor, Qwen2_5_VLForConditionalGeneration

MODEL_ID = "Qwen/Qwen2.5-VL-7B-Instruct"

processor = AutoProcessor.from_pretrained(MODEL_ID)
model = Qwen2_5_VLForConditionalGeneration.from_pretrained(MODEL_ID, torch_dtype=torch.bfloat16)
model.to("cuda")
model.eval()

# Copied here (from pipeline/schema.md in the main repo) by
# sync-hf-space.yml on every sync -- not committed by hand, so it can
# never silently drift from the schema the rest of the pipeline actually
# uses. Read at import time as every other extraction script's system
# prompt already does.
SCHEMA_PATH = pathlib.Path(__file__).resolve().parent / "schema.md"

SYSTEM_PROMPT = """You transcribe entries from a scanned page of a British \
colonial-era "Catalogue of Books registered" print register. Output ONLY a \
JSON array of entry objects following the schema below. Transcribe verbatim; \
never correct or invent. Flag every uncertain reading in `flags`. If the page \
has no catalog entries (cover, blank, title page, index), output [].

Read the printed page number directly off the page image if one is visible \
(printed in a margin, header, or footer) and put it in `printed_page` as an \
integer. If none is visible, use 0 and add a `flags` entry noting the printed \
page number wasn't visible. Leave `quarter` as "" — it isn't known for this \
source. Output the JSON array only, no commentary, no markdown code fences.

SCHEMA:
""" + SCHEMA_PATH.read_text(encoding="utf-8")

# Deliberate duplicate of extract_with_llm.py's/extract_with_gemini.py's/
# extract_with_hf.py's ENTRY_FIELD_NAMES/_looks_like_entry_list/
# _coerce_to_entry_list -- see those modules for the full reasoning behind
# each recovered shape. Kept in sync by hand; this file lives in a
# separate git repo (the HF Space) from the rest of the pipeline, so it
# can't import them directly even if the project's usual
# not-importing-across-extraction-scripts convention allowed it.
ENTRY_FIELD_NAMES = {
    "quarter", "pdf_page", "printed_page", "section", "lang", "char", "topic",
    "serial", "reg", "copies", "printer_verbatim", "printer", "pcity", "author",
    "title", "title_native", "gloss", "pp_verbatim", "publisher", "pubcity",
    "date", "price", "edition", "format", "method", "educ", "copyright",
    "notes", "marks", "flags",
}


def _looks_like_entry_list(value):
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(e, dict) and (e.keys() & ENTRY_FIELD_NAMES) for e in value)
    )


def _coerce_to_entry_list(parsed):
    if isinstance(parsed, list):
        result = parsed
    elif not isinstance(parsed, dict):
        raise ValueError(f"expected a JSON array, got {type(parsed).__name__}")
    else:
        list_items = [(k, v) for k, v in parsed.items() if isinstance(v, list)]
        has_dict_value = any(isinstance(v, dict) for v in parsed.values())

        is_entries_envelope = False
        if len(list_items) == 1 and not has_dict_value:
            sole_key, sole_value = list_items[0]
            if len(sole_value) == 0:
                is_entries_envelope = sole_key not in ENTRY_FIELD_NAMES
            else:
                is_entries_envelope = _looks_like_entry_list(sole_value)

        if is_entries_envelope:
            list_key, entries = list_items[0]
            backfill = {k: v for k, v in parsed.items() if k != list_key and k in ENTRY_FIELD_NAMES}
            for entry in entries:
                for k, v in backfill.items():
                    entry.setdefault(k, v)
            result = entries
        else:
            known_keys = parsed.keys() & ENTRY_FIELD_NAMES
            if known_keys and not has_dict_value:
                result = [parsed]
            elif not known_keys and not has_dict_value:
                if len(list_items) != 1:
                    raise ValueError(f"expected a JSON array, got dict with keys {sorted(parsed.keys())}")
                result = list_items[0][1]
            else:
                raise ValueError(f"expected a JSON array, got dict with keys {sorted(parsed.keys())}")

    bad_types = sorted({type(e).__name__ for e in result if not isinstance(e, dict)})
    if bad_types:
        raise ValueError(f"expected a list of entry objects, got element type(s) {bad_types}")
    return result


MAX_NEW_TOKENS = 2048


@spaces.GPU(duration=60)
def extract(image):
    """The only GPU-metered call in this app -- everything else (model
    load, image decode) runs on CPU. Returns a dict: {"entries": [...],
    "raw_text": "..."} on success, or {"error": "...", "raw_text": "..."}
    if the model's output couldn't be parsed into the expected shape
    (still returns the raw text so a caller can see what actually came
    back, same as the other extraction scripts' content_failure path)."""
    if image is None:
        return {"error": "no image provided"}
    if not isinstance(image, Image.Image):
        image = Image.fromarray(image)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": [
            {"type": "image", "image": image},
            {"type": "text", "text": "Output the JSON array for this page only."},
        ]},
    ]
    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    image_inputs, video_inputs = process_vision_info(messages)
    inputs = processor(
        text=[text], images=image_inputs, videos=video_inputs,
        padding=True, return_tensors="pt",
    ).to("cuda")

    generated_ids = model.generate(**inputs, max_new_tokens=MAX_NEW_TOKENS)
    generated_ids_trimmed = [
        out_ids[len(in_ids):] for in_ids, out_ids in zip(inputs.input_ids, generated_ids)
    ]
    raw_text = processor.batch_decode(
        generated_ids_trimmed, skip_special_tokens=True, clean_up_tokenization_spaces=False,
    )[0]

    text_clean = raw_text.strip()
    text_clean = re.sub(r"^```(json)?|```$", "", text_clean, flags=re.M).strip()
    try:
        parsed = json.loads(text_clean)
        entries = _coerce_to_entry_list(parsed)
    except Exception as exc:
        return {"error": str(exc), "raw_text": raw_text}

    return {"entries": entries, "raw_text": raw_text}


demo = gr.Interface(
    fn=extract,
    inputs=gr.Image(type="pil", label="Page image"),
    outputs=gr.JSON(label="Extraction result"),
    title="Punjab Data Project — catalogue extraction",
    description=(
        "Internal extraction endpoint for the Punjab Data Project pipeline. "
        "Takes one page image, returns structured catalogue entries following "
        "pipeline/schema.md. Called both from this UI and headlessly via "
        "gradio_client from a GitHub Actions workflow."
    ),
)

if __name__ == "__main__":
    demo.launch()
