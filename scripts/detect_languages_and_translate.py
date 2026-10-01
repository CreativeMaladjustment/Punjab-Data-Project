"""Detect languages in full-page OCR text and translate non-English sections to English.

Uses the full-page OCR transcriptions (from page_ocr_text table) produced by
extract_with_llm.py's ocr_full_page() to:
  1. Identify all languages present on each page
  2. For each non-English language detected, translate the relevant sections to English

Results are stored in two tables:
  - page_detected_languages: one row per unique language per page
  - page_translations: original text + English translation for non-English content

Alternates between Gemini 3.5 Flash Lite and Gemini 3.1 Flash Lite for speed and
efficiency -- language identification and translation are straightforward tasks
that don't need larger models. Alternating models lets us work within free-tier
quotas by distributing load across two model buckets (each has 15 RPM, 250K TPM).

Processes pages with successful OCR text that haven't been language-analyzed yet.
Paces requests at 2s apart (start-to-start) to stay under rate limits with margin.
Exits gracefully at 60 minutes to respect runner time limits; next hourly run continues.
"""
import json
import os
import re
import sys
import time

import psycopg2
import requests

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Alternate between two models to distribute quota load
# Gemini 3.5 Flash Lite: 15 RPM, 250K TPM free tier (per-project quota)
# Gemini 3.1 Flash Lite: 15 RPM, 250K TPM free tier (per-project quota)
# By alternating, each model gets ~6 req/min (within limit with margin)
GEMINI_MODELS = ["gemini-3.5-flash-lite", "gemini-3.1-flash-lite"]
GEMINI_MODEL_INDEX = 0  # Start with 3.5
GEMINI_DISABLED_MODELS = set()  # Models that hit rate limits during this run
GEMINI_BACKOFF_MODELS = {}  # Maps model -> backoff_until_time for 503 errors
GEMINI_BACKOFF_SECONDS = 120  # 2 minutes backoff for 503 errors
GEMINI_PACE_SECONDS = 2.0  # 2s from request start to next request start (15 RPM per model when alternating)

DB_CONNECT_MAX_ATTEMPTS = 5
RUNTIME_GUARD_EXIT_CODE = 42
MAX_RUNTIME_SECONDS = 3600  # 60 minutes
MAX_PAGES_PER_WORKER = int(os.environ.get("MAX_PAGES_PER_WORKER", "0"))

# Enable debug mode for small test runs
DEBUG = MAX_PAGES_PER_WORKER > 0 and MAX_PAGES_PER_WORKER < 20

START_TIME = time.time()
LAST_REQUEST_START_TIME = None  # Track when the last request started (for 5s pacing)

SYSTEM_PROMPT = """You are a language identification and translation expert. You will analyze text from scanned pages of British colonial-era "Catalogue of Books registered" volumes.

Your task:
1. Identify ALL languages present in the provided text (not just the dominant one)
2. For each language identified:
   - Provide the language name and ISO 639-1 code
   - If the language is NOT English, extract the relevant portions and translate them to English
3. Do NOT translate English text to English
4. Return results as JSON only, no commentary

Return format:
{
  "languages": [
    {"language": "English", "code": "en"},
    {"language": "Punjabi", "code": "pa"},
    ...
  ],
  "translations": [
    {
      "source_language": "Punjabi",
      "source_code": "pa",
      "original_text": "...[punjabi text]...",
      "english_translation": "...[english translation]..."
    },
    ...
  ]
}

Only include translations for non-English languages. If a page is entirely in English, return empty translations array."""


def elapsed():
    return time.time() - START_TIME


def log_debug(msg):
    """Print debug message if DEBUG mode is enabled."""
    if DEBUG:
        print(f"[DEBUG] {msg}", file=sys.stderr)


def db_connect():
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError as exc:
            if attempt == DB_CONNECT_MAX_ATTEMPTS:
                raise
            print(f"DB connect attempt {attempt}/{DB_CONNECT_MAX_ATTEMPTS} failed; retrying", file=sys.stderr)
            time.sleep(2**attempt)


def get_next_model():
    """Return next available model in rotation (skip disabled/backed-off ones).

    Returns: model name if available, or None if all models are disabled
    Raises: RuntimeError if all models have hit their rate limits or all are backed off
    """
    global GEMINI_MODEL_INDEX

    now = time.time()

    # Clean up expired backoffs
    expired_models = []
    for model, backoff_until in GEMINI_BACKOFF_MODELS.items():
        if now >= backoff_until:
            expired_models.append(model)
    for model in expired_models:
        del GEMINI_BACKOFF_MODELS[model]
        print(f"  ✓ {model} backoff expired, resuming")

    # Check if all models are disabled (permanent quota exhaustion)
    if len(GEMINI_DISABLED_MODELS) == len(GEMINI_MODELS):
        raise RuntimeError(f"All models have hit rate limits: {', '.join(GEMINI_MODELS)}")

    # Find next available model, skipping disabled and backed-off ones
    attempts = 0
    while attempts < len(GEMINI_MODELS):
        model = GEMINI_MODELS[GEMINI_MODEL_INDEX]
        GEMINI_MODEL_INDEX = (GEMINI_MODEL_INDEX + 1) % len(GEMINI_MODELS)

        if model not in GEMINI_DISABLED_MODELS and model not in GEMINI_BACKOFF_MODELS:
            return model
        attempts += 1

    raise RuntimeError(f"No available models (disabled: {GEMINI_DISABLED_MODELS}, backed off: {list(GEMINI_BACKOFF_MODELS.keys())})")


def wait_for_rate_limit():
    """Pace requests: maintain 5s from previous request start to this request start."""
    global LAST_REQUEST_START_TIME
    now = time.time()
    if LAST_REQUEST_START_TIME is not None:
        elapsed_since_last = now - LAST_REQUEST_START_TIME
        if elapsed_since_last < GEMINI_PACE_SECONDS:
            sleep_time = GEMINI_PACE_SECONDS - elapsed_since_last
            time.sleep(sleep_time)
    LAST_REQUEST_START_TIME = time.time()


def detect_and_translate_page(ocr_text, model):
    """Send full OCR text to Gemini, get language IDs and translations.

    Args:
        ocr_text: Full page OCR text to analyze
        model: Model ID to use (e.g., "gemini-3.5-flash-lite")

    Returns: {"languages": [...], "translations": [...]}
    Raises: RuntimeError on API error or unexpected response format
    """
    wait_for_rate_limit()

    log_debug(f"OCR text length: {len(ocr_text)} characters")
    log_debug(f"OCR text (first 500 chars): {ocr_text[:500]!r}")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    log_debug(f"Calling Gemini API: {url} ({model})")

    payload = {
        "contents": [{
            "role": "user",
            "parts": [{
                "text": f"{SYSTEM_PROMPT}\n\nPage OCR text:\n\n{ocr_text}"
            }]
        }],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 4096,
            "responseMimeType": "application/json"
        },
        "safetySettings": [
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_NONE"
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_NONE"
            }
        ]
    }

    log_debug(f"Request payload keys: {list(payload.keys())}")

    start_api = time.time()
    resp = requests.post(
        url,
        json=payload,
        headers={"x-goog-api-key": GEMINI_API_KEY},
        timeout=(10, 600)
    )
    api_time = time.time() - start_api

    log_debug(f"API response status: {resp.status_code} (took {api_time:.1f}s)")
    log_debug(f"Response headers: {dict(resp.headers)}")

    if not resp.ok:
        log_debug(f"Error response body: {resp.text[:2000]}")
        # Signal rate limit errors distinctly so we can disable this model
        if resp.status_code == 429:
            raise RuntimeError(f"Gemini {model} rate limit hit (429): quota exhausted for this model")
        # Signal high-demand errors (503) for temporary backoff
        if resp.status_code == 503:
            raise RuntimeError(f"Gemini {model} high demand (503): {resp.text[:500]}")
        raise RuntimeError(f"Gemini API returned {resp.status_code}: {resp.text[:1000]}")

    response_data = resp.json()
    log_debug(f"Response JSON keys: {list(response_data.keys())}")

    if "candidates" not in response_data or not response_data["candidates"]:
        log_debug(f"Full response: {json.dumps(response_data, indent=2)}")
        raise RuntimeError(f"Unexpected Gemini response format: {response_data}")

    candidate = response_data["candidates"][0]
    log_debug(f"Candidate keys: {list(candidate.keys())}")

    if "content" not in candidate or "parts" not in candidate["content"]:
        log_debug(f"Full candidate: {json.dumps(candidate, indent=2)}")
        raise RuntimeError(f"No content in Gemini response: {response_data}")

    text = candidate["content"]["parts"][0]["text"]
    log_debug(f"Extracted text length: {len(text)} characters")
    log_debug(f"Extracted text (first 1000 chars): {text[:1000]!r}")

    try:
        result = json.loads(text)
    except json.JSONDecodeError as exc:
        log_debug(f"Failed to parse JSON, full text: {text[:1000]}")
        raise RuntimeError(f"Gemini returned invalid JSON: {text[:500]}") from exc

    log_debug(f"Parsed result keys: {list(result.keys())}")
    log_debug(f"Languages detected: {result.get('languages', [])}")
    log_debug(f"Translations count: {len(result.get('translations', []))}")

    for i, trans in enumerate(result.get("translations", [])):
        log_debug(f"  Translation {i+1}: {trans.get('source_language')} → English")
        log_debug(f"    Original length: {len(trans.get('original_text', ''))} chars")
        log_debug(f"    Translation length: {len(trans.get('english_translation', ''))} chars")
        log_debug(f"    Original (first 200 chars): {trans.get('original_text', '')[:200]!r}")
        log_debug(f"    Translation (first 200 chars): {trans.get('english_translation', '')[:200]!r}")

    if not isinstance(result.get("languages"), list):
        log_debug(f"Invalid structure, full result: {json.dumps(result, indent=2)}")
        raise RuntimeError(f"Invalid response structure: {result}")

    return result


def save_languages_and_translations(page_id, model_tag, analysis_result):
    """Store detected languages and translations in database."""
    log_debug(f"Saving results for page {page_id}")

    with db_connect() as conn:
        with conn.cursor() as cur:
            # Insert detected languages
            languages = analysis_result.get("languages", [])
            log_debug(f"  Inserting {len(languages)} language(s)")
            for lang_info in languages:
                log_debug(f"    - {lang_info['language']} ({lang_info['code']})")
                cur.execute(
                    """
                    INSERT INTO page_detected_languages (page_id, language, language_code, model_tag)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (page_id, language_code) DO NOTHING
                    """,
                    (page_id, lang_info["language"], lang_info["code"], model_tag)
                )

            # Insert translations (non-English only)
            translations = analysis_result.get("translations", [])
            log_debug(f"  Inserting {len(translations)} translation(s)")
            for i, trans in enumerate(translations):
                log_debug(f"    - Translation {i+1}: {trans['source_language']} → English")
                log_debug(f"      Original: {len(trans['original_text'])} chars, Translated: {len(trans['english_translation'])} chars")
                cur.execute(
                    """
                    INSERT INTO page_translations (
                        page_id, source_language, source_language_code,
                        original_text, translated_text, model_tag
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        page_id,
                        trans["source_language"],
                        trans["source_code"],
                        trans["original_text"],
                        trans["english_translation"],
                        model_tag
                    )
                )

        conn.commit()

    log_debug(f"  ✓ Saved to database")


def get_pages_needing_language_detection():
    """Fetch pages with successful OCR that haven't been analyzed yet.

    Each page appears exactly once (one OCR per page, even if multiple models
    have OCR'd it). Prioritizes by model_tag: gemma-4-31b-it first, then
    gemma-4-26b-a4b-it, then others in order of creation.

    Returns list of (page_id, ocr_text) tuples.
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (p.id) p.id, pot.raw_text
                FROM pages p
                JOIN page_ocr_text pot ON pot.page_id = p.id
                WHERE pot.status = 'success'
                  AND pot.raw_text IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM page_detected_languages pdl
                    WHERE pdl.page_id = p.id
                  )
                  AND p.image_uploaded_at IS NOT NULL
                  AND p.excluded_at IS NULL
                ORDER BY p.id,
                  CASE pot.model_tag
                    WHEN 'gemma-4-31b-it' THEN 0
                    WHEN 'gemma-4-26b-a4b-it' THEN 1
                    ELSE 2
                  END,
                  pot.created_at
                LIMIT %s
                """,
                (MAX_PAGES_PER_WORKER if MAX_PAGES_PER_WORKER else 1000000,)
            )
            return cur.fetchall()


def main():
    print(f"Language detection and translation worker starting")
    print(f"Models: {' ↔ '.join(GEMINI_MODELS)} (alternating)")
    print(f"  Will skip models that hit rate limits; stops if all models disabled")
    print(f"Pace: {GEMINI_PACE_SECONDS}s between request starts")
    print(f"DEBUG mode: {'ON' if DEBUG else 'OFF'}")
    if DEBUG:
        print(f"  (DEBUG enabled: processing < 20 pages)")

    pages_processed = 0
    pages_failed = 0

    try:
        pages = get_pages_needing_language_detection()
        print(f"Found {len(pages)} pages needing language detection")
        if DEBUG:
            log_debug(f"Pages to process: {[p[0] for p in pages]}")

        for page_num, (page_id, ocr_text) in enumerate(pages, 1):
            if MAX_RUNTIME_SECONDS and elapsed() > MAX_RUNTIME_SECONDS:
                print(f"\n⏱️ Runtime limit reached after {elapsed():.0f}s")
                sys.exit(RUNTIME_GUARD_EXIT_CODE)

            try:
                # Select model for this request (may raise if all models disabled)
                current_model = get_next_model()
                print(f"[{page_num}/{len(pages)}] Processing page {page_id} ({current_model})... ", end="", flush=True)

                if DEBUG:
                    log_debug(f"Page {page_id}: OCR text length: {len(ocr_text)}, using {current_model}")

                # Detect languages and get translations
                result = detect_and_translate_page(ocr_text, current_model)

                # Store in database with model tag
                save_languages_and_translations(page_id, current_model, result)

                lang_count = len(result.get("languages", []))
                trans_count = len(result.get("translations", []))
                print(f"✓ {lang_count} language(s), {trans_count} translation(s)")
                pages_processed += 1

            except RuntimeError as exc:
                error_msg = str(exc)

                # Check if this is a 503 high-demand error for temporary backoff
                if "high demand (503)" in error_msg:
                    # Extract which model hit the limit and back it off
                    model_backedup = False
                    for model in GEMINI_MODELS:
                        if model in error_msg:
                            backoff_until = time.time() + GEMINI_BACKOFF_SECONDS
                            GEMINI_BACKOFF_MODELS[model] = backoff_until
                            print(f"✗ {model} backed off (high demand, will retry in 2 minutes)")
                            model_backedup = True
                            break

                    if model_backedup:
                        # Check if other models still available
                        available_count = len(GEMINI_MODELS) - len(GEMINI_DISABLED_MODELS) - len(GEMINI_BACKOFF_MODELS)
                        if available_count > 0:
                            print(f"  Continuing with remaining model(s)")
                            # Don't count as failed - this is transient
                        else:
                            print(f"✗ All models backed off or disabled - stopping")
                            raise
                    else:
                        # High-demand error but couldn't identify model, treat as transient
                        print(f"✗ {exc}")
                        if DEBUG:
                            import traceback
                            log_debug(f"Exception traceback:")
                            for line in traceback.format_exc().split('\n'):
                                log_debug(line)

                # Check if this is a 429 rate limit error for permanent disable
                elif "rate limit hit (429)" in error_msg:
                    # Extract which model hit the limit and disable it
                    model_disabled = False
                    for model in GEMINI_MODELS:
                        if model in error_msg:
                            GEMINI_DISABLED_MODELS.add(model)
                            print(f"✗ {model} disabled (rate limit)")
                            model_disabled = True
                            break

                    if model_disabled:
                        # Check if other models still available
                        if len(GEMINI_DISABLED_MODELS) < len(GEMINI_MODELS):
                            print(f"  Continuing with remaining model(s)")
                            pages_failed += 1
                        else:
                            print(f"✗ All models disabled - stopping")
                            raise
                    else:
                        # Rate limit error but couldn't identify model, treat as fatal
                        print(f"✗ {exc}")
                        pages_failed += 1
                else:
                    # Non-rate-limit error, just log and continue
                    print(f"✗ {exc}")
                    if DEBUG:
                        import traceback
                        log_debug(f"Exception traceback:")
                        for line in traceback.format_exc().split('\n'):
                            log_debug(line)
                    pages_failed += 1

            except Exception as exc:
                print(f"✗ {exc}")
                if DEBUG:
                    import traceback
                    log_debug(f"Exception traceback:")
                    for line in traceback.format_exc().split('\n'):
                        log_debug(line)
                pages_failed += 1

    except Exception as exc:
        print(f"Fatal error: {exc}", file=sys.stderr)
        if DEBUG:
            import traceback
            log_debug(f"Fatal error details:")
            for line in traceback.format_exc().split('\n'):
                log_debug(line)
        sys.exit(1)

    print(f"\n✓ Processed {pages_processed} pages, {pages_failed} failed ({elapsed():.0f}s elapsed)")
    if pages_failed == 0:
        print("All done!")


if __name__ == "__main__":
    main()
