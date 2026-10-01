"""Detect languages in full-page OCR text and translate non-English sections to English.

Uses the full-page OCR transcriptions (from page_ocr_text table) produced by
extract_with_llm.py's ocr_full_page() to:
  1. Identify all languages present on each page
  2. For each non-English language detected, translate the relevant sections to English

Results are stored in two tables:
  - page_detected_languages: one row per unique language per page
  - page_translations: original text + English translation for non-English content

Uses Gemini 3.5 Flash Lite (or 3.1 Flash Lite as fallback) for speed and efficiency --
language identification and translation are straightforward tasks that don't need
larger models.

Processes pages with successful OCR text that haven't been language-analyzed yet.
Respects Gemini's free-tier rate limits via pacing similar to extract_with_gemini.py.
"""
import json
import os
import re
import sys
import time

import psycopg2
import requests

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

# Gemini 3.5 Flash Lite: 15 RPM, 250K TPM free tier
# Gemini 3.1 Flash Lite: 15 RPM, 250K TPM free tier
# These are per-project quotas shared across ALL callers, so we pace conservatively
GEMINI_PACE_SECONDS = 5.0  # 12 requests/minute per this worker (conservative vs 15 RPM limit)

DB_CONNECT_MAX_ATTEMPTS = 5
RUNTIME_GUARD_EXIT_CODE = 42
MAX_RUNTIME_SECONDS = 18000  # 5 hours
MAX_PAGES_PER_WORKER = int(os.environ.get("MAX_PAGES_PER_WORKER", "0"))

START_TIME = time.time()

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


def db_connect():
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError as exc:
            if attempt == DB_CONNECT_MAX_ATTEMPTS:
                raise
            print(f"DB connect attempt {attempt}/{DB_CONNECT_MAX_ATTEMPTS} failed; retrying", file=sys.stderr)
            time.sleep(2**attempt)


def wait_for_rate_limit():
    """Pace requests to stay under Gemini's free-tier rate limit."""
    time.sleep(GEMINI_PACE_SECONDS)


def detect_and_translate_page(ocr_text, model_tag):
    """Send full OCR text to Gemini, get language IDs and translations.

    Returns: {"languages": [...], "translations": [...]}
    Raises: RuntimeError on API error or unexpected response format
    """
    wait_for_rate_limit()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

    resp = requests.post(
        url,
        json={
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
        },
        headers={"x-goog-api-key": GEMINI_API_KEY},
        timeout=(10, 600)
    )

    if not resp.ok:
        raise RuntimeError(f"Gemini API returned {resp.status_code}: {resp.text[:1000]}")

    response_data = resp.json()
    if "candidates" not in response_data or not response_data["candidates"]:
        raise RuntimeError(f"Unexpected Gemini response format: {response_data}")

    candidate = response_data["candidates"][0]
    if "content" not in candidate or "parts" not in candidate["content"]:
        raise RuntimeError(f"No content in Gemini response: {response_data}")

    text = candidate["content"]["parts"][0]["text"]

    try:
        result = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Gemini returned invalid JSON: {text[:500]}") from exc

    if not isinstance(result.get("languages"), list):
        raise RuntimeError(f"Invalid response structure: {result}")

    return result


def save_languages_and_translations(page_id, model_tag, analysis_result):
    """Store detected languages and translations in database."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            # Insert detected languages
            for lang_info in analysis_result.get("languages", []):
                cur.execute(
                    """
                    INSERT INTO page_detected_languages (page_id, language, language_code, model_tag)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (page_id, language_code) DO NOTHING
                    """,
                    (page_id, lang_info["language"], lang_info["code"], model_tag)
                )

            # Insert translations (non-English only)
            for trans in analysis_result.get("translations", []):
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


def get_pages_needing_language_detection(model_tag):
    """Fetch pages with successful OCR that haven't been analyzed yet.

    Returns list of (page_id, ocr_text) tuples.
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, pot.raw_text
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
                ORDER BY p.id
                LIMIT %s
                """,
                (MAX_PAGES_PER_WORKER if MAX_PAGES_PER_WORKER else 1000000,)
            )
            return cur.fetchall()


def main():
    print(f"Language detection and translation worker starting")
    print(f"Model: {GEMINI_MODEL}, Pace: {GEMINI_PACE_SECONDS}s per request")

    pages_processed = 0
    pages_failed = 0

    try:
        pages = get_pages_needing_language_detection(GEMINI_MODEL)
        print(f"Found {len(pages)} pages needing language detection")

        for page_id, ocr_text in pages:
            if MAX_RUNTIME_SECONDS and elapsed() > MAX_RUNTIME_SECONDS:
                print(f"\n⏱️ Runtime limit reached after {elapsed():.0f}s")
                sys.exit(RUNTIME_GUARD_EXIT_CODE)

            try:
                print(f"Processing page {page_id}... ", end="", flush=True)

                # Detect languages and get translations
                result = detect_and_translate_page(ocr_text, GEMINI_MODEL)

                # Store in database
                save_languages_and_translations(page_id, GEMINI_MODEL, result)

                lang_count = len(result.get("languages", []))
                trans_count = len(result.get("translations", []))
                print(f"✓ {lang_count} language(s), {trans_count} translation(s)")
                pages_processed += 1

            except Exception as exc:
                print(f"✗ {exc}")
                pages_failed += 1

    except Exception as exc:
        print(f"Fatal error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"\n✓ Processed {pages_processed} pages, {pages_failed} failed ({elapsed():.0f}s elapsed)")
    if pages_failed == 0:
        print("All done!")


if __name__ == "__main__":
    main()
