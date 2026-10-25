"""Translate non-English catalogue entry titles to English.

Reads distinct titles from catalogue_entries_canonical that have no row in
title_translations yet, asks Gemini (in batches) whether each is 100% English
and, if not, for an English translation, and records every checked title in
title_translations (English ones with is_english = true and no translation, so
they are not re-checked).

Alternates between two Gemini Flash Lite models like detect_languages_and_translate.py,
backs off on 429/503, and exits with code 42 at the runtime guard so the
next scheduled run resumes.
"""
import json
import logging
import os
import sys
import time

import psycopg2
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_handler)
logger.propagate = False

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

GEMINI_MODELS = ["gemini-3.5-flash-lite", "gemini-3.1-flash-lite"]
PACE_SECONDS = 3.0
BACKOFF_SECONDS = 120
BATCH_SIZE = int(os.environ.get("TITLE_BATCH_SIZE", "25"))
MAX_BATCHES = int(os.environ.get("MAX_BATCHES", "0"))  # 0 = unlimited
MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", "3600"))
RUNTIME_GUARD_EXIT_CODE = 42
MAX_CONSECUTIVE_FAILURES = 5

START_TIME = time.time()

PROMPT = """You are given book titles from scanned British colonial-era "Catalogue of Books registered" volumes of the Punjab. Titles may be in native script (Gurmukhi, Devanagari, Urdu/Arabic) or romanized Indic words (e.g. "Sikhan di Bhagat Mala"), or English.

For EACH title decide whether it is 100% English. A title is English only if every word is English; romanized Punjabi/Hindi/Urdu/Sanskrit words are NOT English. Titles that are a mix are NOT English.

For each non-English (or mixed) title give the primary source language and an accurate, natural English translation of the whole title.

Return JSON only: an array with one object per input, in the same order:
[{"index": 0, "is_english": true},
 {"index": 1, "is_english": false, "language": "Punjabi", "code": "pa", "translation": "..."}]

Titles (JSON array of {index, title}):
"""


def elapsed():
    return time.time() - START_TIME


def db_connect():
    for attempt in range(1, 6):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError:
            if attempt == 5:
                raise
            time.sleep(2 ** attempt)


class Models:
    """Round-robin over models, skipping ones in backoff."""

    def __init__(self):
        self.i = 0
        self.backoff_until = {}

    def next(self):
        now = time.time()
        for _ in range(len(GEMINI_MODELS)):
            model = GEMINI_MODELS[self.i]
            self.i = (self.i + 1) % len(GEMINI_MODELS)
            if self.backoff_until.get(model, 0) <= now:
                return model
        return None

    def back_off(self, model, seconds=BACKOFF_SECONDS):
        self.backoff_until[model] = time.time() + seconds

    def wait_for_any(self):
        return max(0, min(self.backoff_until.values()) - time.time())


_last_request = [0.0]


def call_gemini(model, titles):
    wait = PACE_SECONDS - (time.time() - _last_request[0])
    if wait > 0:
        time.sleep(wait)
    _last_request[0] = time.time()

    payload_titles = json.dumps(
        [{"index": i, "title": t} for i, t in enumerate(titles)], ensure_ascii=False
    )
    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
        json={
            "contents": [{"role": "user", "parts": [{"text": PROMPT + payload_titles}]}],
            "generationConfig": {
                "temperature": 0,
                "maxOutputTokens": 8192,
                "responseMimeType": "application/json",
            },
        },
        headers={"x-goog-api-key": GEMINI_API_KEY},
        timeout=(10, 300),
    )
    if resp.status_code in (429, 503):
        raise RuntimeError(f"backoff:{resp.status_code}")
    if not resp.ok:
        raise ValueError(f"Gemini {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        results = json.loads(text)
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unparseable Gemini response: {str(data)[:500]}") from exc
    if not isinstance(results, list):
        raise ValueError(f"Expected a JSON array, got: {text[:300]}")
    return results


def pending_titles(limit):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT cec.title
            FROM catalogue_entries_canonical cec
            WHERE cec.title IS NOT NULL AND btrim(cec.title) <> ''
              AND NOT EXISTS (
                SELECT 1 FROM title_translations tt WHERE tt.original_title = cec.title
              )
            ORDER BY cec.title
            LIMIT %s
            """,
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]


def save(titles, results, model):
    by_index = {r.get("index"): r for r in results if isinstance(r, dict)}
    rows = []
    for i, title in enumerate(titles):
        r = by_index.get(i)
        if r is None:
            continue  # model skipped it; stays pending for a later run
        if r.get("is_english") is True:
            rows.append((title, True, None, None, None, model))
        else:
            translation = (r.get("translation") or "").strip()
            if not translation:
                continue
            rows.append((title, False, r.get("language"), r.get("code"), translation, model))
    if rows:
        with db_connect() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO title_translations
                    (original_title, is_english, source_language, source_language_code,
                     english_translation, model_tag)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (original_title) DO NOTHING
                """,
                rows,
            )
    return len(rows), sum(1 for r in rows if not r[1])


def main():
    models = Models()
    batches = failures = 0
    total_checked = total_translated = 0

    while True:
        if MAX_RUNTIME_SECONDS and elapsed() > MAX_RUNTIME_SECONDS:
            logger.info("Runtime guard reached after %.0fs", elapsed())
            sys.exit(RUNTIME_GUARD_EXIT_CODE)
        if MAX_BATCHES and batches >= MAX_BATCHES:
            break

        titles = pending_titles(BATCH_SIZE)
        if not titles:
            logger.info("No more untranslated titles")
            break

        model = models.next()
        if model is None:
            wait = models.wait_for_any()
            if elapsed() + wait > MAX_RUNTIME_SECONDS:
                sys.exit(RUNTIME_GUARD_EXIT_CODE)
            time.sleep(wait)
            continue

        try:
            results = call_gemini(model, titles)
            checked, translated = save(titles, results, model)
        except RuntimeError as exc:  # 429/503
            logger.warning("%s %s; backing off model", model, exc)
            models.back_off(model)
            continue
        except Exception as exc:
            failures += 1
            logger.error("Batch failed on %s: %s", model, exc)
            if failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error("Too many consecutive failures; stopping")
                sys.exit(1)
            continue

        failures = 0
        batches += 1
        total_checked += checked
        total_translated += translated
        logger.info("batch %d (%s): %d/%d titles saved, %d need translation | total %d checked",
                    batches, model, checked, len(titles), translated, total_checked)
        if checked == 0:
            failures += 1
            if failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error("Model returned nothing usable repeatedly; stopping")
                sys.exit(1)

    logger.info("Done: %d titles checked, %d translated", total_checked, total_translated)


if __name__ == "__main__":
    main()
