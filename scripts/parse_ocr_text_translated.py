"""Extract structured entries from translated full-page OCR text.

Targets pages awaiting structured extraction (not yet in page_extractions_canonical).
For each page:
  1. Uses translated text from page_translations if available
  2. Falls back to original OCR text if no translation exists
  3. Runs text-based structured extraction
  4. Stores results in llm_extractions

Prioritizes pages that have been language-detected and translated, since
translations make multilingual content more extractable for text models.

Uses Ollama text models (mistral:7b by default) running locally.
Each worker claims pages atomically and marks them 'claimed' while processing,
so multiple workers can run in parallel safely. Workers respect rate limits
and timeouts consistent with extract_with_llm.py.
"""
import json
import logging
import os
import re
import sys
import time

import psycopg2
from psycopg2.extras import Json

# Configure logging for GitHub Actions visibility
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
logger.addHandler(handler)
logger.propagate = False

SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "mistral:7b")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

MODEL_TAG = re.sub(r"[^A-Za-z0-9._-]", "-", OLLAMA_MODEL)
WORKER_ID = os.environ.get("GITHUB_RUN_ID", str(os.getpid()))

CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60
MAX_RUNTIME_SECONDS = 3600  # 60 minutes
START_TIME = time.time()

EXTRACTION_PROMPT = """Extract all catalogue entries from this text. Each entry may contain:
- Title, author, publisher, publication date/location
- Edition, number of pages, price
- Subject classification, language
- Any other relevant metadata

Return a JSON array of objects with these fields (null if not found):
- title
- author
- publisher
- pubcity (publication city)
- date (publication date)
- pages (page count)
- edition
- price
- lang (language)
- notes (any other relevant info)

Return ONLY the JSON array, no other text."""


def db_connect():
    """Connect to Supabase with retry logic."""
    for attempt in range(1, 6):
        try:
            return psycopg2.connect(SUPABASE_DB_URL)
        except psycopg2.OperationalError as exc:
            if attempt == 5:
                raise
            logger.warning(f"DB connect attempt {attempt}/5 failed; retrying...")
            time.sleep(2 ** attempt)


def get_ocr_text_for_page(conn, page_id):
    """Get OCR text for a page, preferring translated version if available."""
    with conn.cursor() as cur:
        # Try to get translated text first
        cur.execute("""
            SELECT
                COALESCE(
                    STRING_AGG(pt.english_translation, E'\n\n' ORDER BY pt.id),
                    pot.raw_text
                ) as text
            FROM pages p
            LEFT JOIN page_ocr_text pot ON pot.page_id = p.id AND pot.status = 'success'
            LEFT JOIN page_translations pt ON pt.page_id = p.id
            WHERE p.id = %s
            GROUP BY pot.raw_text
        """, (page_id,))

        result = cur.fetchone()
        return result[0] if result else None


def extract_with_ollama(ocr_text):
    """Send text to Ollama for extraction."""
    import requests

    if not ocr_text or len(ocr_text) < 50:
        return None, "OCR text too short"

    try:
        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": f"{EXTRACTION_PROMPT}\n\nText:\n{ocr_text}",
                "stream": False,
                "temperature": 0.3,
            },
            timeout=300,
        )

        if response.status_code != 200:
            return None, f"Ollama error: {response.status_code}"

        result = response.json()
        response_text = result.get("response", "").strip()

        # Try to extract JSON from response
        json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if json_match:
            entries = json.loads(json_match.group())
            return entries, None
        else:
            return None, "No JSON found in response"

    except requests.RequestException as e:
        return None, f"Request failed: {str(e)}"
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON: {str(e)}"


def claim_next_awaiting_page(conn):
    """Claim the next page awaiting extraction."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT p.id
                FROM pages p
                LEFT JOIN llm_extractions le
                    ON le.page_id = p.id AND le.model_tag = %s
                WHERE p.image_uploaded_at IS NOT NULL
                  AND p.excluded_at IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM page_extractions_canonical pec
                    WHERE pec.page_id = p.id
                  )
                  AND (
                    le.id IS NULL
                    OR (le.status = 'claimed'
                        AND le.claimed_at < now() - %s * interval '1 second')
                    OR (le.status = 'failed'
                        AND le.claimed_at < now() - %s * interval '1 second')
                  )
                ORDER BY
                    EXISTS (
                        SELECT 1 FROM page_translations pt WHERE pt.page_id = p.id
                    ) DESC,
                    (p.id %% %s) ASC
                LIMIT 1
                FOR UPDATE OF p SKIP LOCKED
            )
            INSERT INTO llm_extractions (page_id, model, model_tag, status, claimed_at, attempt_count)
            SELECT id, %s, %s, 'claimed', now(), 1
            FROM candidate
            ON CONFLICT (page_id, model_tag) DO UPDATE SET
                status = 'claimed',
                claimed_at = now(),
                error_message = NULL,
                raw_text = NULL
            RETURNING page_id
        """, (
            MODEL_TAG,
            CLAIM_TIMEOUT_SECONDS,
            CLAIM_TIMEOUT_SECONDS,
            hash(WORKER_ID) % 1000,
            OLLAMA_MODEL,
            MODEL_TAG,
        ))

        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None


def save_extraction(conn, page_id, entries):
    """Save extracted entries to database."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE llm_extractions
            SET status = 'success', error_message = NULL
            WHERE page_id = %s AND model_tag = %s
        """, (page_id, MODEL_TAG))

        # Insert catalogue entries
        for idx, entry in enumerate(entries or []):
            cur.execute("""
                INSERT INTO catalogue_entries (
                    extraction_id, entry_index, title, author, publisher, pubcity,
                    date, edition, pp_verbatim, price, lang, notes
                )
                SELECT id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                FROM llm_extractions
                WHERE page_id = %s AND model_tag = %s
                ON CONFLICT DO NOTHING
            """, (
                idx,
                entry.get('title'),
                entry.get('author'),
                entry.get('publisher'),
                entry.get('pubcity'),
                entry.get('date'),
                entry.get('edition'),
                entry.get('pages'),
                entry.get('price'),
                entry.get('lang'),
                entry.get('notes'),
                page_id,
                MODEL_TAG,
            ))

        conn.commit()


def save_failure(conn, page_id, error_message):
    """Save extraction failure."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE llm_extractions
            SET status = 'failed', error_message = %s
            WHERE page_id = %s AND model_tag = %s
        """, (error_message, page_id, MODEL_TAG))
        conn.commit()


def main():
    """Main extraction loop."""
    logger.info(f"Starting extracted from translated OCR (model={OLLAMA_MODEL})")

    if not SUPABASE_DB_URL:
        logger.error("SUPABASE_DB_URL not set")
        sys.exit(1)

    conn = db_connect()
    pages_processed = 0

    try:
        while True:
            # Check runtime limit
            elapsed = time.time() - START_TIME
            if elapsed > MAX_RUNTIME_SECONDS:
                logger.info(f"Runtime limit reached ({elapsed:.0f}s)")
                break

            # Claim next page
            page_id = claim_next_awaiting_page(conn)
            if not page_id:
                logger.info("No more pages to process")
                break

            logger.info(f"Processing page {page_id}")

            try:
                # Get OCR text (preferring translated)
                ocr_text = get_ocr_text_for_page(conn, page_id)
                if not ocr_text:
                    save_failure(conn, page_id, "No OCR text available")
                    continue

                # Extract with Ollama
                entries, error = extract_with_ollama(ocr_text)
                if error:
                    save_failure(conn, page_id, error)
                    logger.warning(f"Page {page_id}: {error}")
                    continue

                # Save results
                save_extraction(conn, page_id, entries)
                pages_processed += 1
                logger.info(f"Page {page_id}: extracted {len(entries or [])} entries")

            except Exception as e:
                logger.error(f"Page {page_id}: {type(e).__name__}: {e}")
                save_failure(conn, page_id, f"{type(e).__name__}: {str(e)[:500]}")

    finally:
        conn.close()

    logger.info(f"Complete: {pages_processed} pages processed")


if __name__ == "__main__":
    main()
