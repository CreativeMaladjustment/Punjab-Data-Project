# Language Detection and Translation Pipeline

## Overview

Analyzes full-page OCR text to:
1. Identify all languages present on each page
2. Translate non-English sections to English for analysis/search

Uses **Gemini 3.5 Flash Lite** (optimized for speed/efficiency on text tasks).

## Database Schema

### `page_detected_languages`
One row per unique language detected on a page.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL | Primary key |
| page_id | INTEGER | FK to pages |
| language | TEXT | Language name (e.g., "Punjabi", "English", "Urdu") |
| language_code | TEXT | ISO 639-1/639-2 code (e.g., "pa", "en", "ur") |
| model_tag | TEXT | Which model detected it |
| created_at | TIMESTAMP | When detected |

**Unique constraint:** (page_id, language_code) — one record per language per page

### `page_translations`
Original non-English text + English translation for each language/section on a page.

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL | Primary key |
| page_id | INTEGER | FK to pages |
| source_language | TEXT | Original language name |
| source_language_code | TEXT | ISO code of original |
| original_text | TEXT | Text in original language |
| translated_text | TEXT | English translation |
| section_index | INTEGER | For future multi-section handling (currently 0) |
| model_tag | TEXT | Which model translated it |
| created_at | TIMESTAMP | When translated |

## Processing Flow

**Script:** `scripts/detect_languages_and_translate.py`

1. Reads from `page_ocr_text` (successful OCR text only)
2. Finds pages not yet in `page_detected_languages`
3. For each page:
   - Sends full OCR text to Gemini 3.5 Flash Lite
   - Gets back: list of languages + translations for non-English
   - Stores in both tables
4. Paces at 5s per request (conservative vs 15 RPM free-tier limit)
5. Respects 5-hour runtime limit (exit code 42 to signal for manual resume)

**Workflow:** `.github/workflows/detect-languages.yml`
- Runs on schedule: **5 AM and 5 PM UTC** (twice daily, off-peak hours)
- Can be manually triggered with optional `max_pages` parameter
- Environment: `b2-upload` (where GEMINI_API_KEY is stored)

## Usage

### Automatic (scheduled)
The workflow runs twice daily; no action needed.

### Manual trigger
```bash
gh workflow run detect-languages.yml -f max_pages=100
```

### Query results

```sql
-- All languages detected on a page
SELECT language, language_code
FROM page_detected_languages
WHERE page_id = 42;

-- Translations for a specific page
SELECT source_language, original_text, translated_text
FROM page_translations
WHERE page_id = 42;

-- Pages with multiple languages
SELECT p.id, COUNT(DISTINCT pdl.language_code) as lang_count
FROM pages p
JOIN page_detected_languages pdl ON pdl.page_id = p.id
GROUP BY p.id
HAVING COUNT(DISTINCT pdl.language_code) > 1;

-- Pages with non-English content (has translations)
SELECT DISTINCT p.id
FROM pages p
JOIN page_translations pt ON pt.page_id = p.id;
```

## Model Choice: Gemini 3.5 Flash Lite

- **Why:** Language ID and translation are straightforward tasks; doesn't need model size
- **Speed:** Optimized for fast inference
- **Efficiency:** Excellent token efficiency (13.64K/250K TPM quota)
- **Cost:** Minimal token usage per page
- **Rate limits:** 15 RPM free tier (paced at 5s = 12 req/min for headroom)

Fallback to **Gemini 3.1 Flash Lite** if 3.5 becomes unavailable (same capabilities, same quotas).

## Extending

### Support for more models
Later, can add **Gemini 3.1 Flash Lite** as alternative:
- Set env var: `GEMINI_MODEL=gemini-3.1-flash-lite`
- Update pacing if needed (currently same 15 RPM limit)

### Multi-section handling
Currently treats entire page as one unit. To split OCR text into sections:
1. Parse OCR text into regions (e.g., by detected layout breaks)
2. Increment `section_index` in `page_translations` for each
3. Modify system prompt to translate per-section

### Integration with catalogue_entries
Translations could later feed into:
- Full-text search (searchable English versions of Punjabi/Urdu entries)
- Data quality metrics (what % of pages have multiple languages)
- Workflow targeting (pages needing manual review of translations)
