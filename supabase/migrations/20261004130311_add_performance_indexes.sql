-- Add critical performance indexes to improve extraction and processing queries.
-- These indexes target the four slowest query patterns identified in Supabase analytics:
-- 1. Extraction candidate selection with multiple NOT EXISTS subqueries (299ms/call × 30K calls)
-- 2. OCR text lookups filtering by page_id, model_tag, status
-- 3. QC review lookups by extraction_id
-- 4. Language detection filtering by page_id

-- Index for llm_extractions candidate selection
-- Supports: WHERE ... AND le.page_id = p.id AND le.model_tag = $1
--           AND le.status IN ('claimed', 'failed')
--           AND le.claimed_at < now() - interval
--           AND le.content_failure = ...
-- This is the hottest query path: 30,299 calls × 299ms mean = 9M ms total
CREATE INDEX IF NOT EXISTS idx_llm_extractions_candidate_lookup
  ON llm_extractions(page_id, model_tag, status, claimed_at)
  INCLUDE (content_failure, attempt_count);

-- Index for page_ocr_text lookups
-- Supports: WHERE pot.page_id = p.id AND pot.status = 'success'
--           AND pot.model_tag IN ('...')
-- Used in language detection query (16,841ms mean) and extraction fallback paths
CREATE INDEX IF NOT EXISTS idx_page_ocr_text_candidate_lookup
  ON page_ocr_text(page_id, model_tag, status)
  INCLUDE (raw_text, created_at);

-- Index for QC review lookups
-- Supports: WHERE qr.extraction_id = le.id AND qr.verdict = $1
-- Used in extraction routing logic to determine if a page has been reviewed
CREATE INDEX IF NOT EXISTS idx_qc_reviews_extraction_verdict
  ON qc_reviews(extraction_id, verdict);

-- Index for language detection
-- Supports: WHERE NOT EXISTS (SELECT ... FROM page_detected_languages WHERE page_id = p.id)
-- Language detection query scans 309,644 rows for 14 results without this index
CREATE INDEX IF NOT EXISTS idx_page_detected_languages_page_id
  ON page_detected_languages(page_id);

-- Analysis:
-- Expected query performance improvements:
-- - llm_extractions queries: 300-600ms → 30-50ms (85% improvement)
-- - page_ocr_text queries: 7-16ms → 2-5ms (70% improvement)
-- - Language detection: 16,841ms → 300-500ms (95% improvement)
--
-- Combined daily Supabase CPU reduction: ~93% (from ~16 hours to ~1 hour)
-- Risk: Minimal - indexes are additive, no data changes
-- Rollback: DROP INDEX IF EXISTS idx_*
