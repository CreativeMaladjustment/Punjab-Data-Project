-- New canonical precedence: human:review > textparse:* > first successful other model.
--   1: human:review (human correction)
--   2: textparse:* (text-based parsing after full OCR)
--   3: first successful vision/other model attempt
-- Within a priority, the earliest successful attempt wins (unchanged).

create or replace function select_canonical_extraction(
    p_page_id bigint DEFAULT NULL
)
returns table (
    page_id bigint,
    extraction_id bigint,
    model_tag text,
    priority int,
    entry_count int
) as $$
WITH ranked_extractions AS (
  SELECT
    le.page_id,
    le.id as extraction_id,
    le.model_tag,
    COUNT(ce.id) as entry_count,
    CASE
      WHEN le.model_tag = 'human:review' THEN 1
      WHEN le.model_tag LIKE 'textparse:%' THEN 2
      ELSE 3
    END as priority,
    ROW_NUMBER() OVER (
      PARTITION BY le.page_id,
        CASE WHEN le.model_tag = 'human:review' THEN 1 WHEN le.model_tag LIKE 'textparse:%' THEN 2 ELSE 3 END
      ORDER BY le.created_at ASC
    ) as attempt_order
  FROM llm_extractions le
  LEFT JOIN catalogue_entries ce ON ce.extraction_id = le.id
  WHERE le.status = 'success'
    AND (p_page_id IS NULL OR le.page_id = p_page_id)
  GROUP BY le.id, le.page_id, le.model_tag
),
best_per_page AS (
  SELECT
    page_id,
    extraction_id,
    model_tag,
    entry_count,
    priority,
    ROW_NUMBER() OVER (
      PARTITION BY page_id
      ORDER BY priority ASC, attempt_order ASC
    ) as rank
  FROM ranked_extractions
)
SELECT
  page_id,
  extraction_id,
  model_tag,
  priority,
  entry_count
FROM best_per_page
WHERE rank = 1
$$ LANGUAGE SQL STABLE;

-- Re-rank every page under the new precedence (also re-copies entries).
SELECT refresh_canonical_extractions();
