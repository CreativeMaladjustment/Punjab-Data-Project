-- Single canonical extraction per page for cleaner data representation
-- Strategy: text-based parsing > first successful vision model > human review
-- This table is the "source of truth" extraction for each page, used by
-- reports and the canonical QC review interface.

create table page_extractions_canonical (
    id bigint generated always as identity primary key,
    page_id bigint not null unique references pages (id) on delete cascade,
    extraction_id bigint not null references llm_extractions (id) on delete cascade,
    model_tag text not null,
    -- Ranking priority (lower = higher priority, wins ties)
    -- 1: textparse:* (text-based parsing after full OCR)
    -- 2: first successful vision model attempt
    -- 3: human:review (human correction)
    priority int not null check (priority >= 1 and priority <= 3),
    entry_count int,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index page_extractions_canonical_page_id_idx on page_extractions_canonical (page_id);
create index page_extractions_canonical_model_tag_idx on page_extractions_canonical (model_tag);
create index page_extractions_canonical_priority_idx on page_extractions_canonical (priority);

-- Populate initial canonical extractions for existing pages
-- Priority: textparse:* > human:review > first vision model by attempt order
INSERT INTO page_extractions_canonical (page_id, extraction_id, model_tag, priority, entry_count)
WITH ranked_extractions AS (
  SELECT
    le.page_id,
    le.id as extraction_id,
    le.model_tag,
    COUNT(ce.id) as entry_count,
    -- Assign priority based on model type
    CASE
      WHEN le.model_tag LIKE 'textparse:%' THEN 1
      WHEN le.model_tag = 'human:review' THEN 3
      ELSE 2
    END as priority,
    -- For vision models, rank by creation order (first attempt wins)
    ROW_NUMBER() OVER (
      PARTITION BY le.page_id,
        CASE WHEN le.model_tag LIKE 'textparse:%' THEN 1 WHEN le.model_tag = 'human:review' THEN 3 ELSE 2 END
      ORDER BY le.created_at ASC
    ) as attempt_order
  FROM llm_extractions le
  LEFT JOIN catalogue_entries ce ON ce.extraction_id = le.id
  WHERE le.status = 'success'
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
ON CONFLICT (page_id) DO UPDATE SET
  extraction_id = EXCLUDED.extraction_id,
  model_tag = EXCLUDED.model_tag,
  priority = EXCLUDED.priority,
  entry_count = EXCLUDED.entry_count,
  updated_at = now();
