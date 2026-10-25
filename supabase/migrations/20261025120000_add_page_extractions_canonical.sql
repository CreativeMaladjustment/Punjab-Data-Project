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


-- Function to select the canonical extraction for a page based on priority ranking
-- Priority: 1=textparse:* > 2=first vision model > 3=human:review
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
    -- Assign priority based on model type
    CASE
      WHEN le.model_tag LIKE 'textparse:%' THEN 1
      WHEN le.model_tag = 'human:review' THEN 3
      ELSE 2
    END as priority,
    -- For same-priority extractions, rank by creation order (first attempt wins)
    ROW_NUMBER() OVER (
      PARTITION BY le.page_id,
        CASE WHEN le.model_tag LIKE 'textparse:%' THEN 1 WHEN le.model_tag = 'human:review' THEN 3 ELSE 2 END
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


-- Function to refresh the canonical extractions table
-- Can be called on-demand or by triggers
create or replace function refresh_canonical_extractions(
    p_page_id bigint DEFAULT NULL
)
returns table (
    pages_updated int,
    pages_inserted int,
    pages_deleted int
) as $$
DECLARE
    v_updated int := 0;
    v_inserted int := 0;
    v_deleted int := 0;
BEGIN
    -- Update existing canonical records and insert new ones
    WITH canonical_data AS (
        SELECT * FROM select_canonical_extraction(p_page_id)
    )
    INSERT INTO page_extractions_canonical (page_id, extraction_id, model_tag, priority, entry_count)
    SELECT page_id, extraction_id, model_tag, priority, entry_count
    FROM canonical_data
    ON CONFLICT (page_id) DO UPDATE SET
        extraction_id = EXCLUDED.extraction_id,
        model_tag = EXCLUDED.model_tag,
        priority = EXCLUDED.priority,
        entry_count = EXCLUDED.entry_count,
        updated_at = now();

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    -- Delete canonical records for pages with no successful extractions
    DELETE FROM page_extractions_canonical pec
    WHERE (p_page_id IS NULL OR pec.page_id = p_page_id)
        AND NOT EXISTS (
            SELECT 1 FROM llm_extractions le
            WHERE le.page_id = pec.page_id AND le.status = 'success'
        );

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RETURN QUERY SELECT v_updated, v_inserted, v_deleted;
END;
$$ LANGUAGE plpgsql;


-- Trigger to automatically update canonical extraction when a new extraction is created/updated
create or replace function trigger_refresh_canonical_on_extraction()
returns trigger as $$
BEGIN
    -- Only process successful extractions
    IF NEW.status = 'success' THEN
        PERFORM refresh_canonical_extractions(NEW.page_id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

create trigger trigger_llm_extraction_canonical
after insert or update on llm_extractions
for each row
execute function trigger_refresh_canonical_on_extraction();


-- Trigger to update entry count when catalogue entries change
create or replace function trigger_update_canonical_entry_count()
returns trigger as $$
BEGIN
    UPDATE page_extractions_canonical pec
    SET entry_count = (
        SELECT COUNT(*) FROM catalogue_entries ce
        WHERE ce.extraction_id = pec.extraction_id
    )
    WHERE pec.extraction_id = COALESCE(NEW.extraction_id, OLD.extraction_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

create trigger trigger_catalogue_entry_count
after insert or delete on catalogue_entries
for each row
execute function trigger_update_canonical_entry_count();


-- Initial population
SELECT refresh_canonical_extractions();
