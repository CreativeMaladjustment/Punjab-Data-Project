-- Post-process ditto marks in catalogue entries using LAG window function.
-- Resolves ditto mark variants (ditto, ditto., do., Do., -do-) by replacing
-- them with the corresponding field value from the previous entry on the same page.
-- Applied on INSERT: when a new catalogue_entries row is added, if any ditto-marked
-- field exists, it's replaced with the previous entry's value for that field.

CREATE OR REPLACE FUNCTION resolve_ditto_marks_on_page()
RETURNS TRIGGER AS $$
DECLARE
  v_extraction_id BIGINT;
  v_page_id BIGINT;
  v_prev_author TEXT;
  v_prev_printer TEXT;
  v_prev_pcity TEXT;
  v_prev_publisher TEXT;
  v_prev_pubcity TEXT;
BEGIN
  -- Get extraction and page IDs from the newly inserted row
  v_extraction_id := NEW.extraction_id;

  -- Get page_id from the llm_extractions table
  SELECT page_id INTO v_page_id
  FROM llm_extractions
  WHERE id = v_extraction_id;

  -- If this is the first entry (entry_index = 0) or there's no previous entry,
  -- leave ditto marks as-is (the LLM prompt should have flagged them)
  IF NEW.entry_index > 0 THEN
    -- Get the previous entry's values for fields that commonly have ditto marks
    SELECT author, printer, pcity, publisher, pubcity
    INTO v_prev_author, v_prev_printer, v_prev_pcity, v_prev_publisher, v_prev_pubcity
    FROM catalogue_entries
    WHERE extraction_id = v_extraction_id
      AND entry_index = NEW.entry_index - 1
    LIMIT 1;

    -- Helper function to check if a value is a ditto mark variant
    -- Check case-insensitively and handle the variants
    -- Replace ditto marks with previous entry's value

    IF NEW.author IS NOT NULL AND (
      LOWER(NEW.author) = 'ditto' OR
      LOWER(NEW.author) = 'ditto.' OR
      LOWER(NEW.author) = 'do' OR
      LOWER(NEW.author) = 'do.' OR
      LOWER(NEW.author) = '-do-'
    ) THEN
      NEW.author := v_prev_author;
    END IF;

    IF NEW.printer IS NOT NULL AND (
      LOWER(NEW.printer) = 'ditto' OR
      LOWER(NEW.printer) = 'ditto.' OR
      LOWER(NEW.printer) = 'do' OR
      LOWER(NEW.printer) = 'do.' OR
      LOWER(NEW.printer) = '-do-'
    ) THEN
      NEW.printer := v_prev_printer;
    END IF;

    IF NEW.pcity IS NOT NULL AND (
      LOWER(NEW.pcity) = 'ditto' OR
      LOWER(NEW.pcity) = 'ditto.' OR
      LOWER(NEW.pcity) = 'do' OR
      LOWER(NEW.pcity) = 'do.' OR
      LOWER(NEW.pcity) = '-do-'
    ) THEN
      NEW.pcity := v_prev_pcity;
    END IF;

    IF NEW.publisher IS NOT NULL AND (
      LOWER(NEW.publisher) = 'ditto' OR
      LOWER(NEW.publisher) = 'ditto.' OR
      LOWER(NEW.publisher) = 'do' OR
      LOWER(NEW.publisher) = 'do.' OR
      LOWER(NEW.publisher) = '-do-'
    ) THEN
      NEW.publisher := v_prev_publisher;
    END IF;

    IF NEW.pubcity IS NOT NULL AND (
      LOWER(NEW.pubcity) = 'ditto' OR
      LOWER(NEW.pubcity) = 'ditto.' OR
      LOWER(NEW.pubcity) = 'do' OR
      LOWER(NEW.pubcity) = 'do.' OR
      LOWER(NEW.pubcity) = '-do-'
    ) THEN
      NEW.pubcity := v_prev_pubcity;
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on catalogue_entries INSERT to resolve ditto marks
-- This runs BEFORE INSERT, so it modifies the row before it's actually inserted
DROP TRIGGER IF EXISTS resolve_ditto_marks_trigger ON catalogue_entries;
CREATE TRIGGER resolve_ditto_marks_trigger
BEFORE INSERT ON catalogue_entries
FOR EACH ROW
EXECUTE FUNCTION resolve_ditto_marks_on_page();

-- Post-process existing entries with ditto marks using the same logic
-- This query identifies entries with ditto marks and replaces them with
-- the previous entry's value using a window function approach
WITH ditto_marked AS (
  SELECT
    ce.id,
    ce.extraction_id,
    ce.entry_index,
    ce.author,
    ce.printer,
    ce.pcity,
    ce.publisher,
    ce.pubcity,
    LAG(ce.author) OVER (
      PARTITION BY ce.extraction_id
      ORDER BY ce.entry_index
    ) as prev_author,
    LAG(ce.printer) OVER (
      PARTITION BY ce.extraction_id
      ORDER BY ce.entry_index
    ) as prev_printer,
    LAG(ce.pcity) OVER (
      PARTITION BY ce.extraction_id
      ORDER BY ce.entry_index
    ) as prev_pcity,
    LAG(ce.publisher) OVER (
      PARTITION BY ce.extraction_id
      ORDER BY ce.entry_index
    ) as prev_publisher,
    LAG(ce.pubcity) OVER (
      PARTITION BY ce.extraction_id
      ORDER BY ce.entry_index
    ) as prev_pubcity
  FROM catalogue_entries ce
  WHERE ce.author IS NOT NULL OR ce.printer IS NOT NULL OR
        ce.pcity IS NOT NULL OR ce.publisher IS NOT NULL OR
        ce.pubcity IS NOT NULL
),
resolved AS (
  SELECT
    id,
    CASE
      WHEN LOWER(author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN prev_author
      ELSE author
    END as resolved_author,
    CASE
      WHEN LOWER(printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN prev_printer
      ELSE printer
    END as resolved_printer,
    CASE
      WHEN LOWER(pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN prev_pcity
      ELSE pcity
    END as resolved_pcity,
    CASE
      WHEN LOWER(publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN prev_publisher
      ELSE publisher
    END as resolved_publisher,
    CASE
      WHEN LOWER(pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN prev_pubcity
      ELSE pubcity
    END as resolved_pubcity
  FROM ditto_marked
)
UPDATE catalogue_entries ce
SET
  author = resolved.resolved_author,
  printer = resolved.resolved_printer,
  pcity = resolved.resolved_pcity,
  publisher = resolved.resolved_publisher,
  pubcity = resolved.resolved_pubcity
FROM resolved
WHERE ce.id = resolved.id
  AND (
    ce.author != resolved.resolved_author OR
    ce.printer != resolved.resolved_printer OR
    ce.pcity != resolved.resolved_pcity OR
    ce.publisher != resolved.resolved_publisher OR
    ce.pubcity != resolved.resolved_pubcity
  );
