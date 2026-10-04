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

-- Post-process existing entries with ditto marks using recursive resolution.
-- This handles chains of dittos (do., do., do.) by iterating through entries
-- in order and using already-resolved predecessors, not original values.
WITH RECURSIVE ditto_resolution AS (
  -- Base case: first entry (entry_index=0) keeps original values (no predecessor to reference)
  SELECT
    ce.id,
    ce.extraction_id,
    ce.entry_index,
    ce.author as resolved_author,
    ce.printer as resolved_printer,
    ce.pcity as resolved_pcity,
    ce.publisher as resolved_publisher,
    ce.pubcity as resolved_pubcity
  FROM catalogue_entries ce
  WHERE ce.entry_index = 0
  UNION ALL
  -- Recursive case: resolve each subsequent entry using the previous entry's RESOLVED values
  SELECT
    ce.id,
    ce.extraction_id,
    ce.entry_index,
    CASE
      WHEN ce.author IS NOT NULL AND LOWER(ce.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN dr.resolved_author
      ELSE ce.author
    END as resolved_author,
    CASE
      WHEN ce.printer IS NOT NULL AND LOWER(ce.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN dr.resolved_printer
      ELSE ce.printer
    END as resolved_printer,
    CASE
      WHEN ce.pcity IS NOT NULL AND LOWER(ce.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN dr.resolved_pcity
      ELSE ce.pcity
    END as resolved_pcity,
    CASE
      WHEN ce.publisher IS NOT NULL AND LOWER(ce.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN dr.resolved_publisher
      ELSE ce.publisher
    END as resolved_publisher,
    CASE
      WHEN ce.pubcity IS NOT NULL AND LOWER(ce.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN dr.resolved_pubcity
      ELSE ce.pubcity
    END as resolved_pubcity
  FROM catalogue_entries ce
  JOIN ditto_resolution dr ON
    dr.extraction_id = ce.extraction_id
    AND dr.entry_index = ce.entry_index - 1
)
UPDATE catalogue_entries ce
SET
  author = dr.resolved_author,
  printer = dr.resolved_printer,
  pcity = dr.resolved_pcity,
  publisher = dr.resolved_publisher,
  pubcity = dr.resolved_pubcity
FROM ditto_resolution dr
WHERE ce.id = dr.id
  AND (
    ce.author IS DISTINCT FROM dr.resolved_author OR
    ce.printer IS DISTINCT FROM dr.resolved_printer OR
    ce.pcity IS DISTINCT FROM dr.resolved_pcity OR
    ce.publisher IS DISTINCT FROM dr.resolved_publisher OR
    ce.pubcity IS DISTINCT FROM dr.resolved_pubcity
  );
