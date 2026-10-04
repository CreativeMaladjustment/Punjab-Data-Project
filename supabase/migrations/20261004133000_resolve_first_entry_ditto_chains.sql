-- Resolve ditto chains starting at entry_index=0 by backtracking to previous page.
--
-- Problem: When entry_index=0 contains a ditto mark, subsequent entries form
-- unresolvable chains (all entries point to a ditto mark, not a real value).
--
-- Solution: For entry_index=0 dittos, look at the previous page's last entry
-- value for the same field. If no previous page exists, leave unresolved (mark
-- for manual review).

CREATE OR REPLACE FUNCTION get_previous_page_value(
  p_page_id BIGINT,
  p_field_name TEXT
) RETURNS TEXT AS $$
DECLARE
  v_prev_value TEXT;
BEGIN
  -- Get the previous page's number
  SELECT COALESCE(
    (SELECT MAX(p2.page_no)
     FROM pages p2
     WHERE p2.page_no < (SELECT page_no FROM pages WHERE id = p_page_id)
       AND p2.pcloud_fileid = (SELECT pcloud_fileid FROM pages WHERE id = p_page_id)),
    NULL
  ) INTO v_prev_value;

  IF v_prev_value IS NULL THEN
    RETURN NULL;  -- No previous page
  END IF;

  -- Get the last entry's value from the previous page
  EXECUTE format(
    'SELECT %I FROM catalogue_entries ce
     WHERE ce.extraction_id IN (
       SELECT le.id FROM llm_extractions le
       WHERE le.page_id = (
         SELECT id FROM pages
         WHERE page_no = %L
           AND pcloud_fileid = (SELECT pcloud_fileid FROM pages WHERE id = %L)
       )
       ORDER BY le.id DESC
       LIMIT 1
     )
     ORDER BY ce.entry_index DESC
     LIMIT 1',
    p_field_name, v_prev_value, p_page_id
  ) INTO v_prev_value;

  RETURN v_prev_value;
END;
$$ LANGUAGE plpgsql;

-- Update existing first-entry dittos using previous page values
UPDATE catalogue_entries ce
SET
  author = CASE
    WHEN LOWER(ce.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    THEN get_previous_page_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'author')
    ELSE ce.author
  END,
  printer = CASE
    WHEN LOWER(ce.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    THEN get_previous_page_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'printer')
    ELSE ce.printer
  END,
  pcity = CASE
    WHEN LOWER(ce.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    THEN get_previous_page_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'pcity')
    ELSE ce.pcity
  END,
  publisher = CASE
    WHEN LOWER(ce.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    THEN get_previous_page_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'publisher')
    ELSE ce.publisher
  END,
  pubcity = CASE
    WHEN LOWER(ce.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    THEN get_previous_page_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'pubcity')
    ELSE ce.pubcity
  END
WHERE ce.entry_index = 0
  AND (
    LOWER(ce.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(ce.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(ce.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(ce.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(ce.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
  );

-- Update the BEFORE INSERT trigger to handle entry_index=0 dittos with backtracking
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
  v_extraction_id := NEW.extraction_id;

  SELECT page_id INTO v_page_id
  FROM llm_extractions
  WHERE id = v_extraction_id;

  IF NEW.entry_index = 0 THEN
    -- First entry with ditto: backtrack to previous page's last entry
    SELECT author, printer, pcity, publisher, pubcity
    INTO v_prev_author, v_prev_printer, v_prev_pcity, v_prev_publisher, v_prev_pubcity
    FROM catalogue_entries ce
    WHERE ce.extraction_id IN (
      SELECT le.id FROM llm_extractions le
      WHERE le.page_id = (
        SELECT id FROM pages p2
        WHERE p2.page_no = (SELECT page_no - 1 FROM pages WHERE id = v_page_id)
          AND p2.pcloud_fileid = (SELECT pcloud_fileid FROM pages WHERE id = v_page_id)
      )
      ORDER BY le.id DESC
      LIMIT 1
    )
    ORDER BY ce.entry_index DESC
    LIMIT 1;

    -- Resolve dittos using previous page's values (may still be NULL if no previous page)
    IF NEW.author IS NOT NULL AND LOWER(NEW.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.author := v_prev_author;
    END IF;
    IF NEW.printer IS NOT NULL AND LOWER(NEW.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.printer := v_prev_printer;
    END IF;
    IF NEW.pcity IS NOT NULL AND LOWER(NEW.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.pcity := v_prev_pcity;
    END IF;
    IF NEW.publisher IS NOT NULL AND LOWER(NEW.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.publisher := v_prev_publisher;
    END IF;
    IF NEW.pubcity IS NOT NULL AND LOWER(NEW.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.pubcity := v_prev_pubcity;
    END IF;

  ELSE
    -- Subsequent entries: look at immediate predecessor (entry_index - 1)
    SELECT author, printer, pcity, publisher, pubcity
    INTO v_prev_author, v_prev_printer, v_prev_pcity, v_prev_publisher, v_prev_pubcity
    FROM catalogue_entries
    WHERE extraction_id = v_extraction_id
      AND entry_index = NEW.entry_index - 1
    LIMIT 1;

    IF NEW.author IS NOT NULL AND LOWER(NEW.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.author := v_prev_author;
    END IF;
    IF NEW.printer IS NOT NULL AND LOWER(NEW.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.printer := v_prev_printer;
    END IF;
    IF NEW.pcity IS NOT NULL AND LOWER(NEW.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.pcity := v_prev_pcity;
    END IF;
    IF NEW.publisher IS NOT NULL AND LOWER(NEW.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.publisher := v_prev_publisher;
    END IF;
    IF NEW.pubcity IS NOT NULL AND LOWER(NEW.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-') THEN
      NEW.pubcity := v_prev_pubcity;
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Recreate trigger with updated function
DROP TRIGGER IF EXISTS resolve_ditto_marks_trigger ON catalogue_entries;
CREATE TRIGGER resolve_ditto_marks_trigger
BEFORE INSERT ON catalogue_entries
FOR EACH ROW
EXECUTE FUNCTION resolve_ditto_marks_on_page();

-- Re-apply recursive CTE for post-migration resolution of chains that now start
-- with proper values (since we resolved entry_index=0)
WITH RECURSIVE ditto_resolution AS (
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

-- Log summary of changes
DO $$
DECLARE
  v_unresolved INT;
BEGIN
  SELECT COUNT(*) INTO v_unresolved
  FROM catalogue_entries
  WHERE (LOWER(author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-'));

  RAISE NOTICE 'Ditto chain resolution complete. Remaining unresolved dittos: %', v_unresolved;
END $$;
