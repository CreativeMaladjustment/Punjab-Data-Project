-- Resolve ditto chains starting at entry_index=0 by backtracking to previous page.
--
-- Problem: When entry_index=0 contains a ditto mark, subsequent entries form
-- unresolvable chains (all entries point to a ditto mark, not a real value).
--
-- Solution: For entry_index=0 dittos, look at the previous page's successful
-- extraction's last entry value for the same field. Iteratively resolve until
-- no more changes occur to handle multi-page chains correctly.
--
-- Fixes all ditto fields: author, title, date, printer, pcity, publisher, pubcity

-- Helper: Get predecessor page number (skip missing pages)
CREATE OR REPLACE FUNCTION get_predecessor_page_no(p_page_id BIGINT)
RETURNS BIGINT AS $$
  SELECT MAX(p2.page_no)
  FROM pages p2
  WHERE p2.page_no < (SELECT page_no FROM pages WHERE id = p_page_id)
    AND p2.pcloud_fileid = (SELECT pcloud_fileid FROM pages WHERE id = p_page_id);
$$ LANGUAGE SQL;

-- Helper: Get the last entry's value from previous page (successful extraction)
CREATE OR REPLACE FUNCTION get_previous_page_field_value(
  p_page_id BIGINT,
  p_field_name TEXT
) RETURNS TEXT AS $$
DECLARE
  v_pred_page_no BIGINT;
  v_result TEXT;
BEGIN
  v_pred_page_no := get_predecessor_page_no(p_page_id);

  IF v_pred_page_no IS NULL THEN
    RETURN NULL;  -- No previous page
  END IF;

  -- Get the last entry's value from the previous page's SUCCESSFUL extraction
  EXECUTE format(
    'SELECT %I FROM catalogue_entries ce
     WHERE ce.extraction_id IN (
       SELECT le.id FROM llm_extractions le
       WHERE le.page_id = (
         SELECT id FROM pages
         WHERE page_no = %L
           AND pcloud_fileid = (SELECT pcloud_fileid FROM pages WHERE id = %L)
       )
       AND le.status = %L
       ORDER BY le.created_at DESC
       LIMIT 1
     )
     ORDER BY ce.entry_index DESC
     LIMIT 1',
    p_field_name, v_pred_page_no, p_page_id, 'success'
  ) INTO v_result;

  RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Iteratively resolve first-entry dittos until fixed point (no more changes)
DO $$
DECLARE
  v_updates_this_round INT;
  v_iteration INT := 0;
  v_max_iterations INT := 10;  -- Prevent infinite loops
BEGIN
  LOOP
    v_iteration := v_iteration + 1;
    IF v_iteration > v_max_iterations THEN
      RAISE WARNING 'Ditto resolution reached max iterations (%), stopping', v_max_iterations;
      EXIT;
    END IF;

    -- Update first-entry dittos using previous page values
    -- Use COALESCE to preserve original ditto mark if lookup fails
    UPDATE catalogue_entries ce
    SET
      author = CASE
        WHEN LOWER(ce.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'author'), ce.author)
        ELSE ce.author
      END,
      title = CASE
        WHEN LOWER(ce.title) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'title'), ce.title)
        ELSE ce.title
      END,
      date = CASE
        WHEN LOWER(ce.date) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'date'), ce.date)
        ELSE ce.date
      END,
      printer = CASE
        WHEN LOWER(ce.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'printer'), ce.printer)
        ELSE ce.printer
      END,
      pcity = CASE
        WHEN LOWER(ce.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'pcity'), ce.pcity)
        ELSE ce.pcity
      END,
      publisher = CASE
        WHEN LOWER(ce.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'publisher'), ce.publisher)
        ELSE ce.publisher
      END,
      pubcity = CASE
        WHEN LOWER(ce.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        THEN COALESCE(get_previous_page_field_value((SELECT page_id FROM llm_extractions WHERE id = ce.extraction_id), 'pubcity'), ce.pubcity)
        ELSE ce.pubcity
      END
    WHERE ce.entry_index = 0
      AND (
        LOWER(ce.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        OR LOWER(ce.title) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        OR LOWER(ce.date) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        OR LOWER(ce.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        OR LOWER(ce.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        OR LOWER(ce.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
        OR LOWER(ce.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      );

    GET DIAGNOSTICS v_updates_this_round = ROW_COUNT;

    -- Now apply within-page recursive resolution
    WITH RECURSIVE ditto_resolution AS (
      -- Base: entry_index = 0 (now potentially resolved from previous page)
      SELECT
        ce.id,
        ce.extraction_id,
        ce.entry_index,
        ce.author as resolved_author,
        ce.title as resolved_title,
        ce.date as resolved_date,
        ce.printer as resolved_printer,
        ce.pcity as resolved_pcity,
        ce.publisher as resolved_publisher,
        ce.pubcity as resolved_pubcity
      FROM catalogue_entries ce
      WHERE ce.entry_index = 0
      UNION ALL
      -- Recursive: subsequent entries use previous entry's RESOLVED values
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
          WHEN ce.title IS NOT NULL AND LOWER(ce.title) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
          THEN dr.resolved_title
          ELSE ce.title
        END as resolved_title,
        CASE
          WHEN ce.date IS NOT NULL AND LOWER(ce.date) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
          THEN dr.resolved_date
          ELSE ce.date
        END as resolved_date,
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
      title = dr.resolved_title,
      date = dr.resolved_date,
      printer = dr.resolved_printer,
      pcity = dr.resolved_pcity,
      publisher = dr.resolved_publisher,
      pubcity = dr.resolved_pubcity
    FROM ditto_resolution dr
    WHERE ce.id = dr.id
      AND (
        ce.author IS DISTINCT FROM dr.resolved_author OR
        ce.title IS DISTINCT FROM dr.resolved_title OR
        ce.date IS DISTINCT FROM dr.resolved_date OR
        ce.printer IS DISTINCT FROM dr.resolved_printer OR
        ce.pcity IS DISTINCT FROM dr.resolved_pcity OR
        ce.publisher IS DISTINCT FROM dr.resolved_publisher OR
        ce.pubcity IS DISTINCT FROM dr.resolved_pubcity
      );

    GET DIAGNOSTICS v_updates_this_round = ROW_COUNT;

    -- Exit loop if no changes in this round (fixed point reached)
    IF v_updates_this_round = 0 THEN
      RAISE NOTICE 'Ditto chain resolution reached fixed point after % iterations', v_iteration;
      EXIT;
    END IF;
  END LOOP;
END $$;

-- Update the BEFORE INSERT trigger to handle all ditto fields correctly
CREATE OR REPLACE FUNCTION resolve_ditto_marks_on_page()
RETURNS TRIGGER AS $$
DECLARE
  v_extraction_id BIGINT;
  v_page_id BIGINT;
  v_pred_page_no BIGINT;
  v_prev_author TEXT;
  v_prev_title TEXT;
  v_prev_date TEXT;
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
    -- First entry: backtrack to previous page's successful extraction
    v_pred_page_no := get_predecessor_page_no(v_page_id);

    IF v_pred_page_no IS NOT NULL THEN
      SELECT author, title, date, printer, pcity, publisher, pubcity
      INTO v_prev_author, v_prev_title, v_prev_date, v_prev_printer, v_prev_pcity, v_prev_publisher, v_prev_pubcity
      FROM catalogue_entries ce
      WHERE ce.extraction_id IN (
        SELECT le.id FROM llm_extractions le
        WHERE le.page_id = (
          SELECT id FROM pages p2
          WHERE p2.page_no = v_pred_page_no
            AND p2.pcloud_fileid = (SELECT pcloud_fileid FROM pages WHERE id = v_page_id)
        )
        AND le.status = 'success'
        ORDER BY le.created_at DESC
        LIMIT 1
      )
      ORDER BY ce.entry_index DESC
      LIMIT 1;
    END IF;

    -- Resolve dittos, preserving originals if lookup fails
    NEW.author := CASE
      WHEN NEW.author IS NOT NULL AND LOWER(NEW.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_author, NEW.author)
      ELSE NEW.author
    END;

    NEW.title := CASE
      WHEN NEW.title IS NOT NULL AND LOWER(NEW.title) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_title, NEW.title)
      ELSE NEW.title
    END;

    NEW.date := CASE
      WHEN NEW.date IS NOT NULL AND LOWER(NEW.date) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_date, NEW.date)
      ELSE NEW.date
    END;

    NEW.printer := CASE
      WHEN NEW.printer IS NOT NULL AND LOWER(NEW.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_printer, NEW.printer)
      ELSE NEW.printer
    END;

    NEW.pcity := CASE
      WHEN NEW.pcity IS NOT NULL AND LOWER(NEW.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_pcity, NEW.pcity)
      ELSE NEW.pcity
    END;

    NEW.publisher := CASE
      WHEN NEW.publisher IS NOT NULL AND LOWER(NEW.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_publisher, NEW.publisher)
      ELSE NEW.publisher
    END;

    NEW.pubcity := CASE
      WHEN NEW.pubcity IS NOT NULL AND LOWER(NEW.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_pubcity, NEW.pubcity)
      ELSE NEW.pubcity
    END;

  ELSE
    -- Subsequent entries: look at immediate predecessor (entry_index - 1)
    SELECT author, title, date, printer, pcity, publisher, pubcity
    INTO v_prev_author, v_prev_title, v_prev_date, v_prev_printer, v_prev_pcity, v_prev_publisher, v_prev_pubcity
    FROM catalogue_entries
    WHERE extraction_id = v_extraction_id
      AND entry_index = NEW.entry_index - 1
    LIMIT 1;

    -- Resolve dittos using predecessor values (which are already resolved from previous iterations)
    NEW.author := CASE
      WHEN NEW.author IS NOT NULL AND LOWER(NEW.author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_author, NEW.author)
      ELSE NEW.author
    END;

    NEW.title := CASE
      WHEN NEW.title IS NOT NULL AND LOWER(NEW.title) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_title, NEW.title)
      ELSE NEW.title
    END;

    NEW.date := CASE
      WHEN NEW.date IS NOT NULL AND LOWER(NEW.date) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_date, NEW.date)
      ELSE NEW.date
    END;

    NEW.printer := CASE
      WHEN NEW.printer IS NOT NULL AND LOWER(NEW.printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_printer, NEW.printer)
      ELSE NEW.printer
    END;

    NEW.pcity := CASE
      WHEN NEW.pcity IS NOT NULL AND LOWER(NEW.pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_pcity, NEW.pcity)
      ELSE NEW.pcity
    END;

    NEW.publisher := CASE
      WHEN NEW.publisher IS NOT NULL AND LOWER(NEW.publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_publisher, NEW.publisher)
      ELSE NEW.publisher
    END;

    NEW.pubcity := CASE
      WHEN NEW.pubcity IS NOT NULL AND LOWER(NEW.pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
      THEN COALESCE(v_prev_pubcity, NEW.pubcity)
      ELSE NEW.pubcity
    END;
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

-- Log final summary
DO $$
DECLARE
  v_unresolved INT;
BEGIN
  SELECT COUNT(*) INTO v_unresolved
  FROM catalogue_entries
  WHERE (LOWER(author) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(title) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(date) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(printer) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(pcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(publisher) IN ('ditto', 'ditto.', 'do', 'do.', '-do-')
    OR LOWER(pubcity) IN ('ditto', 'ditto.', 'do', 'do.', '-do-'));

  RAISE NOTICE 'Ditto resolution complete. All 7 fields processed. Remaining unresolved dittos: %', v_unresolved;
END $$;
