-- Drop page_extractions_canonical.raw_response.
--
-- It was a page-level JSON copy of the winning extraction's parsed entries, which
-- catalogue_entries_canonical already holds field by field. Nothing reads it, and it
-- cost ~20 MB (more with dead rows from repeated syncs). The raw model output itself
-- (llm_extractions.raw_text) was never copied to the canonical layer.
--
-- sync_canonical_entries() is replaced first so it no longer writes the column.
-- DROP COLUMN only marks the column dropped; run the "Vacuum full" workflow afterwards
-- to rewrite the table and return the space.

create or replace function sync_canonical_entries(p_page_id bigint DEFAULT NULL)
returns void as $$
BEGIN
    UPDATE page_extractions_canonical pec
    SET entry_count = (SELECT COUNT(*) FROM catalogue_entries ce WHERE ce.extraction_id = pec.extraction_id)
    WHERE pec.extraction_id IS NOT NULL
      AND (p_page_id IS NULL OR pec.page_id = p_page_id);

    DELETE FROM catalogue_entries_canonical cec
    WHERE (p_page_id IS NULL OR cec.page_id = p_page_id);

    INSERT INTO catalogue_entries_canonical (
        page_id, entry_index, source_extraction_id, model_tag, priority,
        quarter, pdf_page, printed_page, section, lang, char_qualifier, topic,
        serial, reg, copies, printer_verbatim, printer, pcity, author, title,
        title_native, gloss, pp_verbatim, publisher, pubcity, date, price,
        edition, format, method, educ, copyright, notes, marks, flags,
        source_folder, source_pdf
    )
    SELECT
        pec.page_id, ce.entry_index, pec.extraction_id, pec.model_tag, pec.priority,
        ce.quarter, ce.pdf_page, ce.printed_page, ce.section, ce.lang, ce.char_qualifier, ce.topic,
        ce.serial, ce.reg, ce.copies, ce.printer_verbatim, ce.printer, ce.pcity, ce.author, ce.title,
        ce.title_native, ce.gloss, ce.pp_verbatim, ce.publisher, ce.pubcity, ce.date, ce.price,
        ce.edition, ce.format, ce.method, ce.educ, ce.copyright, ce.notes, ce.marks, ce.flags,
        ce.source_folder, ce.source_pdf
    FROM page_extractions_canonical pec
    JOIN catalogue_entries ce ON ce.extraction_id = pec.extraction_id
    WHERE (p_page_id IS NULL OR pec.page_id = p_page_id);

    PERFORM map_new_field_values(p_page_id);
    PERFORM apply_catalogue_dates(p_page_id);
    PERFORM infer_catalogue_dates(p_page_id);
END;
$$ LANGUAGE plpgsql;

alter table page_extractions_canonical drop column if exists raw_response;
