-- Make the canonical layer a self-contained COPY of the winning extraction,
-- not a set of pointers into llm_extractions / catalogue_entries.
--
--  * catalogue_entries_canonical holds a full copy of every catalogue entry
--    of each page's winning extraction (same content columns as
--    catalogue_entries), kept in sync by triggers.
--  * page_extractions_canonical gets a copy of the winning raw_response, and
--    its extraction_id becomes a provenance-only hint (set null if the source
--    row is ever deleted) instead of a cascading dependency.

create table catalogue_entries_canonical (
    id bigint generated always as identity primary key,
    page_id bigint not null references pages (id) on delete cascade,
    entry_index int not null,
    -- provenance only: which extraction this copy was taken from
    source_extraction_id bigint,
    model_tag text not null,
    priority int not null,
    quarter text,
    pdf_page int,
    printed_page int,
    section text,
    lang text,
    char_qualifier text,
    topic text,
    serial int,
    reg text,
    copies text,
    printer_verbatim text,
    printer text,
    pcity text,
    author text,
    title text,
    title_native boolean,
    gloss text,
    pp_verbatim text,
    publisher text,
    pubcity text,
    date text,
    price text,
    edition text,
    format text,
    method text,
    educ text,
    copyright text,
    notes text,
    marks text,
    flags jsonb,
    source_folder text,
    source_pdf text,
    created_at timestamptz not null default now(),
    unique (page_id, entry_index)
);

create index catalogue_entries_canonical_page_id_idx on catalogue_entries_canonical (page_id);
create index catalogue_entries_canonical_source_idx on catalogue_entries_canonical (source_extraction_id);

-- Page-level copy: raw winning response, and a non-cascading provenance link.
alter table page_extractions_canonical add column raw_response jsonb;
alter table page_extractions_canonical alter column extraction_id drop not null;
alter table page_extractions_canonical drop constraint page_extractions_canonical_extraction_id_fkey;
alter table page_extractions_canonical
    add constraint page_extractions_canonical_extraction_id_fkey
    foreign key (extraction_id) references llm_extractions (id) on delete set null;


-- Re-copy the winning extraction's content for one page (or all pages).
create or replace function sync_canonical_entries(p_page_id bigint DEFAULT NULL)
returns void as $$
BEGIN
    UPDATE page_extractions_canonical pec
    SET raw_response = le.raw_response,
        entry_count = (SELECT COUNT(*) FROM catalogue_entries ce WHERE ce.extraction_id = pec.extraction_id)
    FROM llm_extractions le
    WHERE le.id = pec.extraction_id
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
END;
$$ LANGUAGE plpgsql;


-- refresh_canonical_extractions now also copies the content, and records the
-- winning raw_response.
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

    DELETE FROM page_extractions_canonical pec
    WHERE (p_page_id IS NULL OR pec.page_id = p_page_id)
        AND NOT EXISTS (
            SELECT 1 FROM llm_extractions le
            WHERE le.page_id = pec.page_id AND le.status = 'success'
        );

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    PERFORM sync_canonical_entries(p_page_id);

    RETURN QUERY SELECT v_updated, v_inserted, v_deleted;
END;
$$ LANGUAGE plpgsql;


-- Keep the copy current when catalogue entries of a winning extraction change.
-- Statement-level with transition tables: one resync per affected page, not per row.
drop trigger if exists trigger_catalogue_entry_count on catalogue_entries;
drop function if exists trigger_update_canonical_entry_count();

create or replace function trigger_sync_canonical_from_entries_ins()
returns trigger as $$
BEGIN
    PERFORM sync_canonical_entries(pec.page_id)
    FROM page_extractions_canonical pec
    WHERE pec.extraction_id IN (SELECT DISTINCT extraction_id FROM new_rows);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

create or replace function trigger_sync_canonical_from_entries_del()
returns trigger as $$
BEGIN
    PERFORM sync_canonical_entries(pec.page_id)
    FROM page_extractions_canonical pec
    WHERE pec.extraction_id IN (SELECT DISTINCT extraction_id FROM old_rows);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

create trigger trigger_canonical_entries_ins
after insert on catalogue_entries
referencing new table as new_rows
for each statement execute function trigger_sync_canonical_from_entries_ins();

create trigger trigger_canonical_entries_upd
after update on catalogue_entries
referencing new table as new_rows
for each statement execute function trigger_sync_canonical_from_entries_ins();

create trigger trigger_canonical_entries_del
after delete on catalogue_entries
referencing old table as old_rows
for each statement execute function trigger_sync_canonical_from_entries_del();


-- Backfill the copy for everything already canonical.
SELECT sync_canonical_entries();
