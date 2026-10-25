-- Generic value-normalization lookup: one table for every field, not one per field.
--
-- Maps each distinct raw value found in catalogue_entries_canonical.<field> to a
-- single canonical spelling (e.g. lang: "Urdu", "Urdú", "URDU", "Urdū." -> "Urdu").
-- The raw value is never overwritten; the review page/reports show
-- canonical_value and keep the as-printed raw_value available.
--
--  * norm_key: raw value with accents, punctuation and case removed -- values
--    sharing a (field, norm_key) are auto-grouped under one canonical value.
--  * status: 'auto'     = grouped by norm_key (default, can be overridden)
--            'approved' = a human confirmed or set canonical_value
--  Existing rows are never overwritten by the auto-mapper, so manual edits stick.
--
-- Adding another field later = add it to the array in map_new_field_values().

create table field_value_map (
    id bigint generated always as identity primary key,
    field text not null,
    raw_value text not null,
    norm_key text not null,
    canonical_value text not null,
    status text not null default 'auto' check (status in ('auto', 'approved')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (field, raw_value)
);

create index field_value_map_field_norm_idx on field_value_map (field, norm_key);
create index field_value_map_canonical_idx on field_value_map (field, canonical_value);


create or replace function normalize_value_key(v text)
returns text as $$
    SELECT lower(btrim(regexp_replace(
        regexp_replace(
            regexp_replace(normalize(v, NFKD), '[̀-ͯ]', '', 'g'),
            '[[:punct:]]+', ' ', 'g'),
        '\s+', ' ', 'g')))
$$ LANGUAGE SQL IMMUTABLE;


-- Add any not-yet-mapped values (for one page, or all pages) to field_value_map.
-- A new spelling joins an existing group if its norm_key matches; otherwise the
-- most frequent spelling among the new values becomes the group's canonical value.
create or replace function map_new_field_values(p_page_id bigint DEFAULT NULL)
returns void as $$
DECLARE
    f text;
    -- fields that get value mapping
    fields text[] := ARRAY['lang', 'pubcity', 'pcity', 'publisher', 'printer'];
BEGIN
    FOREACH f IN ARRAY fields LOOP
        EXECUTE format($q$
            WITH vals AS (
                SELECT %1$I AS raw, count(*) AS n
                FROM catalogue_entries_canonical
                WHERE %1$I IS NOT NULL AND btrim(%1$I) <> ''
                  AND (%2$L IS NULL OR page_id = %2$L)
                GROUP BY %1$I
            ),
            fresh AS (
                SELECT v.raw, v.n, normalize_value_key(v.raw) AS k
                FROM vals v
                WHERE NOT EXISTS (
                    SELECT 1 FROM field_value_map m WHERE m.field = %3$L AND m.raw_value = v.raw
                )
            )
            INSERT INTO field_value_map (field, raw_value, norm_key, canonical_value)
            SELECT %3$L, fr.raw, fr.k,
                   COALESCE(
                       (SELECT m.canonical_value FROM field_value_map m
                        WHERE m.field = %3$L AND m.norm_key = fr.k
                        ORDER BY (m.status = 'approved') DESC, m.id LIMIT 1),
                       first_value(fr.raw) OVER (
                           PARTITION BY fr.k ORDER BY fr.n DESC, length(fr.raw), fr.raw)
                   )
            FROM fresh fr
            WHERE fr.k <> ''
            ON CONFLICT (field, raw_value) DO NOTHING
        $q$, f, p_page_id, f);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- sync_canonical_entries now also maps the new values it copied.
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

    PERFORM map_new_field_values(p_page_id);
END;
$$ LANGUAGE plpgsql;


-- Backfill: map every value already in the canonical table (most frequent
-- spelling per group wins).
SELECT map_new_field_values();
