-- Store parsed/inferred dates on catalogue_entries_canonical and expose them
-- in canonical_entries_view. See 20261027100000 for the column meanings.

alter table catalogue_entries_canonical
    add column date_start date,
    add column date_end date,
    add column date_iso text,
    add column date_precision text,
    add column date_era text,
    add column date_status text,
    add column date_basis text,
    add column date_note text,
    add column date_part_month int,
    add column date_part_day int,
    add column date_est_from date,
    add column date_est_to date;

create index catalogue_entries_canonical_date_status_idx on catalogue_entries_canonical (date_status);
-- neighbour lookups walk a PDF in page order
create index catalogue_entries_canonical_pdf_order_idx
    on catalogue_entries_canonical (source_folder, source_pdf, pdf_page, entry_index)
    where date_status = 'ok';


-- Parse every entry's printed date, using its PDF's year span to settle
-- two-digit years and implausible years.
create or replace function apply_catalogue_dates(p_page_id bigint DEFAULT NULL)
returns void as $$
BEGIN
    UPDATE catalogue_entries_canonical c
    SET date_start = p.date_start,
        date_end = p.date_end,
        date_iso = p.date_iso,
        date_precision = p.precision,
        date_era = p.era,
        date_status = p.status,
        date_basis = CASE WHEN p.date_start IS NOT NULL THEN 'printed' END,
        date_note = p.note,
        date_part_month = p.part_month,
        date_part_day = p.part_day,
        date_est_from = NULL,
        date_est_to = NULL
    FROM catalogue_entries_canonical src
    CROSS JOIN LATERAL catalogue_pdf_year_window(src.source_pdf) w
    CROSS JOIN LATERAL parse_catalogue_date(src.date, w.y_from, w.y_to) p
    WHERE src.id = c.id
      AND (p_page_id IS NULL OR c.page_id = p_page_id);
END;
$$ LANGUAGE plpgsql;


-- Fill in what the printed date didn't give us:
--   1. partial dates ("April 10"): pick the year closest to the nearest dated
--      neighbour in the same PDF (or the PDF's year if it is a single year)
--   2. "ditto" dates: copy the previous dated entry
--   3. entries with no usable date: an estimated window from the explicit
--      quarter, else the dated neighbours before/after in the PDF, else the
--      PDF's year span.
create or replace function infer_catalogue_dates(p_page_id bigint DEFAULT NULL)
returns void as $$
BEGIN
    -- 1. partial dates: year from neighbours / PDF
    UPDATE catalogue_entries_canonical c
    SET date_start = g.d,
        date_end = CASE WHEN c.date_part_day IS NULL THEN (g.d + interval '1 month - 1 day')::date ELSE g.d END,
        date_precision = CASE WHEN c.date_part_day IS NULL THEN 'month' ELSE 'day' END,
        date_iso = CASE WHEN c.date_part_day IS NULL THEN to_char(g.d, 'YYYY-MM') ELSE to_char(g.d, 'YYYY-MM-DD') END,
        date_status = 'inferred',
        date_basis = g.basis,
        date_note = 'year taken from ' || g.basis
    FROM (
        SELECT c2.id,
               COALESCE(
                   (SELECT x.d FROM (
                        SELECT catalogue_safe_date(y, c2.date_part_month, COALESCE(c2.date_part_day, 1)) AS d
                        FROM generate_series(
                                 extract(year FROM COALESCE(pv.date_start, nx.date_start))::int - 1,
                                 extract(year FROM COALESCE(pv.date_start, nx.date_start))::int + 1) y
                    ) x
                    WHERE x.d IS NOT NULL
                    ORDER BY abs(x.d - COALESCE(pv.date_start, nx.date_start)) LIMIT 1),
                   CASE WHEN w.y_from IS NOT NULL AND w.y_from = w.y_to
                        THEN catalogue_safe_date(w.y_from, c2.date_part_month, COALESCE(c2.date_part_day, 1)) END
               ) AS d,
               CASE WHEN pv.date_start IS NOT NULL OR nx.date_start IS NOT NULL THEN 'neighbors' ELSE 'year_window' END AS basis
        FROM catalogue_entries_canonical c2
        CROSS JOIN LATERAL catalogue_pdf_year_window(c2.source_pdf) w
        LEFT JOIN LATERAL (
            SELECT pr.date_start FROM catalogue_entries_canonical pr
            WHERE pr.source_pdf = c2.source_pdf AND pr.source_folder IS NOT DISTINCT FROM c2.source_folder
              AND pr.date_status = 'ok' AND pr.date_precision IN ('day', 'month')
              AND (pr.pdf_page, pr.entry_index) < (c2.pdf_page, c2.entry_index)
            ORDER BY pr.pdf_page DESC, pr.entry_index DESC LIMIT 1
        ) pv ON true
        LEFT JOIN LATERAL (
            SELECT nx2.date_start FROM catalogue_entries_canonical nx2
            WHERE nx2.source_pdf = c2.source_pdf AND nx2.source_folder IS NOT DISTINCT FROM c2.source_folder
              AND nx2.date_status = 'ok' AND nx2.date_precision IN ('day', 'month')
              AND (nx2.pdf_page, nx2.entry_index) > (c2.pdf_page, c2.entry_index)
            ORDER BY nx2.pdf_page ASC, nx2.entry_index ASC LIMIT 1
        ) nx ON true
        WHERE c2.date_status = 'needs_year' AND c2.date_part_month IS NOT NULL
          AND (p_page_id IS NULL OR c2.page_id = p_page_id)
    ) g
    WHERE c.id = g.id AND g.d IS NOT NULL;

    -- 2. ditto: copy the previous dated entry (a printed day keeps that month/year)
    UPDATE catalogue_entries_canonical c
    SET date_start = CASE WHEN c.date_part_day IS NULL THEN pv.date_start
                          ELSE catalogue_safe_date(extract(year FROM pv.date_start)::int,
                                                   extract(month FROM pv.date_start)::int, c.date_part_day) END,
        date_end = CASE WHEN c.date_part_day IS NULL THEN pv.date_end
                        ELSE catalogue_safe_date(extract(year FROM pv.date_start)::int,
                                                 extract(month FROM pv.date_start)::int, c.date_part_day) END,
        date_precision = CASE WHEN c.date_part_day IS NULL THEN pv.date_precision ELSE 'day' END,
        date_iso = CASE WHEN c.date_part_day IS NULL THEN pv.date_iso
                        ELSE to_char(catalogue_safe_date(extract(year FROM pv.date_start)::int,
                                                         extract(month FROM pv.date_start)::int, c.date_part_day), 'YYYY-MM-DD') END,
        date_status = 'inferred',
        date_basis = 'ditto',
        date_note = 'copied from previous entry'
    FROM catalogue_entries_canonical c2
    CROSS JOIN LATERAL (
        SELECT pr.date_start, pr.date_end, pr.date_precision, pr.date_iso
        FROM catalogue_entries_canonical pr
        WHERE pr.source_pdf = c2.source_pdf AND pr.source_folder IS NOT DISTINCT FROM c2.source_folder
          AND pr.date_status = 'ok' AND pr.date_precision IN ('day', 'month')
          AND (pr.pdf_page, pr.entry_index) < (c2.pdf_page, c2.entry_index)
        ORDER BY pr.pdf_page DESC, pr.entry_index DESC LIMIT 1
    ) pv
    WHERE c2.id = c.id AND c.date_status = 'ditto'
      AND (c.date_part_day IS NULL OR catalogue_safe_date(extract(year FROM pv.date_start)::int,
                                                           extract(month FROM pv.date_start)::int, c.date_part_day) IS NOT NULL)
      AND (p_page_id IS NULL OR c.page_id = p_page_id);

    -- 3. estimated window for entries still without a date
    UPDATE catalogue_entries_canonical c
    SET date_est_from = e.est_from,
        date_est_to = e.est_to,
        date_basis = e.basis
    FROM (
        SELECT c2.id,
               CASE
                   WHEN q.qf IS NOT NULL THEN q.qf
                   WHEN pv.date_start IS NOT NULL AND nx.date_start IS NOT NULL THEN
                        GREATEST(LEAST(pv.date_start, nx.date_start), COALESCE(make_date(w.y_from, 1, 1), '1800-01-01'))
                   WHEN COALESCE(pv.date_start, nx.date_start) IS NOT NULL THEN
                        GREATEST((COALESCE(pv.date_start, nx.date_start) - interval '3 months')::date, COALESCE(make_date(w.y_from, 1, 1), '1800-01-01'))
                   ELSE make_date(w.y_from, 1, 1)
               END AS est_from,
               CASE
                   WHEN q.qt IS NOT NULL THEN q.qt
                   WHEN pv.date_start IS NOT NULL AND nx.date_start IS NOT NULL THEN
                        LEAST(GREATEST(pv.date_end, nx.date_end), COALESCE(make_date(w.y_to, 12, 31), '1950-12-31'))
                   WHEN COALESCE(pv.date_start, nx.date_start) IS NOT NULL THEN
                        LEAST((COALESCE(pv.date_end, nx.date_end) + interval '3 months')::date, COALESCE(make_date(w.y_to, 12, 31), '1950-12-31'))
                   ELSE make_date(w.y_to, 12, 31)
               END AS est_to,
               CASE
                   WHEN q.qf IS NOT NULL THEN 'quarter'
                   WHEN pv.date_start IS NOT NULL OR nx.date_start IS NOT NULL THEN 'neighbors'
                   ELSE 'year_window'
               END AS basis
        FROM catalogue_entries_canonical c2
        CROSS JOIN LATERAL catalogue_pdf_year_window(c2.source_pdf) w
        LEFT JOIN LATERAL (
            SELECT make_date(m[1]::int, (m[2]::int - 1) * 3 + 1, 1) AS qf,
                   (make_date(m[1]::int, (m[2]::int - 1) * 3 + 1, 1) + interval '3 months - 1 day')::date AS qt
            FROM (SELECT regexp_match(c2.quarter, '^\s*(\d{4})\s*Q([1-4])\s*$') AS m) qm
            WHERE m IS NOT NULL
        ) q ON true
        LEFT JOIN LATERAL (
            SELECT pr.date_start, pr.date_end FROM catalogue_entries_canonical pr
            WHERE pr.source_pdf = c2.source_pdf AND pr.source_folder IS NOT DISTINCT FROM c2.source_folder
              AND pr.date_status = 'ok' AND pr.date_precision IN ('day', 'month')
              AND (pr.pdf_page, pr.entry_index) < (c2.pdf_page, c2.entry_index)
            ORDER BY pr.pdf_page DESC, pr.entry_index DESC LIMIT 1
        ) pv ON true
        LEFT JOIN LATERAL (
            SELECT nx2.date_start, nx2.date_end FROM catalogue_entries_canonical nx2
            WHERE nx2.source_pdf = c2.source_pdf AND nx2.source_folder IS NOT DISTINCT FROM c2.source_folder
              AND nx2.date_status = 'ok' AND nx2.date_precision IN ('day', 'month')
              AND (nx2.pdf_page, nx2.entry_index) > (c2.pdf_page, c2.entry_index)
            ORDER BY nx2.pdf_page ASC, nx2.entry_index ASC LIMIT 1
        ) nx ON true
        WHERE c2.date_start IS NULL
          AND (p_page_id IS NULL OR c2.page_id = p_page_id)
    ) e
    WHERE c.id = e.id AND e.est_from IS NOT NULL AND e.est_to IS NOT NULL;
END;
$$ LANGUAGE plpgsql;


-- sync_canonical_entries: after copying and mapping values, parse and infer dates.
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
    PERFORM apply_catalogue_dates(p_page_id);
    PERFORM infer_catalogue_dates(p_page_id);
END;
$$ LANGUAGE plpgsql;


-- Canonical review view: date_iso etc. right after the as-extracted date.
drop view if exists canonical_entries_view;

create view canonical_entries_view as
select
    cec.page_id,
    cec.entry_index,
    cec.model_tag,
    cec.priority,
    cec.serial,
    cec.reg,
    cec.title,
    tt.english_translation as title_en,
    cec.author,
    cec.gloss,
    coalesce(mp.canonical_value, cec.publisher) as publisher,
    coalesce(mc.canonical_value, cec.pubcity) as pubcity,
    cec.pp_verbatim,
    coalesce(mr.canonical_value, cec.printer) as printer,
    coalesce(mpc.canonical_value, cec.pcity) as pcity,
    cec.printer_verbatim,
    cec.date,
    cec.date_iso,
    cec.date_end,
    cec.date_precision,
    cec.date_status,
    cec.date_basis,
    cec.date_est_from,
    cec.date_est_to,
    cec.date_era,
    en.note as reviewer_note,
    cec.copies,
    cec.price,
    cec.edition,
    cec.format,
    cec.method,
    cec.educ,
    cec.copyright,
    coalesce(ml.canonical_value, cec.lang) as lang,
    cec.char_qualifier,
    cec.title_native,
    cec.topic,
    cec.section,
    cec.quarter,
    cec.pdf_page,
    cec.printed_page,
    cec.marks,
    cec.flags,
    cec.notes
from catalogue_entries_canonical cec
left join title_translations tt on tt.original_title = cec.title and not tt.is_english
left join field_value_map ml  on ml.field  = 'lang'      and ml.raw_value  = cec.lang
left join field_value_map mc  on mc.field  = 'pubcity'   and mc.raw_value  = cec.pubcity
left join field_value_map mpc on mpc.field = 'pcity'     and mpc.raw_value = cec.pcity
left join field_value_map mp  on mp.field  = 'publisher' and mp.raw_value  = cec.publisher
left join field_value_map mr  on mr.field  = 'printer'   and mr.raw_value  = cec.printer
left join canonical_entry_notes en on en.page_id = cec.page_id and en.entry_index = cec.entry_index;


-- Backfill
SELECT apply_catalogue_dates();
SELECT infer_catalogue_dates();
