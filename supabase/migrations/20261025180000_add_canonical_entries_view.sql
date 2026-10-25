-- One flat, human-readable row per canonical catalogue entry for the
-- Canonical review table: normalized values (field_value_map) in place of
-- the as-extracted spellings, plus the English title translation.
-- The as-extracted values stay in catalogue_entries_canonical.

create or replace view canonical_entries_view as
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
left join field_value_map mr  on mr.field  = 'printer'   and mr.raw_value  = cec.printer;
