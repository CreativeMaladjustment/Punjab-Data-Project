-- Human notes on canonical catalogue entries.
--
-- Keyed by (page_id, entry_index) -- the entry's place on the page -- and
-- deliberately NOT a foreign key into catalogue_entries_canonical, which is
-- rebuilt whenever the winning extraction changes. Alongside the note we
-- snapshot enough to find the record the note was written about (which
-- extraction/model won at the time, its serial/reg and title).

create table canonical_entry_notes (
    id bigint generated always as identity primary key,
    page_id bigint not null references pages (id) on delete cascade,
    entry_index int not null,
    note text not null,
    -- snapshot of the canonical record when the note was last saved
    source_extraction_id bigint,
    model_tag text,
    priority int,
    serial int,
    reg text,
    title text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (page_id, entry_index)
);

create index canonical_entry_notes_page_id_idx on canonical_entry_notes (page_id);


-- Expose the note in the Canonical review view (right after date).
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
