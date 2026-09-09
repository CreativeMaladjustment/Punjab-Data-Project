-- Processing-pipeline state and LLM output, replacing B2 .done markers and
-- per-page JSON files as the source of truth (see scripts/process_pcloud.py
-- and scripts/extract_with_llm.py). B2 continues to hold the actual image
-- and PDF bytes; this schema tracks what has been uploaded where, and holds
-- LLM extraction output for analysis and review.

-- One row per PDF discovered on pCloud. pCloud's own file id is a stable,
-- natural primary key -- no surrogate key needed here.
create table pcloud_files (
    pcloud_fileid bigint primary key,
    name text not null,
    folder text not null,
    page_count int,
    created_at timestamptz not null default now()
);

-- One row per page of a pcloud_files PDF, recording exactly which B2
-- account/bucket holds that page's image (and, optionally, its split-out
-- single-page PDF). Pages of the same PDF can legitimately live in
-- different B2 accounts if the active account changed between runs --
-- the row says exactly where each one is, so there's no ambiguity the way
-- there was when B2 object listing was the only source of truth.
create table pages (
    id bigint generated always as identity primary key,
    pcloud_fileid bigint not null references pcloud_files (pcloud_fileid) on delete cascade,
    page_no int not null,
    b2_account text not null,
    b2_bucket text not null,
    image_key text not null,
    image_uploaded_at timestamptz,
    page_pdf_key text,
    page_pdf_uploaded_at timestamptz,
    created_at timestamptz not null default now(),
    unique (pcloud_fileid, page_no)
);

create index pages_pcloud_fileid_idx on pages (pcloud_fileid);

-- One row per (page, model) extraction attempt -- "which images have been
-- processed by which LLM". model is the raw Ollama model name (e.g.
-- "qwen3-vl:2b"); model_tag is the same value slugified, matching what the
-- old B2 key layout used, kept for continuity/readability.
create table llm_extractions (
    id bigint generated always as identity primary key,
    page_id bigint not null references pages (id) on delete cascade,
    model text not null,
    model_tag text not null,
    status text not null check (status in ('success', 'failed')),
    error_message text,
    raw_response jsonb,
    created_at timestamptz not null default now(),
    unique (page_id, model_tag)
);

create index llm_extractions_page_id_idx on llm_extractions (page_id);
create index llm_extractions_model_tag_idx on llm_extractions (model_tag);

-- One row per catalogue entry extracted from a page -- the queryable,
-- typed layer for analysis, mirroring pipeline/schema.md's field-by-field
-- shape. entry_index is the entry's position within that page's extracted
-- array (pages can hold more than one catalogue entry).
create table catalogue_entries (
    id bigint generated always as identity primary key,
    extraction_id bigint not null references llm_extractions (id) on delete cascade,
    entry_index int not null,
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
    unique (extraction_id, entry_index)
);

create index catalogue_entries_extraction_id_idx on catalogue_entries (extraction_id);

-- Convenience view: one row per catalogue entry, joined all the way back
-- to its image and its original pCloud PDF -- "linked to the image and the
-- original pdf file in pcloud".
create view catalogue_entries_full as
select
    ce.*,
    le.model,
    le.model_tag,
    p.pcloud_fileid,
    p.page_no,
    p.b2_account,
    p.b2_bucket,
    p.image_key,
    pf.name as pcloud_name,
    pf.folder as pcloud_folder
from catalogue_entries ce
join llm_extractions le on le.id = ce.extraction_id
join pages p on p.id = le.page_id
join pcloud_files pf on pf.pcloud_fileid = p.pcloud_fileid;
