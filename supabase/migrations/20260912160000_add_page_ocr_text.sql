-- Full-page OCR text, kept separate from llm_extractions/catalogue_entries
-- rather than a new column on either: those two tables hold the *structured*
-- extraction (a JSON array of catalogue entries only, per the schema.md
-- prompt) -- non-entry page content (running headers, page furniture,
-- anything outside the schema) was never asked for and never captured
-- there. This table records a separate, independent model call whose job
-- is a verbatim transcription of everything visible on the page image, not
-- just the catalogue entries -- see scripts/extract_with_llm.py's
-- ocr_full_page(). Namespaced by (page_id, model_tag) exactly like
-- llm_extractions, since a page can be OCR'd by more than one model without
-- clobbering another model's transcription, and processed independently:
-- this call's success/failure has no effect on llm_extractions' retry
-- accounting, content_failure classification, or MAX_ATTEMPTS_PER_PAGE --
-- a page can have a failed structured extraction and a successful
-- full-page OCR, or vice versa.
create table page_ocr_text (
    id bigint generated always as identity primary key,
    page_id bigint not null references pages (id) on delete cascade,
    model text not null,
    model_tag text not null,
    status text not null check (status in ('success', 'failed')),
    error_message text,
    raw_text text,
    created_at timestamptz not null default now(),
    unique (page_id, model_tag)
);

create index page_ocr_text_page_id_idx on page_ocr_text (page_id);
create index page_ocr_text_model_tag_idx on page_ocr_text (model_tag);

-- Convenience view, mirroring catalogue_entries_full: one row per page's
-- full-page OCR text, joined back to its image and original pCloud PDF.
create view page_ocr_text_full as
select
    pot.*,
    p.pcloud_fileid,
    p.page_no,
    p.b2_account,
    p.b2_bucket,
    p.image_key,
    pf.name as pcloud_name,
    pf.folder as pcloud_folder
from page_ocr_text pot
join pages p on p.id = pot.page_id
join pcloud_files pf on pf.pcloud_fileid = p.pcloud_fileid;
