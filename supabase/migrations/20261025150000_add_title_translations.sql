-- Lookup of catalogue entry titles checked for English-ness, with the English
-- translation for those that are not (fully) English.
-- One row per distinct original title string; populated by
-- scripts/translate_canonical_titles.py from catalogue_entries_canonical.
-- Titles judged fully English are recorded too (is_english = true, no
-- translation) so they aren't re-checked on every run.

create table title_translations (
    id bigint generated always as identity primary key,
    original_title text not null unique,
    is_english boolean not null,
    source_language text,          -- e.g. "Punjabi", "Hindi", "Urdu"; null when English
    source_language_code text,     -- ISO 639-1, e.g. "pa", "hi", "ur"
    english_translation text,      -- null when is_english
    model_tag text not null,
    created_at timestamptz not null default now(),
    check (is_english or english_translation is not null)
);

create index title_translations_is_english_idx on title_translations (is_english);
