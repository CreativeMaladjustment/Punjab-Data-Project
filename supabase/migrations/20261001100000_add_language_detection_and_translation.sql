-- Language detection and translation tables for page OCR text
-- page_detected_languages: one row per unique language found on a page
-- page_translations: original text + English translation, one per language/section

CREATE TABLE page_detected_languages (
    id BIGSERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    language TEXT NOT NULL,                    -- e.g., "Punjabi", "English", "Urdu"
    language_code TEXT,                        -- ISO 639-1 or 639-2 code, e.g., "pa", "en", "ur"
    model_tag TEXT NOT NULL,                   -- which model detected it
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE(page_id, language_code)             -- one record per language per page
);

CREATE TABLE page_translations (
    id BIGSERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    source_language TEXT NOT NULL,             -- detected language of original text
    source_language_code TEXT,                 -- ISO code for source language
    original_text TEXT NOT NULL,               -- text in original language
    translated_text TEXT NOT NULL,             -- English translation
    section_index INTEGER DEFAULT 0,           -- for future multi-section handling
    model_tag TEXT NOT NULL,                   -- which model performed translation
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Index for querying languages/translations by page
CREATE INDEX idx_page_detected_languages_page_id ON page_detected_languages(page_id);
CREATE INDEX idx_page_translations_page_id ON page_translations(page_id);
CREATE INDEX idx_page_translations_source_language ON page_translations(source_language_code);
