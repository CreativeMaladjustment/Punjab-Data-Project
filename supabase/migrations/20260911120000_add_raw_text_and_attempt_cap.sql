-- Two additions surfaced by the first real extraction run after fixing
-- glm-ocr's context-size 400s (see scripts/extract_with_llm.py):
--
-- 1. raw_text: the model's actual response string (before any JSON
--    parsing/coercion), stored on every attempt -- success or failure. Until
--    now the model's actual output for a page was only ever visible as a
--    transient, truncated print() in that run's Actions log: a successful
--    page's `raw_response` column stores the *parsed and coerced* entries,
--    not what the model said, and a failed page's actual text was never
--    persisted anywhere.
--
-- 2. attempt_count: how many times a page has been claimed under a given
--    model_tag. Without a cap, a page that fails the same way every time (a
--    bad JSON shape the model keeps producing, not a transient error) got
--    reclaimed forever, every CLAIM_TIMEOUT_SECONDS, burning worker time on
--    a page that was never going to succeed. claim_next_page() now excludes
--    a stale 'claimed'/'failed' row once attempt_count reaches
--    MAX_ATTEMPTS_PER_PAGE (scripts/extract_with_llm.py) -- it stays
--    queryable (status = 'failed' and attempt_count >= MAX_ATTEMPTS_PER_PAGE)
--    for manual review or a different model, instead of disappearing into a
--    retry loop that never ends.
alter table llm_extractions
    add column raw_text text,
    add column attempt_count int not null default 0;

-- Every pre-existing row was claimed/attempted at least once; backfill so
-- the retry cap applies to that history too, instead of treating every row
-- that predates this migration as attempt zero.
update llm_extractions
    set attempt_count = 1
    where attempt_count = 0;
