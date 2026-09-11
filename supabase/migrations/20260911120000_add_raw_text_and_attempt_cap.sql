-- Three additions surfaced by the first real extraction run after fixing
-- glm-ocr's context-size 400s, and by review of this migration itself
-- before it merged (see scripts/extract_with_llm.py):
--
-- 1. raw_text: the model's actual response string (before any JSON
--    parsing/coercion), stored on every attempt -- success or failure. Until
--    now the model's actual output for a page was only ever visible as a
--    transient, truncated print() in that run's Actions log: a successful
--    page's `raw_response` column stores the *parsed and coerced* entries,
--    not what the model said, and a failed page's actual text was never
--    persisted anywhere.
--
-- 2. attempt_count + content_failure: how many times a page has genuinely
--    been attempted, and whether its last failure was a content/shape
--    problem as opposed to a transient one. A page whose *content* fails
--    the same way every time (a bad JSON shape the model keeps producing)
--    was, without a cap, reclaimed forever, every CLAIM_TIMEOUT_SECONDS,
--    burning worker time on a page that was never going to succeed with
--    this model. But process_page() calls db_save_extraction_failure() for
--    every kind of failure alike -- B2/network errors, a non-ok Ollama
--    HTTP response, an unconfigured account, a bad catalogue_entries
--    insert, not just a bad model response -- and those are transient:
--    exactly the multi-hour B2 quota outage this project hit on
--    2026-09-10 (see D-021 in DECISIONS.md) would, under a cap that didn't
--    distinguish the two, have permanently excluded every page it touched
--    even after B2 recovered. content_failure is true only when the model
--    actually responded and that response's JSON/shape was unusable
--    (scripts/extract_with_llm.py's is_content_failure()); the cap
--    (MAX_ATTEMPTS_PER_PAGE) and attempt_count's increment both apply only
--    when content_failure is true, so a transient failure keeps retrying
--    indefinitely exactly like before this cap existed.
--
--    A stale 'claimed' row (a worker that died before ever recording any
--    outcome -- content or transient) is exempt from the cap check
--    entirely and always stays reclaimable regardless of attempt_count:
--    it never got a real attempt at extraction, so counting it against
--    the cap would let a worker crash on a page's final permitted try
--    strand that row at status='claimed' forever, looking perpetually
--    in-progress while actually abandoned.
--
--    Once a row is capped out, it stays queryable (status = 'failed' AND
--    content_failure AND attempt_count >= MAX_ATTEMPTS_PER_PAGE) for
--    manual review or a different model, instead of disappearing into a
--    retry loop that never ends.
alter table llm_extractions
    add column raw_text text,
    add column attempt_count int not null default 0,
    add column content_failure boolean;

-- Every pre-existing row was claimed/attempted at least once; backfill so
-- the retry cap applies to that history too, instead of treating every row
-- that predates this migration as attempt zero. Pre-existing failures
-- predate the content/transient distinction entirely -- treated as
-- content failures (the more conservative choice: eligible for the cap,
-- rather than retrying forever unexamined) since there's no way to
-- reclassify them after the fact.
update llm_extractions
    set attempt_count = 1,
        content_failure = (status = 'failed')
    where attempt_count = 0;
