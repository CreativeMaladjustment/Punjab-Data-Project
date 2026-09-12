"""Report the extraction pipeline's current backlog state: for each model
that's been run against the page images, how many pages haven't been
attempted yet, how many are done (successfully, or permanently given up
on), and how many are still in some retryable state -- the "what's left to
work on" questions that otherwise mean reading through Actions logs or
writing one-off SQL.

Read-only: this never claims, writes, or modifies anything in
llm_extractions -- it's safe to run at any time, including while
extract-pages.yml workers are actively claiming pages, without
interfering with them.

CLAIM_TIMEOUT_SECONDS and MAX_ATTEMPTS_PER_PAGE below mirror the same-named
constants in extract_with_llm.py (not imported from there directly: that
module does real work at import time -- connecting to B2, requiring
OLLAMA_MODEL -- that a read-only status report has no reason to need).
Keep them in sync if either changes there.
"""
import json
import os
import sys

import psycopg2

SUPABASE_DB_URL = os.environ["SUPABASE_DB_URL"]

CLAIM_TIMEOUT_SECONDS = 3 * 60 * 60  # see extract_with_llm.py's CLAIM_TIMEOUT_SECONDS
MAX_ATTEMPTS_PER_PAGE = 2  # see extract_with_llm.py's MAX_ATTEMPTS_PER_PAGE

# One row per model_tag that has ever been attempted, via conditional
# aggregation (COUNT ... FILTER) rather than one query per bucket -- a
# single pass over llm_extractions per model_tag instead of eight.
# total_attempted is exactly count(distinct page_id) for that model_tag
# already, since (page_id, model_tag) is unique -- no extra distinct
# needed.
STATUS_BY_MODEL_SQL = """
    select
        model_tag,
        count(*) as total_attempted,
        count(*) filter (
            where status = 'success' and coalesce(jsonb_array_length(raw_response), 0) > 0
        ) as success_with_entries,
        count(*) filter (
            where status = 'success' and coalesce(jsonb_array_length(raw_response), 0) = 0
        ) as success_empty,
        count(*) filter (
            where status = 'claimed' and claimed_at >= now() - %(claim_timeout)s * interval '1 second'
        ) as claimed_active,
        count(*) filter (
            where status = 'claimed' and claimed_at < now() - %(claim_timeout)s * interval '1 second'
        ) as claimed_stale,
        count(*) filter (
            where status = 'failed' and not content_failure
        ) as failed_transient,
        count(*) filter (
            where status = 'failed' and content_failure and attempt_count < %(max_attempts)s
        ) as failed_content_retryable,
        count(*) filter (
            where status = 'failed' and content_failure and attempt_count >= %(max_attempts)s
        ) as failed_content_capped,
        count(*) filter (where raw_text is not null) as has_raw_text
    from llm_extractions
    group by model_tag
    order by model_tag
"""

TOTAL_IMAGES_SQL = "select count(*) from pages where image_uploaded_at is not null"

ENTRY_COUNTS_SQL = """
    select le.model_tag, count(*)
    from catalogue_entries ce
    join llm_extractions le on le.id = ce.extraction_id
    group by le.model_tag
"""

# Diagnostic: what's actually breaking, for pages that have permanently
# given up (capped out) -- grouped on a truncated error_message since two
# failures with the same root cause rarely have byte-identical messages
# (line/column/char positions differ), but the first ~60 chars usually
# capture which kind of failure it was (e.g. "Unterminated string
# starting at" vs "expected a JSON array, got dict").
TOP_CAPPED_FAILURE_REASONS_SQL = """
    select model_tag, left(error_message, 60) as reason, count(*) as n
    from llm_extractions
    where status = 'failed' and content_failure and attempt_count >= %(max_attempts)s
    group by model_tag, left(error_message, 60)
    order by model_tag, n desc
"""


def db_connect():
    return psycopg2.connect(SUPABASE_DB_URL)


def fetch_report(conn):
    with conn.cursor() as cur:
        cur.execute(TOTAL_IMAGES_SQL)
        (total_images,) = cur.fetchone()

        cur.execute(
            STATUS_BY_MODEL_SQL,
            {"claim_timeout": CLAIM_TIMEOUT_SECONDS, "max_attempts": MAX_ATTEMPTS_PER_PAGE},
        )
        columns = [d.name for d in cur.description]
        by_model = [dict(zip(columns, row)) for row in cur.fetchall()]

        cur.execute(ENTRY_COUNTS_SQL)
        entry_counts = dict(cur.fetchall())

        cur.execute(TOP_CAPPED_FAILURE_REASONS_SQL, {"max_attempts": MAX_ATTEMPTS_PER_PAGE})
        capped_reasons = {}
        for model_tag, reason, n in cur.fetchall():
            capped_reasons.setdefault(model_tag, []).append((reason, n))

    for row in by_model:
        row["never_attempted"] = total_images - row["total_attempted"]
        # "What's left to work on": anything that could still change on
        # its own without a person doing something first. A stale claim
        # gets reclaimed automatically; a transient or under-cap content
        # failure gets retried automatically. A capped content failure
        # won't move again until a person reviews it or a different model
        # is tried -- so it's excluded here even though it's not "success".
        row["remaining"] = (
            row["never_attempted"]
            + row["claimed_stale"]
            + row["failed_transient"]
            + row["failed_content_retryable"]
        )
        row["catalogue_entries"] = entry_counts.get(row["model_tag"], 0)
        row["top_capped_reasons"] = capped_reasons.get(row["model_tag"], [])[:5]

    return total_images, by_model


def render_markdown(total_images, by_model):
    lines = []
    lines.append(f"## Extraction status ({total_images} images uploaded and ready to process)")
    lines.append("")
    if not by_model:
        lines.append("No model has been run against any page yet.")
        return "\n".join(lines) + "\n"

    lines.append(
        "| model | remaining | never attempted | claimed (active) | claimed (stale) "
        "| failed: retryable | failed: transient | failed: capped (needs review) "
        "| success (entries) | success (blank) | has model text | catalogue entries |"
    )
    lines.append("|---" * 12 + "|")
    for row in by_model:
        lines.append(
            f"| {row['model_tag']} | **{row['remaining']}** | {row['never_attempted']} "
            f"| {row['claimed_active']} | {row['claimed_stale']} "
            f"| {row['failed_content_retryable']} | {row['failed_transient']} "
            f"| {row['failed_content_capped']} "
            f"| {row['success_with_entries']} | {row['success_empty']} "
            f"| {row['has_raw_text']} "
            f"| {row['catalogue_entries']} |"
        )
    lines.append("")
    lines.append(
        "- **remaining** = never attempted + stale claims + transient failures + "
        "under-cap content failures -- everything still eligible to be picked up "
        "and retried automatically, either by a fresh claim or the next run."
    )
    lines.append(
        "- **failed: capped (needs review)** pages have failed the same way "
        f"{MAX_ATTEMPTS_PER_PAGE} times in a row and won't be reclaimed again "
        "for this model -- they need a person to look at the page, or a "
        "different model."
    )
    lines.append(
        "- **success (blank)** pages are legitimate zero-entry results (covers, "
        "blank pages, indexes) -- not failures."
    )
    lines.append(
        "- **has model text** counts every row with a real response from the "
        "model (`raw_text is not null`), success or failure alike -- a content "
        "failure still has text (that's exactly what made it a content failure "
        "rather than a transient one), so this is always >= success + capped."
    )

    for row in by_model:
        if row["top_capped_reasons"]:
            lines.append("")
            lines.append(f"### Top capped-failure reasons for `{row['model_tag']}`")
            lines.append("")
            lines.append("| count | reason (truncated) |")
            lines.append("|---|---|")
            for reason, n in row["top_capped_reasons"]:
                lines.append(f"| {n} | {reason} |")

    return "\n".join(lines) + "\n"


def main():
    conn = db_connect()
    total_images, by_model = fetch_report(conn)
    report = render_markdown(total_images, by_model)

    print(report)
    print("raw data:", json.dumps({"total_images": total_images, "by_model": by_model}, default=str))

    step_summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary_path:
        with open(step_summary_path, "a", encoding="utf-8") as f:
            f.write(report)


if __name__ == "__main__":
    sys.exit(main())
