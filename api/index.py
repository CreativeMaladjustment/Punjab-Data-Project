"""Processing-status dashboard, deployed as a Vercel Python (Flask/WSGI)
serverless function. Read-only: shows how extraction is going -- which
models have run, how many pages each has attempted/succeeded/failed for
both the structured catalogue-entry extraction and the independent
full-page OCR pass (see scripts/extract_with_llm.py and
supabase/migrations/20260912160000_add_page_ocr_text.sql).

Talks to Supabase via SUPABASE_DB_URL, same env var name as the pipeline
scripts for consistency -- but this MUST be set in Vercel to the
Transaction-mode pooler connection string (port 6543 in Supabase's
connection-string picker), not the Session-mode one extract_with_llm.py
uses. A serverless function can have many concurrent invocations each
opening a connection; session-mode's small fixed client-slot count is
exactly what caused the EMAXCONNSESSION contention this project already
hit once with the extraction workers alone (see PERFORMANCE_NOTES.md /
git history) -- pointing this dashboard at the same session-mode URL would
reintroduce that same contention every time someone loads the page while
workers are running.

No RLS is configured on these tables yet, so this deliberately never
accepts a database credential from the browser and never queries Supabase
via its REST/anon-key path -- only this server-side code, with the
Transaction-pooler URL, ever touches the database. Put Vercel's own
Deployment Protection in front of this route (Project Settings ->
Deployment Protection) since anyone who can reach the URL can currently
see everything this dashboard shows.
"""
import os

import psycopg2
from flask import Flask, render_template

from queries import fetch_dashboard_data

app = Flask(__name__)

DB_CONNECT_MAX_ATTEMPTS = 3  # short retry, not the pipeline's 5 -- a
# dashboard request should fail fast and let the user reload, not hold a
# serverless invocation open for a long backoff.


def db_connect():
    # Read at call time, not at module import: Vercel's Python builder
    # imports this file to find the WSGI `app` object, and raising here
    # (as a bare os.environ[...] at module level would, if the env var
    # isn't set for this deployment's environment -- e.g. added for
    # Production but not Preview) would abort the whole build with an
    # opaque "Error" status instead of a clear message on the one request
    # that actually needs it.
    try:
        db_url = os.environ["SUPABASE_DB_URL"]
    except KeyError:
        raise RuntimeError(
            "SUPABASE_DB_URL is not set for this deployment -- add it in "
            "Vercel's project environment variables (Supabase's "
            "Transaction-mode pooler connection string, port 6543), making "
            "sure it's enabled for this deployment's environment "
            "(Production/Preview/Development)."
        ) from None

    import time

    last_exc = None
    for attempt in range(1, DB_CONNECT_MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(db_url)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if attempt < DB_CONNECT_MAX_ATTEMPTS:
                time.sleep(1)
    raise last_exc


@app.route("/")
def dashboard():
    conn = db_connect()
    try:
        data = fetch_dashboard_data(conn)
    finally:
        conn.close()
    return render_template("dashboard.html", **data)


@app.route("/healthz")
def healthz():
    return {"ok": True}
