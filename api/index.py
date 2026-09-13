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
Transaction-pooler URL, ever touches the database.

Access is gated by a single shared password (see login()/login_required
below) rather than Vercel Deployment Protection, so it can't rely on
Vercel-account login. Two env vars are required for this:

  DASHBOARD_PASSWORD  -- the string a visitor must type in at /login.
                         Never sent back to the browser in any form; only
                         compared against, server-side, in constant time.
  SESSION_SECRET_KEY  -- an unrelated random secret Flask uses to sign the
                         session cookie (via itsdangerous). The cookie it
                         produces contains only the plaintext claim
                         {"authenticated": true} plus a signature; the
                         signature proves the claim wasn't forged without
                         revealing this key (HMAC is one-way), and the key
                         itself never appears in the cookie or in any
                         client-side code. Generate it once with, e.g.,
                         `python -c "import secrets; print(secrets.token_hex(32))"`
                         -- it must stay the same across deployments/cold
                         starts for logins to persist, so it has to come
                         from an env var rather than being generated at
                         import time.
"""
import functools
import os
import sys
from datetime import timedelta

# Vercel's Python runtime imports this file directly by path (see
# vc_init.py in its traceback), which does not add this file's own
# directory to sys.path the way running `python index.py` normally would
# -- confirmed in production logs as `ModuleNotFoundError: No module named
# 'queries'` even though the exact same import works fine when run
# locally (where sys.path already includes the script's directory).
# Adding it explicitly makes the sibling import work regardless of how
# the entry file is loaded.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import hmac

import psycopg2
from flask import Flask, redirect, render_template, request, session, url_for

from queries import fetch_dashboard_data

# template_folder is given as an absolute path rather than left to Flask's
# default __name__-based resolution: that default depends on this module
# being registered in sys.modules under a normal dotted name, which
# Vercel's importlib-by-path loading (see the sys.path comment above)
# doesn't guarantee -- confirmed locally by reproducing that exact load
# path, which raised jinja2.exceptions.TemplateNotFound even after fixing
# the `queries` import.
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"),
)

# app.secret_key is what Flask/itsdangerous signs the session cookie with.
# Read (not generated) so it's stable across cold starts -- a randomly
# generated key would invalidate every logged-in session the moment
# Vercel spins up a fresh instance. Left None if unset rather than raising
# here: Flask only raises (its own clear "no secret key was set" error)
# once something actually tries to open/save a session, which keeps
# /healthz working even if this is misconfigured.
app.secret_key = os.environ.get("SESSION_SECRET_KEY")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,  # Vercel is HTTPS-only; never send over plain HTTP
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
)

DB_CONNECT_MAX_ATTEMPTS = 3  # short retry, not the pipeline's 5 -- a
# dashboard request should fail fast and let the user reload, not hold a
# serverless invocation open for a long backoff.


def _dashboard_password():
    try:
        return os.environ["DASHBOARD_PASSWORD"]
    except KeyError:
        raise RuntimeError(
            "DASHBOARD_PASSWORD is not set for this deployment -- add it "
            "in Vercel's project environment variables."
        ) from None


def login_required(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        submitted = request.form.get("password", "")
        # compare_digest for constant-time comparison -- a plain `==`
        # leaks how many leading characters matched via response timing.
        if submitted and hmac.compare_digest(submitted, _dashboard_password()):
            session.clear()
            session["authenticated"] = True
            session.permanent = True
            return redirect(request.args.get("next") or url_for("dashboard"))
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


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
@login_required
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
