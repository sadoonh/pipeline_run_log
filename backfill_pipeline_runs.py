"""
backfill_pipeline_runs.py
─────────────────────────
Backfills historical GitHub Actions workflow runs into the pipeline_runs
PostgreSQL table. Run this once per repo before enabling the live
record-pipeline-run workflow to avoid gaps in your data.

Requirements
────────────
    pip install requests psycopg2-binary

Environment variables
─────────────────────
    GITHUB_TOKEN   — personal access token with Actions: read
    DATABASE_URL   — postgresql://user:pass@host:5432/dbname

Usage
─────
    # All workflows, all time
    python backfill_pipeline_runs.py --repo owner/repo

    # Specific workflow
    python backfill_pipeline_runs.py --repo owner/repo --workflow "CI"

    # Date-bounded (recommended for large repos — slice by quarter)
    python backfill_pipeline_runs.py --repo owner/repo --since 2024-01-01 --until 2024-03-31

    # Dry run — fetch and enrich but do not write
    python backfill_pipeline_runs.py --repo owner/repo --dry-run

    # Tune parallelism for failed-step lookups (default 4)
    python backfill_pipeline_runs.py --repo owner/repo --workers 8
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
PER_PAGE   = 100

INSERT_SQL = """
INSERT INTO pipeline_runs (
    run_id, workflow_name, conclusion,
    run_started_at, run_completed_at,
    repository, branch, actor, head_sha,
    run_url, failed_step, source
) VALUES (
    %(run_id)s, %(workflow_name)s, %(conclusion)s,
    %(run_started_at)s, %(run_completed_at)s,
    %(repository)s, %(branch)s, %(actor)s, %(head_sha)s,
    %(run_url)s, %(failed_step)s, %(source)s
)
ON CONFLICT (run_id) DO UPDATE SET
    conclusion       = EXCLUDED.conclusion,
    run_completed_at = EXCLUDED.run_completed_at,
    failed_step      = EXCLUDED.failed_step
"""


# ─── HTTP session ──────────────────────────────────────────────────────────────

def build_session(token: str) -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    return session


# ─── Rate limit ────────────────────────────────────────────────────────────────

def _respect_rate_limit(resp: requests.Response) -> None:
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 9999))
    if remaining < 20:
        reset_at = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(0, reset_at - time.time()) + 2
        log.warning("Rate limit low (%d remaining) — sleeping %.0fs", remaining, wait)
        time.sleep(wait)


# ─── GitHub API ────────────────────────────────────────────────────────────────

def resolve_workflow_id(session: requests.Session, repo: str, name_or_file: str) -> str:
    resp = session.get(
        f"{GITHUB_API}/repos/{repo}/actions/workflows",
        params={"per_page": 100},
        timeout=15,
    )
    resp.raise_for_status()
    for wf in resp.json().get("workflows", []):
        if wf["name"] == name_or_file or wf["path"].endswith(name_or_file):
            return str(wf["id"])
    raise SystemExit(f"Workflow '{name_or_file}' not found in {repo}")


def fetch_runs(
    session: requests.Session,
    repo: str,
    workflow: Optional[str],
    since: Optional[str],
    until: Optional[str],
) -> list[dict]:
    """Page through all completed runs, returning raw API objects."""
    if workflow:
        wf_id = resolve_workflow_id(session, repo, workflow)
        url   = f"{GITHUB_API}/repos/{repo}/actions/workflows/{wf_id}/runs"
    else:
        url = f"{GITHUB_API}/repos/{repo}/actions/runs"

    params: dict = {"per_page": PER_PAGE, "page": 1, "status": "completed"}
    if since and until:
        params["created"] = f"{since}..{until}"
    elif since:
        params["created"] = f">={since}"
    elif until:
        params["created"] = f"<={until}"

    runs: list[dict] = []
    while True:
        resp = session.get(url, params=params, timeout=20)
        _respect_rate_limit(resp)
        resp.raise_for_status()
        batch = resp.json().get("workflow_runs", [])
        if not batch:
            break
        runs.extend(batch)
        log.info("  Page %d fetched — %d runs total so far", params["page"], len(runs))
        if len(batch) < PER_PAGE:
            break
        params["page"] += 1

    return runs


def fetch_failed_step(
    session: requests.Session,
    repo: str,
    run_id: int,
) -> Optional[str]:
    """Return the name of the first failed step for a run, or None."""
    url = f"{GITHUB_API}/repos/{repo}/actions/runs/{run_id}/jobs"
    try:
        resp = session.get(url, timeout=15)
        _respect_rate_limit(resp)
        resp.raise_for_status()
        for job in resp.json().get("jobs", []):
            for step in job.get("steps", []):
                if step.get("conclusion") == "failure":
                    return step["name"][:255]
    except requests.HTTPError as exc:
        log.warning("Could not fetch jobs for run %d: %s", run_id, exc)
    return None


# ─── Enrichment ────────────────────────────────────────────────────────────────

def enrich_failed_steps(
    session: requests.Session,
    repo: str,
    runs: list[dict],
    workers: int,
) -> dict[int, Optional[str]]:
    """
    Parallel Jobs API lookups for every failed run.
    Returns {run_id: failed_step_name_or_None}.
    """
    failed = [r for r in runs if r.get("conclusion") == "failure"]
    if not failed:
        return {}

    log.info("Looking up failed steps for %d failed runs (workers=%d)…", len(failed), workers)
    results: dict[int, Optional[str]] = {}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_id = {
            pool.submit(fetch_failed_step, session, repo, r["id"]): r["id"]
            for r in failed
        }
        done = 0
        for future in as_completed(future_to_id):
            run_id = future_to_id[future]
            results[run_id] = future.result()
            done += 1
            if done % 50 == 0 or done == len(failed):
                log.info("  Failed step lookup: %d/%d done", done, len(failed))

    return results


# ─── Data mapping ──────────────────────────────────────────────────────────────

def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def to_row(run: dict, failed_step: Optional[str]) -> dict:
    return {
        "run_id":           run["id"],
        "workflow_name":    run["name"][:255],
        "conclusion":       run.get("conclusion"),
        "run_started_at":   parse_dt(run.get("run_started_at") or run.get("created_at")),
        "run_completed_at": parse_dt(run.get("updated_at")),
        "repository":       run["repository"]["full_name"][:255],
        "branch":           (run.get("head_branch") or "")[:255] or None,
        "actor":            ((run.get("actor") or {}).get("login") or "")[:100] or None,
        "head_sha":         (run.get("head_sha") or "")[:40] or None,
        "run_url":          (run.get("html_url") or "")[:500] or None,
        "failed_step":      failed_step,
        "source":           "backfill",
    }


# ─── Database ──────────────────────────────────────────────────────────────────

def upsert_batch(conn: psycopg2.extensions.connection, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, rows, page_size=100)
        affected = cur.rowcount
    conn.commit()
    return max(affected, 0)


# ─── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill GitHub Actions runs into pipeline_runs (Postgres 16)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--repo",        required=True,       help="owner/repo")
    p.add_argument("--workflow",    default=None,        help="Workflow name or filename (optional)")
    p.add_argument("--since",       default=None,        help="Start date YYYY-MM-DD (optional)")
    p.add_argument("--until",       default=None,        help="End date YYYY-MM-DD (optional)")
    p.add_argument("--workers",     type=int, default=4, help="Parallelism for Jobs API calls (default: 4)")
    p.add_argument("--batch-size",  type=int, default=200, help="DB insert batch size (default: 200)")
    p.add_argument("--dry-run",     action="store_true", help="Fetch and enrich but skip DB writes")
    p.add_argument("--verbose",     action="store_true", help="Debug logging")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("ERROR: GITHUB_TOKEN environment variable not set")

    db_url = os.environ.get("DATABASE_URL")
    if not db_url and not args.dry_run:
        raise SystemExit("ERROR: DATABASE_URL environment variable not set (or pass --dry-run)")

    session = build_session(token)

    # ── Fetch ──────────────────────────────────────────────────────────────────
    log.info("Fetching completed runs for %s …", args.repo)
    if args.workflow:
        log.info("  Workflow filter : %s", args.workflow)
    if args.since or args.until:
        log.info("  Date range      : %s → %s", args.since or "beginning", args.until or "now")

    runs = fetch_runs(session, args.repo, args.workflow, args.since, args.until)
    log.info("Runs fetched: %d", len(runs))

    if not runs:
        log.info("Nothing to backfill.")
        return

    # ── Enrich ─────────────────────────────────────────────────────────────────
    failed_step_map = enrich_failed_steps(session, args.repo, runs, args.workers)

    rows = [to_row(r, failed_step_map.get(r["id"])) for r in runs]

    # ── Dry run preview ────────────────────────────────────────────────────────
    if args.dry_run:
        log.info("Dry-run mode — no DB writes. First 5 rows:")
        for row in rows[:5]:
            log.info(
                "  run_id=%-12s  %-12s  %-40s  failed_step=%s",
                row["run_id"], row["conclusion"],
                row["workflow_name"], row["failed_step"],
            )
        if len(rows) > 5:
            log.info("  … and %d more", len(rows) - 5)
        return

    # ── Upsert ─────────────────────────────────────────────────────────────────
    log.info("Upserting into PostgreSQL in batches of %d …", args.batch_size)
    conn = psycopg2.connect(db_url)
    total = 0
    try:
        for i in range(0, len(rows), args.batch_size):
            batch = rows[i : i + args.batch_size]
            total += upsert_batch(conn, batch)
            log.info(
                "  Processed %d / %d rows",
                min(i + args.batch_size, len(rows)),
                len(rows),
            )
    finally:
        conn.close()

    log.info("Done. %d rows upserted from %d fetched.", total, len(rows))


if __name__ == "__main__":
    main()