"""
Scheduler — multi-campaign dispatcher for Claude Code routines.

Queries all active campaigns for an org and runs enrichment on each one
in headless (non-interactive) mode. Designed to be called by a Claude Code
scheduled routine (CronCreate/RemoteTrigger) or a cron job.

Usage:
    python -m agents.scheduler --run-all-active [--org <uuid>] [--dry-run] [--json-log <path>]

Exit codes:
    0 — all campaigns processed cleanly (or nothing to do)
    1 — one or more campaigns hit a hard error (blocking credential failure, exception)
    2 — partial success: all campaigns ran but some leads are dead (retry_count >= 3)

The --json-log flag writes a machine-readable JSONL summary alongside terminal output.
Claude Code routines can parse this to surface results in notifications.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import anthropic
from dotenv import load_dotenv
from supabase import Client, create_client

from agents.config import load_campaign_config
from agents.quota import (
    BillingError,
    CredentialInvalidError,
    CreditsLowError,
    check_apify_credits,
    has_blocking_failures,
    run_preflight,
)
from agents.enrichment.agent import run_batch
from agents.results import EnrichmentRunResult

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def fetch_active_campaigns(supabase: Client, org_id: Optional[str] = None) -> list[dict]:
    """Return all campaigns with status='active', optionally scoped to one org."""
    query = (
        supabase.table("campaigns")
        .select("id, name, org_id")
        .eq("status", "active")
    )
    if org_id:
        query = query.eq("org_id", org_id)
    result = query.execute()
    return result.data or []


def _write_json_log(path: str, payload: dict) -> None:
    """Append a JSONL entry to the log file."""
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception as e:
        log.warning("Failed to write JSON log: %s", e)


def run_all_active(
    supabase: Client,
    anthropic_client: anthropic.Anthropic,
    org_id: Optional[str] = None,
    dry_run: bool = False,
    json_log_path: Optional[str] = None,
) -> int:
    """
    Run enrichment on all active campaigns.

    Returns the appropriate exit code (0/1/2).
    """
    started_at = datetime.now(timezone.utc).isoformat()
    campaigns = fetch_active_campaigns(supabase, org_id)

    if not campaigns:
        print("  No active campaigns found. Nothing to do.")
        if json_log_path:
            _write_json_log(json_log_path, {
                "timestamp": started_at,
                "campaigns_run": 0,
                "leads_enriched": 0,
                "leads_dead": 0,
                "errors": [],
            })
        return 0

    print(f"\n  Scheduler — {len(campaigns)} active campaign(s)")
    print(f"  {'─' * 50}")
    for c in campaigns:
        print(f"  • {c['name']} ({c['id']})")
    print()

    # Pre-flight once (uses first campaign's org to check shared credentials)
    first_org = org_id or campaigns[0]["org_id"]
    checks = run_preflight(supabase, first_org)
    if has_blocking_failures(checks):
        print("\n  ✗ Blocking credential failures. Fix before running.\n")
        if json_log_path:
            _write_json_log(json_log_path, {
                "timestamp": started_at,
                "campaigns_run": 0,
                "leads_enriched": 0,
                "leads_dead": 0,
                "errors": ["BLOCKING_CREDENTIAL_FAILURE"],
            })
        return 1

    try:
        check_apify_credits()
    except CreditsLowError as e:
        print(f"  ⚠ {e}\n  Facebook scrape may be limited.\n")
    except Exception as e:
        print(f"  ⚠ Apify credit check failed: {e}\n")

    # Run enrichment per campaign
    all_results: list[EnrichmentRunResult] = []
    hard_errors: list[str] = []

    for campaign in campaigns:
        campaign_id = campaign["id"]
        campaign_name = campaign["name"]
        print(f"  ▶ Starting: {campaign_name}")

        try:
            config = load_campaign_config(supabase, campaign_id)
            result = asyncio.run(
                run_batch(supabase, config, anthropic_client,
                          dry_run=dry_run, headless=True)
            )
            all_results.append(result)

            status = "✓" if result.success else "⚠"
            print(
                f"  {status} {campaign_name} — "
                f"{result.leads_processed} processed, "
                f"{result.leads_dead} dead"
            )
            if result.errors:
                for err in result.errors:
                    print(f"    ✗ {err}")

        except (CredentialInvalidError, BillingError) as e:
            msg = f"{campaign_name}: {e}"
            hard_errors.append(msg)
            print(f"  ✗ {msg}")
        except Exception as e:
            msg = f"{campaign_name}: unexpected error — {e}"
            hard_errors.append(msg)
            log.exception("Campaign %s failed", campaign_name)

    # Aggregate
    total_enriched = sum(r.leads_enriched for r in all_results)
    total_dead = sum(r.leads_dead for r in all_results)
    all_errors = hard_errors + [e for r in all_results for e in r.errors]

    print(f"\n  Scheduler complete.")
    print(f"  Campaigns: {len(campaigns)} | Enriched: {total_enriched} | Dead: {total_dead}")
    if all_errors:
        print(f"  Errors: {len(all_errors)}")

    if json_log_path:
        _write_json_log(json_log_path, {
            "timestamp": started_at,
            "campaigns_run": len(campaigns),
            "leads_enriched": total_enriched,
            "leads_dead": total_dead,
            "errors": all_errors,
        })

    if hard_errors:
        return 1
    if total_dead > 0:
        return 2
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrichment Scheduler — runs all active campaigns")
    parser.add_argument("--run-all-active", action="store_true", required=True,
                        help="Run enrichment for all active campaigns")
    parser.add_argument("--org", default=None, help="Scope to a specific org UUID")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Preview without processing any leads")
    parser.add_argument("--json-log", default=None,
                        help="Path to append a JSONL run summary (e.g. logs/scheduler.jsonl)")
    args = parser.parse_args()

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    exit_code = run_all_active(
        supabase=supabase,
        anthropic_client=anthropic_client,
        org_id=args.org,
        dry_run=args.dry_run,
        json_log_path=args.json_log,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
