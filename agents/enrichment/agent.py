"""
Enrichment Agent — main orchestrator.

Usage:
    python -m agents.enrichment.agent --campaign <uuid> [--dry-run] [--limit N]

Per lead, runs these steps in order:
    1. web_search   — find DM name/title via web
    2. website_fetch — read actual site pages (skipped if no website)
    3. linkedin     — verify LinkedIn profile
    4. hunter_io    — direct API email lookup
    4b. website_scrape is handled inside website_fetch (email extracted there)
    4c. facebook    — Apify scrape for email in About section
    4d. domain_guess — last resort construction
    5. icp_score    — score ICP fit (Haiku model)
    6. write result to Supabase decision_makers

Concurrency: asyncio + Semaphore(max_concurrent_workers) to cap simultaneous
Anthropic calls and avoid thundering-herd rate limit failures.

Error handling:
    - Each step is isolated: one step failing never kills the lead
    - CREDENTIAL_INVALID on Anthropic/Supabase → hard abort
    - CREDENTIAL_INVALID on Hunter/Apify → warning, fallback applied
    - Rate limits → exponential backoff, auto-retry up to 4 times
    - Lead stuck in_progress after crash → reaper re-queues after 15 min
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import time
import uuid
from typing import Optional

import anthropic
from dotenv import load_dotenv
from supabase import Client, create_client

from agents.config import CampaignConfig, load_campaign_config
from agents.quota import (
    CredentialInvalidError,
    QuotaExhaustedError,
    CreditsLowError,
    RateLimitedError,
    check_apify_credits,
    claim_hunter_request,
    has_blocking_failures,
    run_preflight,
)
from agents.enrichment.steps import (
    run_web_search,
    run_website_fetch,
    run_linkedin_verify,
    run_hunter_lookup,
    run_facebook_scrape,
    run_domain_guess,
    run_icp_score,
)
from agents.enrichment.steps._base import StepResult
from agents.enrichment.steps.icp_score import extract_icp_score

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Global rate-limit state — shared across all concurrent workers
# ---------------------------------------------------------------------------
_hunter_quota_exhausted = False
_anthropic_semaphore: Optional[asyncio.Semaphore] = None


# ---------------------------------------------------------------------------
# Single-lead enrichment (async, runs inside the semaphore)
# ---------------------------------------------------------------------------

async def enrich_lead(
    lead: dict,
    config: CampaignConfig,
    supabase: Client,
    anthropic_client: anthropic.Anthropic,
    agent_session_id: str,
    lead_index: int,
    total_leads: int,
) -> None:
    global _hunter_quota_exhausted

    lead_id = lead["id"]
    business = lead.get("business_name", "?")
    prefix = f"[{lead_index}/{total_leads}] {business}"

    job_id = _start_job(supabase, lead_id, config)
    steps_log: dict[str, str] = {}
    total_cost = 0.0
    start = time.monotonic()

    # Accumulated context passed between steps
    name: Optional[str] = None
    title: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None
    email_confidence: Optional[str] = None
    email_source: Optional[str] = None
    notes_parts: list[str] = []

    def update_context(result: StepResult) -> None:
        nonlocal name, title, linkedin_url, email, email_confidence, email_source
        if result.name and not name:
            name = result.name
        if result.title and not title:
            title = result.title
        if result.linkedin_url and not linkedin_url:
            linkedin_url = result.linkedin_url
        if result.email and not email:
            email = result.email
            email_confidence = result.email_confidence
            email_source = result.email_source
        if result.notes:
            notes_parts.append(f"[{result.step}] {result.notes}")

    def log_step(result: StepResult) -> None:
        nonlocal total_cost
        total_cost += result.cost_usd
        steps_log[result.step] = result.error_code if not result.success else "ok"
        if not result.success and result.error_code not in ("NO_URL", "NO_DOMAIN", "NO_NAME"):
            log.warning("%s — step %s: %s", prefix, result.step, result.error_message)

    async def run_with_backoff(fn, *args, **kwargs) -> StepResult:
        """Run a sync step function with exponential backoff on Anthropic rate limits."""
        for attempt in range(4):
            try:
                # Run synchronous step in thread pool to not block event loop
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
                return result
            except anthropic.RateLimitError:
                wait = (2 ** attempt) + random.uniform(0, 1)
                log.info("%s — Anthropic rate limit, backing off %.1fs (attempt %d)", prefix, wait, attempt + 1)
                await asyncio.sleep(wait)
        # Final attempt
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    try:
        async with _anthropic_semaphore:
            # ── Step 1: Web search ─────────────────────────────────────────
            if total_cost < config.enrichment.max_cost_per_lead_usd:
                r = await run_with_backoff(run_web_search, lead, anthropic_client, config.enrichment)
                update_context(r)
                log_step(r)

            # ── Step 2: Website fetch ──────────────────────────────────────
            if total_cost < config.enrichment.max_cost_per_lead_usd:
                r = await run_with_backoff(run_website_fetch, lead, anthropic_client, config.enrichment, name)
                update_context(r)
                log_step(r)

            # ── Step 3: LinkedIn verify ────────────────────────────────────
            if total_cost < config.enrichment.max_cost_per_lead_usd and linkedin_url:
                r = await run_with_backoff(run_linkedin_verify, lead, anthropic_client, config.enrichment, linkedin_url, name)
                update_context(r)
                log_step(r)

            # ── Step 4a: Hunter.io ─────────────────────────────────────────
            if (
                not (email and config.enrichment.stop_after_first_email)
                and total_cost < config.enrichment.max_cost_per_lead_usd
                and "hunter_io" in config.enrichment.email_sources
                and not _hunter_quota_exhausted
            ):
                loop = asyncio.get_event_loop()
                quota_granted = await loop.run_in_executor(
                    None, lambda: claim_hunter_request(supabase, config.org_id)
                )
                if quota_granted:
                    try:
                        r = await loop.run_in_executor(None, lambda: run_hunter_lookup(lead, name))
                        update_context(r)
                        log_step(r)
                    except QuotaExhaustedError:
                        _hunter_quota_exhausted = True
                        steps_log["hunter_io"] = "QUOTA_EXHAUSTED"
                        log.warning("Hunter.io quota exhausted — disabling for this batch")
                    except CredentialInvalidError as e:
                        steps_log["hunter_io"] = "CREDENTIAL_INVALID"
                        _alert_slack(str(e), "hunter_io", config.campaign_name, lead_index)
                else:
                    steps_log["hunter_io"] = "QUOTA_EXHAUSTED"

            # ── Step 4c: Facebook scrape ───────────────────────────────────
            if (
                not (email and config.enrichment.stop_after_first_email)
                and total_cost < config.enrichment.max_cost_per_lead_usd
                and "facebook" in config.enrichment.email_sources
            ):
                try:
                    loop = asyncio.get_event_loop()
                    r = await loop.run_in_executor(None, lambda: run_facebook_scrape(lead))
                    update_context(r)
                    log_step(r)
                except CreditsLowError as e:
                    steps_log["facebook"] = "CREDITS_LOW"
                    log.warning("%s — Apify credits low: %s", prefix, e)
                except CredentialInvalidError as e:
                    steps_log["facebook"] = "CREDENTIAL_INVALID"
                    _alert_slack(str(e), "apify", config.campaign_name, lead_index)

            # ── Step 4d: Domain guess ──────────────────────────────────────
            if (
                not (email and config.enrichment.stop_after_first_email)
                and "domain_guess" in config.enrichment.email_sources
                and lead.get("website")
                and name
            ):
                loop = asyncio.get_event_loop()
                r = await loop.run_in_executor(None, lambda: run_domain_guess(lead, name))
                update_context(r)
                log_step(r)

            # ── Step 5: ICP Score ──────────────────────────────────────────
            research_summary = "\n".join(notes_parts)
            r = await run_with_backoff(run_icp_score, lead, anthropic_client, config.enrichment, research_summary)
            log_step(r)
            icp_score = extract_icp_score(r)
            if r.notes:
                notes_parts.append(f"[icp] {r.notes}")

    except Exception as e:
        log.error("%s — unexpected error: %s", prefix, e, exc_info=True)
        steps_log["orchestrator"] = f"UNEXPECTED: {e}"
        _mark_lead_failed(supabase, lead_id, str(e))
        _finish_job(supabase, job_id, "failed", steps_log, total_cost, time.monotonic() - start)
        return

    # ── Write result to Supabase ───────────────────────────────────────────
    duration = time.monotonic() - start
    if name or email:
        _write_decision_maker(supabase, lead, name, title, linkedin_url,
                               email, email_confidence, email_source,
                               icp_score, "\n".join(notes_parts), config.campaign_name)
        _mark_lead_enriched(supabase, lead_id)
        status_icon = "✓" if email else "~"
        email_display = email or "no email found"
        print(
            f"  {status_icon} [{lead_index}/{total_leads}] {business} — "
            f"{name or 'DM unknown'} — {email_display} — ICP: {icp_score}"
        )
    else:
        _mark_lead_failed(supabase, lead_id, "No DM or email found after all steps")
        print(f"  ✗ [{lead_index}/{total_leads}] {business} — no result")

    _finish_job(supabase, job_id, "done" if (name or email) else "failed",
                steps_log, total_cost, duration)


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

async def run_batch(
    supabase: Client,
    config: CampaignConfig,
    anthropic_client: anthropic.Anthropic,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> None:
    global _anthropic_semaphore, _hunter_quota_exhausted
    _hunter_quota_exhausted = False
    _anthropic_semaphore = asyncio.Semaphore(config.enrichment.max_concurrent_workers)

    # Run reaper first — re-queue any stale in_progress leads
    reaper_result = supabase.rpc("reap_stale_locks", {"p_campaign_id": config.campaign_id}).execute()
    log.info("Reaper complete")

    # Count queued leads
    count_result = (
        supabase.table("leads")
        .select("id", count="exact")
        .eq("campaign_id", config.campaign_id)
        .eq("enrichment_status", "queued")
        .lt("retry_count", 3)
    )
    if config.lead_filter.has_website is True:
        count_result = count_result.neq("website", "").not_.is_("website", "null")
    elif config.lead_filter.has_website is False:
        count_result = count_result.or_("website.is.null,website.eq.")

    count_result = count_result.execute()
    total_queued = count_result.count or 0

    if limit:
        total_queued = min(total_queued, limit)

    batch_size = min(config.lead_filter.batch_size, total_queued)

    # Dry-run summary
    from agents.quota import get_hunter_quota_status
    quota = get_hunter_quota_status(supabase, config.org_id)
    est_cost_low  = total_queued * 0.02
    est_cost_high = total_queued * config.enrichment.max_cost_per_lead_usd
    est_time_min  = (total_queued * 30) / (config.enrichment.max_concurrent_workers * 60)

    print(f"\n  Lead Enrichment {'— DRY RUN' if dry_run else ''}")
    print(f"  {'─' * 50}")
    print(f"  Campaign:       {config.campaign_name}")
    print(f"  Filter:         has_website = {config.lead_filter.has_website}")
    print(f"  Leads queued:   {total_queued}")
    print(f"  Research depth: {config.enrichment.research_depth}")
    print(f"  Workers:        {config.enrichment.max_concurrent_workers}")
    print(f"  Est. cost:      ${est_cost_low:.2f}–${est_cost_high:.2f}")
    print(f"  Est. time:      ~{est_time_min:.0f} minutes")
    print(f"  Hunter quota:   {quota['used']}/{quota['limit']} used this month")
    print()

    if dry_run:
        print("  Dry run complete. Pass --no-dry-run to execute.")
        return

    if total_queued == 0:
        print("  No leads to process.")
        return

    answer = input("  Proceed? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("  Aborted.")
        return

    print(f"\n  Starting enrichment for {total_queued} leads...\n")

    agent_session_id = str(uuid.uuid4())
    processed = 0

    while processed < total_queued:
        # Claim a batch of leads atomically
        claim_size = min(config.enrichment.max_concurrent_workers, total_queued - processed)
        claimed = supabase.rpc("claim_leads_for_enrichment", {
            "p_campaign_id": config.campaign_id,
            "p_agent_id": agent_session_id,
            "p_batch_size": claim_size,
        }).execute()

        leads = claimed.data or []
        if not leads:
            break  # No more leads

        tasks = [
            enrich_lead(
                lead=lead,
                config=config,
                supabase=supabase,
                anthropic_client=anthropic_client,
                agent_session_id=agent_session_id,
                lead_index=processed + i + 1,
                total_leads=total_queued,
            )
            for i, lead in enumerate(leads)
        ]
        await asyncio.gather(*tasks)
        processed += len(leads)

    print(f"\n  Done. Processed {processed} leads.")


# ---------------------------------------------------------------------------
# Supabase helper writes
# ---------------------------------------------------------------------------

def _start_job(supabase: Client, lead_id: str, config: CampaignConfig) -> str:
    result = supabase.table("enrichment_jobs").insert({
        "lead_id": lead_id,
        "campaign_id": config.campaign_id,
        "org_id": config.org_id,
        "status": "running",
    }).execute()
    return result.data[0]["id"] if result.data else str(uuid.uuid4())


def _finish_job(supabase: Client, job_id: str, status: str, steps: dict,
                cost_usd: float, duration_sec: float) -> None:
    supabase.table("enrichment_jobs").update({
        "status": status,
        "steps_completed": steps,
        "cost_usd": round(cost_usd, 5),
        "duration_sec": int(duration_sec),
        "completed_at": "now()",
    }).eq("id", job_id).execute()


def _mark_lead_enriched(supabase: Client, lead_id: str) -> None:
    supabase.table("leads").update({
        "enrichment_status": "enriched",
        "enriched_at": "now()",
        "locked_at": None,
        "locked_by": None,
    }).eq("id", lead_id).execute()


def _mark_lead_failed(supabase: Client, lead_id: str, reason: str) -> None:
    # Increment retry_count — reaper will re-queue if < 3, mark dead if >= 3
    supabase.rpc("increment_lead_retry", {
        "p_lead_id": lead_id,
        "p_error": reason[:500],
    }).execute()


def _write_decision_maker(
    supabase: Client,
    lead: dict,
    name: Optional[str],
    title: Optional[str],
    linkedin_url: Optional[str],
    email: Optional[str],
    email_confidence: Optional[str],
    email_source: Optional[str],
    icp_score: int,
    research_notes: str,
    campaign_name: str,
) -> None:
    supabase.table("decision_makers").upsert({
        "lead_id": lead["id"],
        "name": name,
        "title": title,
        "linkedin_url": linkedin_url,
        "email": email,
        "email_confidence": email_confidence,
        "email_source": email_source,
        "icp_score": icp_score,
        "research_notes": research_notes[:2000],  # cap length
    }, on_conflict="lead_id,email").execute()


def _alert_slack(message: str, service: str, campaign: str, lead_index: int) -> None:
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook:
        return
    import httpx
    try:
        httpx.post(webhook, json={
            "text": (
                f"🔴 *Enrichment Alert — CREDENTIAL_INVALID*\n"
                f"Service: {service} | Campaign: {campaign}\n"
                f"Lead #{lead_index}\n"
                f"Action: {message}"
            )
        }, timeout=5)
    except Exception:
        pass  # don't let Slack failure kill the enrichment run


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Enrichment Agent")
    parser.add_argument("--campaign", required=True, help="Campaign UUID")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Preview run without processing leads")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of leads to process")
    args = parser.parse_args()

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # Pre-flight
    checks = run_preflight(supabase, _get_org_id(supabase, args.campaign))
    if has_blocking_failures(checks):
        print("\n  ✗ Blocking credential failures found. Fix before proceeding.\n")
        return

    # Check Apify credits before starting (non-blocking: warning only)
    try:
        check_apify_credits()
    except Exception as e:
        print(f"  ⚠ {e}\n  Facebook scrape may be limited.\n")

    config = load_campaign_config(supabase, args.campaign)
    asyncio.run(run_batch(supabase, config, anthropic_client, args.dry_run, args.limit))


def _get_org_id(supabase: Client, campaign_id: str) -> str:
    result = (
        supabase.table("campaigns")
        .select("org_id")
        .eq("id", campaign_id)
        .single()
        .execute()
    )
    return result.data["org_id"] if result.data else ""


if __name__ == "__main__":
    main()
