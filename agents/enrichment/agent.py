"""
Enrichment Agent — main orchestrator.

Usage (interactive — recommended):
    python -m agents.enrichment.agent

Usage (scripted):
    python -m agents.enrichment.agent --campaign <uuid> [--dry-run] [--limit N]

Philosophy: enrichment is about scraping ALL possible contact information for a
business. The downstream email generation step picks who to contact. We write
one row per decision maker and one row per additional email found.

Per lead, the pipeline runs in two phases:

  LEAD-LEVEL (run once per lead):
    1. web_search       — 3× GPT+Brave → find ALL decision makers (up to 5)
    2. website_fetch    — read actual site pages; find email, LinkedIn *
    3. company_email    — 3× GPT+Brave → find business contact email(s)
    4. hunter_io        — domain search; find all emails on record *
    5. facebook         — Apify scrape of Facebook About for email
    6. icp_score        — score lead against ICP criteria (Haiku)

  PER-DM (loop over each DM found in step 1, up to 5):
    7. personal_email   — 3× GPT+Brave → find this person's personal email
    8. linkedin_verify  — verify LinkedIn profile (primary DM only, if found) *
    9. pick best email  — personal > hunter-match > website > facebook >
                          company > any-hunter > domain-guess *
   10. write row        — decision_makers upsert

  After DM loop:
   11. write remaining hunter / company emails not claimed by any DM

  * = skipped when the batch filter is set to "no website" leads, or when
      the individual lead has no website field.

Concurrency: asyncio + Semaphore(max_concurrent_workers) caps simultaneous
Anthropic calls. Each lead is one semaphore slot. GPT steps spawn their own
internal thread pools for the 3× parallel runs.

Error handling:
    - Each step is isolated; one step failing never kills the lead
    - CREDENTIAL_INVALID on Anthropic/Supabase → hard abort
    - CREDENTIAL_INVALID on Hunter/Apify → warning, fallback applied
    - Rate limits → exponential backoff, auto-retry up to 4 times
    - Stale in_progress leads → reaper re-queues after 15 min
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import queue
import random
import time
import uuid
from typing import Optional

import anthropic
import openai
from dotenv import load_dotenv
from openai import OpenAI
from rich.console import Console
from rich.panel import Panel
from supabase import Client, create_client

from agents.config import CampaignConfig, EnrichmentConfig, load_campaign_config
from agents.results import EnrichmentRunResult
from agents.quota import (
    CredentialInvalidError,
    QuotaExhaustedError,
    CreditsLowError,
    claim_hunter_request,
    run_preflight,
)
from agents.enrichment.steps import (
    run_web_search,
    run_personal_email_search,
    run_company_email_search,
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

console = Console()

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_hunter_quota_exhausted = False
_anthropic_semaphore: Optional[asyncio.Semaphore] = None
_hard_stop: bool = False
_hard_stop_reason: str = ""


class _HardStopSignal(Exception):
    pass


def _signal_hard_stop(reason: str) -> None:
    global _hard_stop, _hard_stop_reason
    _hard_stop = True
    _hard_stop_reason = reason
    print(f"\n  ✗ HARD STOP — {reason}")
    print("  The run is paused. Fix the issue above, then it will auto-resume.\n")


def _reset_hard_stop() -> None:
    global _hard_stop, _hard_stop_reason
    _hard_stop = False
    _hard_stop_reason = ""


def _wait_for_credentials(supabase: Client, org_id: str, has_website: bool | None = None, poll_seconds: int = 30) -> None:
    print(f"  Checking every {poll_seconds}s… (Ctrl+C to abort)\n")
    while True:
        time.sleep(poll_seconds)
        print("  Re-checking credentials…")
        checks = run_preflight(supabase, org_id, has_website=has_website)
        if not any(not c["ok"] and not c.get("skipped") for c in checks):
            print("  ✓ All credentials valid — resuming.\n")
            return
        print(f"  Still failing. Retrying in {poll_seconds}s…\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dm_full_name(dm: dict) -> str:
    first = (dm.get("firstName") or "").strip()
    last  = (dm.get("lastName") or "").strip()
    return f"{first} {last}".strip()


def _extract_city(location: str) -> str:
    """
    Reduce a full street address to city/suburb + state for web search queries.
    '105 Landsborough Street, Normanton, QLD 4890' → 'Normanton, QLD'
    'Keswick, Keswick, SA 5035'                    → 'Keswick, SA'
    'Adelaide, SA'                                 → 'Adelaide, SA'
    Falls back to the original string if parsing yields nothing useful.
    """
    import re as _re
    if not location:
        return ""
    parts = [p.strip() for p in location.split(",")]
    # Drop parts that start with a digit (street number/name) or are purely numeric (postcodes)
    parts = [p for p in parts if p and not p[:1].isdigit()]
    if not parts:
        return location
    # Strip trailing 4–5 digit postcode from each token (e.g. "QLD 4890" → "QLD")
    parts = [_re.sub(r"\s+\d{4,5}$", "", p).strip() for p in parts]
    parts = [p for p in parts if p]
    if not parts:
        return location
    # Prefer last two tokens: suburb + state abbreviation
    return ", ".join(parts[-2:]) if len(parts) >= 2 else parts[-1]


def _map_hunter_confidence(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _pick_best_email_for_dm(
    dm_name: str,
    personal_email: Optional[str],
    hunter_emails: list[dict],
    website_email: Optional[str],
    facebook_email: Optional[str],
    company_emails: list[str],
    lead: dict,
    config: EnrichmentConfig,
    skip_website_steps: bool,
    used_emails: set[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Pick the best available email for this DM, respecting the used_emails pool
    so the same address is never written to two rows.

    Priority:
      1. Personal email (GPT+Brave searched for this person specifically)
      2. Hunter email matched by name
      3. Website contact page email (first unclaimed)
      4. Facebook email (first unclaimed)
      5. Company email from GPT+Brave (next unclaimed)
      6. Any remaining Hunter email (next unclaimed)
      7. Domain guess (last resort, skipped for no-website batches)

    Returns (email, confidence, source) — all None if nothing available.
    """
    def claim(email: str, confidence: str, source: str):
        used_emails.add(email.lower())
        return email, confidence, source

    # 1. Personal email
    if personal_email and personal_email.lower() not in used_emails:
        return claim(personal_email, "medium", "gpt_brave")

    # 2. Hunter email matched by name
    if dm_name:
        dm_parts = set(dm_name.lower().split())
        for h in hunter_emails:
            h_email = (h.get("email") or "").strip()
            if not h_email or h_email.lower() in used_emails:
                continue
            h_first = (h.get("first_name") or "").lower().strip()
            h_last  = (h.get("last_name") or "").lower().strip()
            if h_first and h_last and h_first in dm_parts and h_last in dm_parts:
                return claim(h_email, _map_hunter_confidence(h.get("confidence", 0)), "hunter_io")

    # 3. Website email
    if not skip_website_steps and website_email and website_email.lower() not in used_emails:
        return claim(website_email, "medium", "contact_page")

    # 4. Facebook email
    if facebook_email and facebook_email.lower() not in used_emails:
        return claim(facebook_email, "medium", "facebook")

    # 5. Next unused company email
    for ce in company_emails:
        if ce.lower() not in used_emails:
            return claim(ce, "medium", "company_email_search")

    # 6. Next unused hunter email (unmatched)
    for h in hunter_emails:
        h_email = (h.get("email") or "").strip()
        if h_email and h_email.lower() not in used_emails:
            return claim(h_email, _map_hunter_confidence(h.get("confidence", 0)), "hunter_io")

    # 7. Domain guess
    if (
        not skip_website_steps
        and lead.get("website")
        and "domain_guess" in config.email_sources
        and dm_name
    ):
        guess = run_domain_guess(lead, dm_name)
        if guess.success and guess.email and guess.email.lower() not in used_emails:
            return claim(guess.email, "low", "domain_guess")

    return None, None, None


def _write_remaining_emails(
    supabase: Client,
    lead: dict,
    icp_score: int,
    lead_notes: str,
    campaign_name: str,
    hunter_emails: list[dict],
    company_emails: list[str],
    used_emails: set[str],
) -> None:
    """Write any hunter/company emails not claimed by a DM row."""
    for h in hunter_emails:
        h_email = (h.get("email") or "").strip()
        if not h_email or h_email.lower() in used_emails:
            continue
        h_name = f"{h.get('first_name', '')} {h.get('last_name', '')}".strip() or None
        _write_decision_maker(
            supabase, lead, h_name, h.get("position"), None,
            h_email, _map_hunter_confidence(h.get("confidence", 0)), "hunter_io",
            icp_score, "Additional email from Hunter.io domain search", campaign_name,
        )
        used_emails.add(h_email.lower())
        print(f"    + {h_name or 'Unknown'} — {h_email} (Hunter, unclaimed)")

    for ce in company_emails:
        if ce.lower() in used_emails:
            continue
        _write_decision_maker(
            supabase, lead, None, None, None,
            ce, "medium", "company_email_search",
            icp_score, "Company contact email from GPT+Brave search", campaign_name,
        )
        used_emails.add(ce.lower())
        print(f"    + company email — {ce}")


# ---------------------------------------------------------------------------
# Single-lead enrichment
# ---------------------------------------------------------------------------

async def enrich_lead(
    lead: dict,
    config: CampaignConfig,
    supabase: Client,
    anthropic_client: anthropic.Anthropic,
    openai_client: OpenAI,
    agent_session_id: str,
    lead_index: int,
    total_leads: int,
    progress_queue: Optional[queue.Queue] = None,
) -> str:
    """Returns status string: 'enriched' | 'no_dm' | 'no_result' | 'failed'."""
    global _hunter_quota_exhausted

    lead_id  = lead["id"]
    business = lead.get("business_name", "?")
    prefix   = f"[{lead_index}/{total_leads}] {business}"

    # Website steps are skipped when the batch explicitly targets no-website leads,
    # OR when this individual lead has no website recorded.
    skip_website_steps = config.lead_filter.has_website is False
    lead_has_website   = bool(lead.get("website"))

    # Normalised location for search queries — city/suburb + state only.
    # Full street addresses produce worse Brave queries than "Normanton, QLD".
    search_lead = {**lead, "location": _extract_city(lead.get("location", ""))}

    job_id     = _start_job(supabase, lead_id, config)
    steps_log: dict[str, str] = {}
    total_cost = 0.0
    start      = time.monotonic()

    async def run_with_backoff(fn, *args, **kwargs) -> StepResult:
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(5):
            try:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
            except (anthropic.RateLimitError, openai.RateLimitError) as exc:
                last_exc = exc
                wait = (2 ** attempt) + random.uniform(0, 1)
                log.info("%s — rate limit, backing off %.1fs (attempt %d)", prefix, wait, attempt + 1)
                await asyncio.sleep(wait)
        # All retries exhausted — return a failed StepResult instead of propagating
        log.error("%s — rate limit persisted after 5 retries: %s", prefix, last_exc)
        return StepResult(
            step="rate_limited",
            success=False,
            error_code="RATE_LIMITED",
            error_message=str(last_exc)[:300],
        )

    def log_step(result: StepResult) -> None:
        nonlocal total_cost
        total_cost += result.cost_usd
        steps_log[result.step] = result.error_code if not result.success else "ok"
        if not result.success and result.error_code not in ("NO_URL", "NO_DOMAIN", "NO_NAME", "NO_RESULT"):
            log.warning("%s — step %s: %s", prefix, result.step, result.error_message)

    def emit_step(result: StepResult) -> None:
        """Log step and stream a structured debug event to the UI."""
        log_step(result)
        raw = result.raw or {}
        # Compute a human-readable "found" summary
        parts = []
        if result.name:  parts.append(result.name)
        if result.email: parts.append(result.email)
        if result.title: parts.append(result.title)
        if not parts and result.notes:
            parts.append(result.notes[:80])
        _emit(progress_queue, {
            "type":          "step",
            "business_name": business,
            "step":          result.step,
            "success":       result.success,
            "found":         ", ".join(parts) if parts else None,
            "notes":         result.notes or result.error_message,
            "error_code":    result.error_code,
            "duration_sec":  round(result.duration_sec, 2),
            "cost_usd":      round(result.cost_usd, 5),
            "brave_queries": raw.get("brave_queries", []),
            "gpt_turns":     raw.get("gpt_turns"),
            "agent_runs":    raw.get("agent_runs"),
            "domain":        raw.get("domain"),
            "endpoint":      raw.get("endpoint"),
        })

    try:
        async with _anthropic_semaphore:

            # ── 1. DM Discovery ───────────────────────────────────────────
            web_result = await run_with_backoff(run_web_search, search_lead, openai_client, config.enrichment)
            emit_step(web_result)
            all_dms: list[dict] = []
            lead_notes: list[str] = []
            if web_result.success and web_result.raw:
                all_dms = web_result.raw.get("all_dms", [])[:5]
                lead_notes.append(f"[web_search] {web_result.notes}")

            # ── 2. Website fetch ──────────────────────────────────────────
            website_email:    Optional[str] = None
            website_linkedin: Optional[str] = None
            if not skip_website_steps and lead_has_website:
                primary_name = _dm_full_name(all_dms[0]) if all_dms else None
                r = await run_with_backoff(run_website_fetch, lead, anthropic_client, config.enrichment, primary_name)
                emit_step(r)
                if r.success:
                    website_email    = r.email
                    website_linkedin = r.linkedin_url
                    if r.notes:
                        lead_notes.append(f"[website_fetch] {r.notes}")

            # ── 3. Company email search ───────────────────────────────────
            company_emails: list[str] = []
            r = await run_with_backoff(run_company_email_search, search_lead, openai_client, config.enrichment)
            emit_step(r)
            if r.success and r.raw:
                company_emails = r.raw.get("all_emails", [])
                lead_notes.append(f"[company_email] {len(company_emails)} email(s) found: {', '.join(company_emails)}")

            # ── 4. Hunter.io domain search ────────────────────────────────
            hunter_emails: list[dict] = []
            if not skip_website_steps and lead_has_website and not _hunter_quota_exhausted:
                loop = asyncio.get_event_loop()
                quota_granted = await loop.run_in_executor(
                    None, lambda: claim_hunter_request(supabase, config.org_id)
                )
                if quota_granted:
                    try:
                        r = await loop.run_in_executor(None, lambda: run_hunter_lookup(lead, None))
                        emit_step(r)
                        if r.success and r.raw:
                            hunter_emails = r.raw.get("all_emails", [])
                            lead_notes.append(f"[hunter] {len(hunter_emails)} email(s) on domain")
                    except QuotaExhaustedError:
                        steps_log["hunter_io"] = "QUOTA_EXHAUSTED"
                        _signal_hard_stop("Hunter.io monthly quota exhausted.")
                        raise _HardStopSignal("Hunter.io QUOTA_EXHAUSTED")
                    except CredentialInvalidError as e:
                        steps_log["hunter_io"] = "CREDENTIAL_INVALID"
                        _alert_slack(str(e), "hunter_io", config.campaign_name, lead_index)
                        _signal_hard_stop("Hunter.io CREDENTIAL_INVALID — update HUNTER_IO_API_KEY in .env")
                        raise _HardStopSignal(str(e))
                else:
                    steps_log["hunter_io"] = "QUOTA_EXHAUSTED"

            # ── 5. Facebook scrape ────────────────────────────────────────
            facebook_email: Optional[str] = None
            try:
                loop = asyncio.get_event_loop()
                r = await loop.run_in_executor(None, lambda: run_facebook_scrape(lead))
                emit_step(r)
                if r.success:
                    facebook_email = r.email
                    lead_notes.append(f"[facebook] {r.email}")
            except CreditsLowError as e:
                steps_log["facebook"] = "CREDITS_LOW"
                _signal_hard_stop(f"Apify credits critically low ({e})")
                raise _HardStopSignal(str(e))
            except CredentialInvalidError as e:
                steps_log["facebook"] = "CREDENTIAL_INVALID"
                _alert_slack(str(e), "apify", config.campaign_name, lead_index)
                _signal_hard_stop("Apify CREDENTIAL_INVALID — update APIFY_API_KEY in .env")
                raise _HardStopSignal(str(e))

            # ── 6. ICP score ──────────────────────────────────────────────
            r = await run_with_backoff(run_icp_score, lead, anthropic_client, config.enrichment, "\n".join(lead_notes))
            emit_step(r)
            icp_score = extract_icp_score(r)
            if r.notes:
                lead_notes.append(f"[icp] {r.notes}")

            lead_research_notes = "\n".join(lead_notes)

            # ── 7–10. Per-DM enrichment loop ──────────────────────────────
            used_emails: set[str] = set()

            if not all_dms:
                # No named DMs found — still write any contact emails we found
                _write_remaining_emails(
                    supabase, lead, icp_score, lead_research_notes,
                    config.campaign_name, hunter_emails, company_emails, used_emails,
                )
                if website_email and website_email.lower() not in used_emails:
                    _write_decision_maker(
                        supabase, lead, None, None, None,
                        website_email, "medium", "contact_page",
                        icp_score, lead_research_notes, config.campaign_name,
                    )
                    used_emails.add(website_email.lower())
                if facebook_email and facebook_email.lower() not in used_emails:
                    _write_decision_maker(
                        supabase, lead, None, None, None,
                        facebook_email, "medium", "facebook",
                        icp_score, lead_research_notes, config.campaign_name,
                    )

                if used_emails or hunter_emails or company_emails or website_email or facebook_email:
                    _mark_lead_enriched(supabase, lead_id)
                    print(f"  ~ [{lead_index}/{total_leads}] {business} — no DM name found — ICP: {icp_score}")
                    _emit(progress_queue, {"type": "lead", "business_name": business,
                                           "dm_name": None, "email": next(iter(used_emails), None),
                                           "icp_score": icp_score, "status": "no_dm"})
                    return "no_dm"
                else:
                    _mark_lead_failed(supabase, lead_id, "No DM or email found after all steps")
                    print(f"  ✗ [{lead_index}/{total_leads}] {business} — no result")
                    _emit(progress_queue, {"type": "lead", "business_name": business,
                                           "dm_name": None, "email": None,
                                           "icp_score": icp_score, "status": "no_result"})
                    return "no_result"

            else:
                for i, dm in enumerate(all_dms):
                    dm_name  = _dm_full_name(dm)
                    dm_title = (dm.get("position") or "").strip()
                    dm_notes = [lead_research_notes]

                    # 7. Personal email search
                    personal_email: Optional[str] = None
                    if dm_name:
                        r = await run_with_backoff(
                            run_personal_email_search,
                            search_lead, dm_name, dm_title, openai_client, config.enrichment,
                        )
                        emit_step(r)
                        if r.success:
                            personal_email = r.email
                            dm_notes.append(f"[personal_email] {r.notes}")

                    # 8. LinkedIn verify (primary DM only)
                    dm_linkedin: Optional[str] = None
                    if i == 0 and website_linkedin and not skip_website_steps:
                        r = await run_with_backoff(
                            run_linkedin_verify,
                            lead, anthropic_client, config.enrichment, website_linkedin, dm_name,
                        )
                        emit_step(r)
                        if r.success:
                            dm_linkedin = website_linkedin
                            if r.title:
                                dm_title = r.title
                            if r.name and not dm_name:
                                dm_name = r.name

                    # 9. Pick best email
                    best_email, confidence, source = _pick_best_email_for_dm(
                        dm_name=dm_name,
                        personal_email=personal_email,
                        hunter_emails=hunter_emails,
                        website_email=website_email,
                        facebook_email=facebook_email,
                        company_emails=company_emails,
                        lead=lead,
                        config=config.enrichment,
                        skip_website_steps=skip_website_steps,
                        used_emails=used_emails,
                    )

                    # 10. Write DM row
                    _write_decision_maker(
                        supabase, lead,
                        dm_name or None, dm_title or None, dm_linkedin,
                        best_email, confidence, source,
                        icp_score, "\n".join(dm_notes)[:2000], config.campaign_name,
                    )

                    icon   = "✓" if best_email else "~"
                    suffix = f"— {best_email}" if best_email else "— no email"
                    print(f"  {icon} [{lead_index}/{total_leads}] {business} — {dm_name or '?'} {suffix} — ICP: {icp_score}")
                    if i == 0:  # emit once per lead using the primary DM
                        _emit(progress_queue, {"type": "lead", "business_name": business,
                                               "dm_name": dm_name or None, "email": best_email,
                                               "icp_score": icp_score, "status": "enriched"})

                _mark_lead_enriched(supabase, lead_id)

                # 11. Write any remaining unclaimed emails
                _write_remaining_emails(
                    supabase, lead, icp_score, lead_research_notes,
                    config.campaign_name, hunter_emails, company_emails, used_emails,
                )

    except _HardStopSignal:
        _mark_lead_failed(supabase, lead_id, _hard_stop_reason or "Hard stop triggered")
        _finish_job(supabase, job_id, "failed", steps_log, total_cost, time.monotonic() - start)
        _emit(progress_queue, {"type": "lead", "business_name": business,
                               "dm_name": None, "email": None, "icp_score": 0, "status": "failed"})
        return "failed"
    except Exception as e:
        log.error("%s — unexpected error: %s", prefix, e, exc_info=True)
        steps_log["orchestrator"] = f"UNEXPECTED: {e}"
        _mark_lead_failed(supabase, lead_id, str(e))
        _finish_job(supabase, job_id, "failed", steps_log, total_cost, time.monotonic() - start)
        _emit(progress_queue, {"type": "lead", "business_name": business,
                               "dm_name": None, "email": None, "icp_score": 0, "status": "failed"})
        return "failed"

    _finish_job(supabase, job_id, "done", steps_log, total_cost, time.monotonic() - start)
    return "enriched"


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

async def run_batch(
    supabase: Client,
    config: CampaignConfig,
    anthropic_client: anthropic.Anthropic,
    openai_client: OpenAI,
    dry_run: bool = False,
    limit: Optional[int] = None,
    headless: bool = False,
    progress_queue: Optional[queue.Queue] = None,
) -> EnrichmentRunResult:
    global _anthropic_semaphore, _hunter_quota_exhausted
    _hunter_quota_exhausted = False
    _anthropic_semaphore = asyncio.Semaphore(config.enrichment.max_concurrent_workers)

    reaper_result = supabase.rpc("reap_stale_locks", {"p_campaign_id": config.campaign_id}).execute()
    log.info("Reaper complete")

    count_q = (
        supabase.table("leads")
        .select("id", count="exact")
        .eq("campaign_id", config.campaign_id)
        .eq("enrichment_status", "queued")
        .lt("retry_count", 3)
    )
    if config.lead_filter.has_website is True:
        count_q = count_q.neq("website", "").not_.is_("website", "null")
    elif config.lead_filter.has_website is False:
        count_q = count_q.or_("website.is.null,website.eq.")

    total_queued = (count_q.execute().count or 0)
    if limit:
        total_queued = min(total_queued, limit)

    from agents.quota import get_hunter_quota_status
    quota = get_hunter_quota_status(supabase, config.org_id)
    est_cost_low  = total_queued * 0.03
    est_cost_high = total_queued * config.enrichment.max_cost_per_lead_usd
    est_time_min  = (total_queued * 45) / (config.enrichment.max_concurrent_workers * 60)

    print(f"\n  Lead Enrichment {'— DRY RUN' if dry_run else ''}")
    print(f"  {'─' * 50}")
    print(f"  Campaign:         {config.campaign_name}")
    print(f"  Filter:           has_website = {config.lead_filter.has_website}")
    print(f"  Website steps:    {'DISABLED (no-website batch)' if config.lead_filter.has_website is False else 'enabled'}")
    print(f"  Leads queued:     {total_queued}")
    print(f"  Research depth:   {config.enrichment.research_depth}")
    print(f"  Workers:          {config.enrichment.max_concurrent_workers}")
    print(f"  Est. cost:        ${est_cost_low:.2f}–${est_cost_high:.2f}")
    print(f"  Est. time:        ~{est_time_min:.0f} minutes")
    print(f"  Hunter quota:     {quota['used']}/{quota['limit']} used this month")
    print()
    _emit(progress_queue, {"type": "log", "text": f"Campaign: {config.campaign_name}"})
    _emit(progress_queue, {"type": "log", "text": f"{total_queued} leads queued · est. ${est_cost_low:.2f}–${est_cost_high:.2f} · ~{est_time_min:.0f} min"})

    result = EnrichmentRunResult(campaign_id=config.campaign_id)

    if dry_run:
        print("  Dry run complete. Pass --no-dry-run to execute.")
        _emit(progress_queue, {"type": "done", "total": total_queued, "enriched": 0,
                               "failed": 0, "dead": 0, "dry_run": True})
        return result

    if total_queued == 0:
        print("  No leads to process.")
        _emit(progress_queue, {"type": "done", "total": 0, "enriched": 0,
                               "failed": 0, "dead": 0, "dry_run": False})
        return result

    if not headless:
        answer = input("  Proceed? [y/N]: ").strip().lower()
        if answer not in ("y", "yes"):
            print("  Aborted.")
            return result

    print(f"\n  Starting enrichment for {total_queued} leads...\n")

    agent_session_id = str(uuid.uuid4())
    processed = 0

    while processed < total_queued:
        claim_size = min(config.enrichment.max_concurrent_workers, total_queued - processed)
        claimed = supabase.rpc("claim_leads_for_enrichment", {
            "p_campaign_id": config.campaign_id,
            "p_agent_id": agent_session_id,
            "p_batch_size": claim_size,
        }).execute()

        leads = claimed.data or []
        if not leads:
            break

        tasks = [
            enrich_lead(
                lead=lead,
                config=config,
                supabase=supabase,
                anthropic_client=anthropic_client,
                openai_client=openai_client,
                agent_session_id=agent_session_id,
                lead_index=processed + i + 1,
                total_leads=total_queued,
                progress_queue=progress_queue,
            )
            for i, lead in enumerate(leads)
        ]
        statuses = await asyncio.gather(*tasks)
        processed += len(leads)
        for s in statuses:
            if s in ("enriched", "no_dm"):
                result.leads_enriched += 1
            elif s in ("failed", "no_result"):
                result.leads_failed += 1

        if _hard_stop:
            _wait_for_credentials(supabase, config.org_id, has_website=config.lead_filter.has_website)
            _reset_hard_stop()

    result.leads_processed = processed
    print(f"\n  Done. Processed {processed} leads.")
    _emit(progress_queue, {"type": "done", "total": processed,
                           "enriched": result.leads_enriched, "failed": result.leads_failed,
                           "dead": result.leads_dead, "dry_run": False})
    return result


# ---------------------------------------------------------------------------
# Supabase helpers
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
        "research_notes": research_notes[:2000],
    }, on_conflict="lead_id,email").execute()


def _emit(q: Optional[queue.Queue], event: dict) -> None:
    """Put a structured progress event onto the queue when running from the UI."""
    if q is not None:
        q.put(event)


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
        pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _run_interactive_form(supabase: Client, args: argparse.Namespace) -> argparse.Namespace:
    console.print(Panel.fit(
        "[bold cyan]Titan Systems — Lead Enrichment[/bold cyan]\n"
        "Find decision-makers, emails, and ICP scores for queued leads.",
        border_style="cyan",
    ))
    console.print()

    campaigns = (
        supabase.table("campaigns")
        .select("id, name, status")
        .order("name")
        .execute()
    ).data or []

    if not campaigns:
        console.print("  [yellow]No campaigns found in Supabase.[/yellow]")
        console.print("  Create one at your Supabase dashboard first, then re-run.\n")
        import sys; sys.exit(1)

    console.print("  [bold]Available campaigns:[/bold]")
    for i, c in enumerate(campaigns, 1):
        colour = "green" if c["status"] == "active" else "yellow"
        console.print(
            f"  [dim]{i}.[/dim] {c['name']}  "
            f"[{colour}]{c['status']}[/{colour}]  "
            f"[dim]{c['id']}[/dim]"
        )
    console.print()

    while True:
        raw = input("  Select campaign number (or paste UUID): ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(campaigns):
            args.campaign = campaigns[int(raw) - 1]["id"]
            break
        elif raw:
            args.campaign = raw
            break

    console.print()
    console.print("  [bold]Website filter:[/bold]")
    console.print("  [dim]1.[/dim] All leads")
    console.print("  [dim]2.[/dim] Leads with a website only")
    console.print("  [dim]3.[/dim] Leads without a website only")
    console.print()
    while True:
        raw_filter = input("  Select filter [1]: ").strip() or "1"
        if raw_filter == "1":
            args.has_website = None
            break
        elif raw_filter == "2":
            args.has_website = True
            break
        elif raw_filter == "3":
            args.has_website = False
            break
        console.print("  [red]Enter 1, 2, or 3.[/red]")

    console.print()
    raw_limit = input("  Max leads to process (leave blank for all): ").strip()
    args.limit = int(raw_limit) if raw_limit.isdigit() else None

    raw_dry = input("  Dry-run first? (preview without processing) [y/N]: ").strip().lower()
    args.dry_run = raw_dry in ("y", "yes")

    console.print()
    return args


def main() -> None:
    import sys
    parser = argparse.ArgumentParser(
        description="Enrichment Agent — run without --campaign to use the interactive form",
    )
    parser.add_argument("--campaign", default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--headless", action="store_true", default=False)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--has-website", choices=["true", "false"], default=None)
    args = parser.parse_args()

    if not hasattr(args, "has_website") or args.has_website is None:
        args.has_website = None
    elif isinstance(args.has_website, str):
        args.has_website = args.has_website == "true"

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    openai_client    = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    if not args.campaign:
        args = _run_interactive_form(supabase, args)

    org_id = _get_org_id(supabase, args.campaign)
    while True:
        checks = run_preflight(supabase, org_id, has_website=args.has_website)
        if not any(not c["ok"] and not c.get("skipped") for c in checks):
            break
        print("\n  ✗ One or more credentials failed. Fix the issues above.\n"
              "  Retrying in 30s… (Ctrl+C to abort)\n")
        time.sleep(30)

    config = load_campaign_config(supabase, args.campaign)

    if args.has_website is not None:
        config.lead_filter.has_website = args.has_website

    try:
        asyncio.run(run_batch(supabase, config, anthropic_client, openai_client,
                              args.dry_run, args.limit, args.headless))

        if args.dry_run and not args.headless:
            console.print()
            raw = input("  Proceed with real run? [y/N]: ").strip().lower()
            if raw in ("y", "yes"):
                asyncio.run(run_batch(supabase, config, anthropic_client, openai_client,
                                      dry_run=False, limit=args.limit, headless=args.headless))
            else:
                console.print("  Exiting.\n")

    except Exception as e:
        log.error("Enrichment run failed: %s", e)
        sys.exit(1)
    sys.exit(0)


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
