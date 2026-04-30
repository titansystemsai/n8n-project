"""
API quota and credit management.

Responsibilities:
- Track Hunter.io monthly request count (100/month free tier)
- Check Apify credit balance before starting a batch
- Classify API errors into actionable error codes
- Provide a pre-flight credential validator

Error codes (used in enrichment_jobs.steps_completed and CLI output):
    CREDENTIAL_INVALID  — 401: key is wrong or expired
    QUOTA_EXHAUSTED     — 429: monthly limit hit, won't retry
    CREDITS_LOW         — Apify balance below threshold
    RATE_LIMITED        — 429 that should back off and retry
    SERVICE_DOWN        — 5xx: transient, retry on next cycle
    TIMEOUT             — actor/request timed out
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import httpx
from supabase import Client

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom exceptions — callers differentiate these instead of catching Exception
# ---------------------------------------------------------------------------

class APIError(Exception):
    """Base for all classified API errors."""
    code: str = "UNKNOWN"

class CredentialInvalidError(APIError):
    code = "CREDENTIAL_INVALID"

class QuotaExhaustedError(APIError):
    code = "QUOTA_EXHAUSTED"

class CreditsLowError(APIError):
    code = "CREDITS_LOW"

class RateLimitedError(APIError):
    code = "RATE_LIMITED"

class ServiceDownError(APIError):
    code = "SERVICE_DOWN"

class TimeoutError(APIError):
    code = "TIMEOUT"

class BillingError(APIError):
    """402 or account-suspended response — subscription lapsed or payment failed."""
    code = "BILLING_ERROR"


# ---------------------------------------------------------------------------
# Hunter.io quota — atomic claim via Supabase RPC
# ---------------------------------------------------------------------------

HUNTER_MONTHLY_LIMIT = 100  # free tier

def claim_hunter_request(supabase: Client, org_id: str) -> bool:
    """
    Atomically claim one Hunter.io request against the monthly quota.
    Returns True if the request is granted, False if quota is exhausted.
    Does NOT raise — caller checks the return value and skips or falls through.
    """
    result = supabase.rpc("claim_api_quota", {
        "p_org_id": org_id,
        "p_service": "hunter_io",
        "p_limit": HUNTER_MONTHLY_LIMIT,
    }).execute()
    granted = bool(result.data)
    if not granted:
        log.warning("Hunter.io quota exhausted for org %s", org_id)
    return granted


def get_hunter_quota_status(supabase: Client, org_id: str) -> dict:
    """Return current Hunter.io usage for this billing period (for pre-flight display)."""
    period = datetime.now(timezone.utc).strftime("%Y-%m-01")
    result = (
        supabase.table("api_quota_usage")
        .select("requests_used, requests_limit")
        .eq("org_id", org_id)
        .eq("service", "hunter_io")
        .eq("period", period)
        .maybe_single()
        .execute()
    )
    if result and result.data:
        return {
            "used": result.data["requests_used"],
            "limit": result.data["requests_limit"] or HUNTER_MONTHLY_LIMIT,
        }
    return {"used": 0, "limit": HUNTER_MONTHLY_LIMIT}


# ---------------------------------------------------------------------------
# Apify credit check — circuit breaker before each batch
# ---------------------------------------------------------------------------

APIFY_MIN_CREDITS = 10  # abort batch if remaining credits fall below this

def check_apify_credits() -> float:
    """
    Fetch remaining Apify credits. Raises CreditsLowError if below threshold.
    Call once before starting a batch — not per lead.
    """
    api_key = os.environ["APIFY_API_KEY"]
    try:
        resp = httpx.get(
            "https://api.apify.com/v2/users/me",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
    except httpx.TimeoutException:
        raise TimeoutError("Apify account API timed out during credit check")

    if resp.status_code == 401:
        raise CredentialInvalidError(
            "APIFY_API_KEY is invalid or expired. Update in .env"
        )
    if not resp.is_success:
        raise ServiceDownError(f"Apify account API returned {resp.status_code}")

    data = resp.json().get("data", {})
    # Apify reports credits under different keys depending on plan.
    # If the field is absent (free plan / new account), credits are unknown — don't fail.
    credits_raw = (
        data.get("monthlyUsage", {}).get("monthlyUsageCredits", {}).get("remaining")
        or data.get("limits", {}).get("monthlyUsageCreditsUsd")
    )
    if credits_raw is None:
        # Credits not reported by this plan — assume functional, skip threshold check.
        return -1.0

    credits = float(credits_raw)
    if credits < APIFY_MIN_CREDITS:
        raise CreditsLowError(
            f"Apify credits critically low: {credits:.2f} remaining. "
            "Top up at https://console.apify.com/billing before running Facebook scrape."
        )

    return credits


# ---------------------------------------------------------------------------
# Credential pre-flight validator
# ---------------------------------------------------------------------------

def _check_hunter(api_key: str) -> tuple[bool, str]:
    try:
        resp = httpx.get(
            "https://api.hunter.io/v2/account",
            params={"api_key": api_key},
            timeout=8,
        )
        if resp.status_code == 401:
            return False, "CREDENTIAL_INVALID — update HUNTER_IO_API_KEY in .env"
        if resp.status_code == 402:
            return False, "BILLING_ERROR — Hunter.io subscription lapsed or payment failed"
        if resp.status_code == 429:
            return False, "QUOTA_EXHAUSTED — monthly limit reached"
        if not resp.is_success:
            return False, f"SERVICE_DOWN ({resp.status_code})"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def _check_anthropic(api_key: str) -> tuple[bool, str]:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        client.models.list()
        return True, "valid"
    except Exception as e:
        msg = str(e).lower()
        if "401" in msg or "authentication" in msg:
            return False, "CREDENTIAL_INVALID — update ANTHROPIC_API_KEY in .env"
        if "402" in msg or "billing" in msg or "payment" in msg or "credit" in msg:
            return False, "BILLING_ERROR — Anthropic account billing issue; check console.anthropic.com"
        return False, f"unreachable ({e})"


def _check_supabase(url: str, service_key: str) -> tuple[bool, str]:
    try:
        resp = httpx.get(
            f"{url}/rest/v1/",
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
            },
            timeout=8,
        )
        if resp.status_code in (401, 403):
            return False, "CREDENTIAL_INVALID — update SUPABASE_SERVICE_ROLE_KEY in .env"
        if resp.status_code == 402:
            return False, "BILLING_ERROR — Supabase project paused (free tier limit); upgrade at supabase.com"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def _check_openai(api_key: str) -> tuple[bool, str]:
    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        client.models.list()
        return True, "valid"
    except Exception as e:
        msg = str(e).lower()
        if "401" in msg or "authentication" in msg or "incorrect api key" in msg:
            return False, "CREDENTIAL_INVALID — update OPENAI_API_KEY in .env"
        if "402" in msg or "billing" in msg or "quota" in msg:
            return False, "BILLING_ERROR — OpenAI account billing issue; check platform.openai.com"
        return False, f"unreachable ({e})"


def _check_brave(api_key: str) -> tuple[bool, str]:
    try:
        resp = httpx.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": "test", "count": 1},
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": api_key,
            },
            timeout=8,
        )
        if resp.status_code == 401:
            return False, "CREDENTIAL_INVALID — update BRAVE_API_KEY in .env"
        if resp.status_code == 429:
            return False, "QUOTA_EXHAUSTED — Brave monthly limit reached; recharge at api.search.brave.com"
        if not resp.is_success:
            return False, f"SERVICE_DOWN ({resp.status_code})"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def _check_apify(api_key: str) -> tuple[bool, str]:
    try:
        resp = httpx.get(
            "https://api.apify.com/v2/users/me",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=8,
        )
        if resp.status_code == 401:
            return False, "CREDENTIAL_INVALID — update APIFY_API_KEY in .env"
        if resp.status_code == 402:
            return False, "BILLING_ERROR — Apify subscription lapsed or payment failed; check console.apify.com"
        if not resp.is_success:
            return False, f"SERVICE_DOWN ({resp.status_code})"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def run_preflight(supabase: Client, org_id: str, has_website: bool | None = None) -> list[dict]:
    """
    Validate credentials required for the given run context.

    has_website controls which steps will actually execute:
      None  — all leads (all credentials checked)
      True  — website-only leads (all credentials checked)
      False — no-website leads (Hunter.io skipped — it is never called for these leads)

    Returns:
        List of dicts: [{name, ok, message, blocking, skipped}]
        'blocking' = True means the run must not proceed.
        'skipped'  = True means the credential is not used for this run type.
    """
    checks = []

    # Supabase (blocking always)
    ok, msg = _check_supabase(
        os.environ.get("SUPABASE_URL", ""),
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
    )
    checks.append({"name": "SUPABASE_SERVICE_ROLE_KEY", "ok": ok, "message": msg, "blocking": True, "skipped": False})

    # Anthropic (blocking — used for icp_score on all leads; also website_fetch/linkedin_verify with websites)
    ok, msg = _check_anthropic(os.environ.get("ANTHROPIC_API_KEY", ""))
    checks.append({"name": "ANTHROPIC_API_KEY", "ok": ok, "message": msg, "blocking": True, "skipped": False})

    # OpenAI (blocking — web_search, company_email_search, personal_email_search run for all leads)
    ok, msg = _check_openai(os.environ.get("OPENAI_API_KEY", ""))
    checks.append({"name": "OPENAI_API_KEY", "ok": ok, "message": msg, "blocking": True, "skipped": False})

    # Brave Search (blocking — all three GPT+Brave steps use it)
    ok, msg = _check_brave(os.environ.get("BRAVE_API_KEY", ""))
    checks.append({"name": "BRAVE_API_KEY", "ok": ok, "message": msg, "blocking": True, "skipped": False})

    # Hunter.io — only relevant when website steps run (needs a domain to search)
    if has_website is False:
        checks.append({
            "name": "HUNTER_IO_API_KEY",
            "ok": True,
            "message": "skipped — not used for no-website leads",
            "blocking": False,
            "skipped": True,
        })
    else:
        ok, msg = _check_hunter(os.environ.get("HUNTER_IO_API_KEY", ""))
        quota = get_hunter_quota_status(supabase, org_id)
        quota_note = f"  ({quota['used']}/{quota['limit']} used this month)"
        checks.append({
            "name": "HUNTER_IO_API_KEY",
            "ok": ok,
            "message": msg + (quota_note if ok else ""),
            "blocking": False,
            "skipped": False,
        })

    # Apify (non-blocking — Facebook scrape runs for all leads regardless of website filter)
    ok, msg = _check_apify(os.environ.get("APIFY_API_KEY", ""))
    if ok:
        try:
            credits = check_apify_credits()
            msg = "valid  (credits not reported — free plan)" if credits < 0 else f"valid  ({credits:.0f} credits remaining)"
        except CreditsLowError as e:
            ok = False
            msg = str(e)
        except Exception:
            msg = "valid (credit check unavailable)"
    checks.append({"name": "APIFY_API_KEY", "ok": ok, "message": msg, "blocking": False, "skipped": False})

    # Print formatted summary
    filter_label = {None: "all leads", True: "website leads only", False: "no-website leads only"}[has_website]
    print(f"\n  Pre-flight credential check  [{filter_label}]:")
    print("  " + "─" * 50)
    for c in checks:
        if c.get("skipped"):
            icon = "–"
        elif c["ok"]:
            icon = "✓"
        else:
            icon = "✗"
        pad = 32 - len(c["name"])
        suffix = "  ← BLOCKING" if not c["ok"] and c["blocking"] else ""
        print(f"  {icon} {c['name']}{' ' * pad}{c['message']}{suffix}")
    print()

    return checks


def has_blocking_failures(checks: list[dict]) -> bool:
    return any(not c["ok"] and c["blocking"] and not c.get("skipped") for c in checks)
