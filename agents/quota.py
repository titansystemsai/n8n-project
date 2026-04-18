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
    if result.data:
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
    if not resp.ok:
        raise ServiceDownError(f"Apify account API returned {resp.status_code}")

    data = resp.json().get("data", {})
    # Apify returns credits under different keys depending on plan
    credits = (
        data.get("monthlyUsage", {}).get("monthlyUsageCredits", {}).get("remaining")
        or data.get("limits", {}).get("monthlyUsageCreditsUsd")
        or 0
    )
    credits = float(credits)

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
        if resp.status_code == 429:
            return False, "QUOTA_EXHAUSTED — monthly limit reached"
        if not resp.ok:
            return False, f"SERVICE_DOWN ({resp.status_code})"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def _check_anthropic(api_key: str) -> tuple[bool, str]:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        # Minimal call — list models is the cheapest validation
        client.models.list()
        return True, "valid"
    except Exception as e:
        msg = str(e).lower()
        if "401" in msg or "authentication" in msg:
            return False, "CREDENTIAL_INVALID — update ANTHROPIC_API_KEY in .env"
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
        if not resp.ok:
            return False, f"SERVICE_DOWN ({resp.status_code})"
        return True, "valid"
    except Exception as e:
        return False, f"unreachable ({e})"


def run_preflight(supabase: Client, org_id: str) -> list[dict]:
    """
    Validate all credentials and quota before an enrichment run.
    Returns a list of check results. Prints a formatted summary.

    Returns:
        List of dicts: [{name, ok, message, blocking}]
        'blocking' = True means the run must not proceed.
    """
    checks = []

    # Supabase (blocking — agents cannot run without it)
    ok, msg = _check_supabase(
        os.environ.get("SUPABASE_URL", ""),
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
    )
    checks.append({"name": "SUPABASE_SERVICE_ROLE_KEY", "ok": ok, "message": msg, "blocking": True})

    # Anthropic (blocking — agents cannot reason without it)
    ok, msg = _check_anthropic(os.environ.get("ANTHROPIC_API_KEY", ""))
    checks.append({"name": "ANTHROPIC_API_KEY", "ok": ok, "message": msg, "blocking": True})

    # Hunter.io (non-blocking — fallback steps exist)
    ok, msg = _check_hunter(os.environ.get("HUNTER_IO_API_KEY", ""))
    quota = get_hunter_quota_status(supabase, org_id)
    quota_note = f"  ({quota['used']}/{quota['limit']} used this month)"
    checks.append({
        "name": "HUNTER_IO_API_KEY",
        "ok": ok,
        "message": msg + (quota_note if ok else ""),
        "blocking": False,
    })

    # Apify (non-blocking — Facebook step skipped if unavailable)
    ok, msg = _check_apify(os.environ.get("APIFY_API_KEY", ""))
    if ok:
        try:
            credits = check_apify_credits()
            msg = f"valid  ({credits:.0f} credits remaining)"
        except CreditsLowError as e:
            ok = False
            msg = str(e)
        except Exception:
            msg = "valid (credit check unavailable)"
    checks.append({"name": "APIFY_API_KEY", "ok": ok, "message": msg, "blocking": False})

    # Print formatted summary
    print("\n  Pre-flight credential check:")
    print("  " + "─" * 50)
    for c in checks:
        icon = "✓" if c["ok"] else "✗"
        pad = 32 - len(c["name"])
        suffix = "  ← BLOCKING" if not c["ok"] and c["blocking"] else ""
        print(f"  {icon} {c['name']}{' ' * pad}{c['message']}{suffix}")
    print()

    return checks


def has_blocking_failures(checks: list[dict]) -> bool:
    return any(not c["ok"] and c["blocking"] for c in checks)
