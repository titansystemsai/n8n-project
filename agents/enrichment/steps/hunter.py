"""
Step 4a — Hunter.io Email Lookup.

Direct API call — no LLM involved.
Returns the most confident email for the domain.
Quota-tracked: caller must call claim_hunter_request() before invoking this step.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Optional

import httpx

from ._base import StepResult
from agents.quota import (
    CredentialInvalidError,
    QuotaExhaustedError,
    ServiceDownError,
)

log = logging.getLogger(__name__)

HUNTER_DOMAIN_SEARCH_URL = "https://api.hunter.io/v2/domain-search"
HUNTER_EMAIL_FINDER_URL  = "https://api.hunter.io/v2/email-finder"

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')


def run_hunter_lookup(
    lead: dict,
    name: Optional[str] = None,
) -> StepResult:
    """
    Look up the domain in Hunter.io.

    If a name is provided, tries the Email Finder endpoint (person-specific).
    Otherwise falls back to Domain Search (returns all emails for the domain).

    The caller must have already claimed a quota slot via claim_hunter_request().
    """
    start = time.monotonic()
    api_key = os.environ.get("HUNTER_IO_API_KEY", "")
    website = lead.get("website", "")

    if not website:
        return StepResult(
            step="hunter_io", success=False,
            error_code="NO_DOMAIN",
            error_message="No website URL — Hunter.io requires a domain",
            duration_sec=time.monotonic() - start,
        )

    domain = _extract_domain(website)

    try:
        all_emails: list[dict] = []
        if name and " " in name:
            parts = name.strip().split()
            first, last = parts[0], parts[-1]
            data, confidence = _email_finder(api_key, domain, first, last)
        else:
            data, confidence, all_emails = _domain_search(api_key, domain, name)

        if not data:
            return StepResult(
                step="hunter_io", success=False,
                error_code="NO_RESULT",
                error_message=f"Hunter.io found no emails for domain: {domain}",
                duration_sec=time.monotonic() - start,
            )

        endpoint = "email_finder" if (name and " " in name) else "domain_search"
        return StepResult(
            step="hunter_io",
            success=True,
            email=data.get("email"),
            email_confidence=_map_confidence(confidence),
            email_source="hunter_io",
            name=((data.get("first_name") or "") + " " + (data.get("last_name") or "")).strip() or None,
            title=data.get("position"),
            notes=f"{endpoint} · {domain} · {len(all_emails) or 1} email(s) · score {confidence}",
            raw={"primary": data, "all_emails": all_emails, "domain": domain, "endpoint": endpoint},
            duration_sec=time.monotonic() - start,
        )

    except CredentialInvalidError:
        raise
    except QuotaExhaustedError:
        raise
    except ServiceDownError:
        raise
    except Exception as e:
        return StepResult(
            step="hunter_io", success=False,
            error_code="SERVICE_DOWN",
            error_message=str(e),
            duration_sec=time.monotonic() - start,
        )


def _email_finder(api_key: str, domain: str, first: str, last: str) -> tuple[Optional[dict], int]:
    """Hunter Email Finder — most accurate when we have a name."""
    resp = httpx.get(
        HUNTER_EMAIL_FINDER_URL,
        params={"domain": domain, "first_name": first, "last_name": last, "api_key": api_key},
        timeout=10,
    )
    _raise_for_hunter_status(resp)
    data = resp.json().get("data", {})
    email = data.get("email")
    if not email or not EMAIL_REGEX.match(email):
        return None, 0
    return data, data.get("score", 0)


def _domain_search(api_key: str, domain: str, name: Optional[str]) -> tuple[Optional[dict], int, list[dict]]:
    """Hunter Domain Search — returns best email as primary + all valid emails for writing."""
    resp = httpx.get(
        HUNTER_DOMAIN_SEARCH_URL,
        params={"domain": domain, "api_key": api_key, "limit": 10},
        timeout=10,
    )
    _raise_for_hunter_status(resp)
    data = resp.json().get("data", {})
    emails = data.get("emails", [])
    if not emails:
        return None, 0, []

    priority_titles = {"owner", "director", "founder", "principal", "partner", "ceo", "managing"}

    def score_email(e: dict) -> tuple:
        title = (e.get("position") or "").lower()
        title_match = any(t in title for t in priority_titles)
        return (title_match, e.get("confidence", 0))

    valid = [e for e in emails if EMAIL_REGEX.match(e.get("email", ""))]
    if not valid:
        return None, 0, []

    best = max(valid, key=score_email)
    return best, best.get("confidence", 0), valid


def _raise_for_hunter_status(resp: httpx.Response) -> None:
    if resp.status_code == 401:
        raise CredentialInvalidError(
            "HUNTER_IO_API_KEY is invalid or expired. Update in .env"
        )
    if resp.status_code == 429:
        raise QuotaExhaustedError(
            "Hunter.io monthly quota exhausted. Fallback steps will be used."
        )
    if resp.status_code >= 500:
        raise ServiceDownError(f"Hunter.io service error: {resp.status_code}")


def _extract_domain(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path
    return domain.lstrip("www.").split("/")[0]


def _map_confidence(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"
