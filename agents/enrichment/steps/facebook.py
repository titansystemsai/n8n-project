"""
Step 4c — Facebook Page Scrape via Apify.

Goal: Find the business's Facebook page and extract any personal email from
the About section. Small business owners (realtors, tradespeople) frequently
list personal emails here that don't appear anywhere else.

Runs for EVERY lead (not just no-website leads) — this is by design.
Uses Brave Search to find the FB page URL, then Apify to scrape it.
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
    CreditsLowError,
    ServiceDownError,
    TimeoutError as ApifyTimeoutError,
)

log = logging.getLogger(__name__)

APIFY_RUN_URL     = "https://api.apify.com/v2/acts/apify~facebook-pages-scraper/runs"
BRAVE_SEARCH_URL  = "https://api.search.brave.com/res/v1/web/search"
APIFY_POLL_INTERVAL_SEC = 5
APIFY_MAX_WAIT_SEC = 90

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')


def run_facebook_scrape(lead: dict) -> StepResult:
    """
    1. Brave search for the business Facebook page URL.
    2. Apify Facebook Pages Scraper to extract About section.
    3. Parse any email from the About text.
    """
    start = time.monotonic()
    business = lead.get("business_name", "")
    location = lead.get("location", "")

    try:
        fb_url = _find_facebook_url(business, location)
        if not fb_url:
            return StepResult(
                step="facebook", success=False,
                error_code="NO_RESULT",
                error_message=f"No Facebook page found for '{business}'",
                duration_sec=time.monotonic() - start,
            )

        about_data = _scrape_facebook_page(fb_url)
        if not about_data:
            return StepResult(
                step="facebook", success=False,
                error_code="NO_RESULT",
                error_message="Facebook page scraped but no useful data returned",
                duration_sec=time.monotonic() - start,
            )

        email = _extract_email(about_data)
        if not email:
            return StepResult(
                step="facebook", success=False,
                error_code="NO_RESULT",
                error_message=f"Facebook page found ({fb_url}) but no email in About section",
                duration_sec=time.monotonic() - start,
            )

        return StepResult(
            step="facebook",
            success=True,
            email=email,
            email_confidence="medium",
            email_source="facebook",
            notes=f"Email extracted from Facebook About section: {fb_url}",
            raw=about_data,
            duration_sec=time.monotonic() - start,
            cost_usd=0.01,  # approximate Apify actor cost
        )

    except CredentialInvalidError:
        raise
    except CreditsLowError:
        raise
    except ApifyTimeoutError:
        return StepResult(
            step="facebook", success=False,
            error_code="TIMEOUT",
            error_message=f"Apify actor timed out for '{business}'. Facebook step skipped.",
            duration_sec=time.monotonic() - start,
        )
    except ServiceDownError as e:
        return StepResult(
            step="facebook", success=False,
            error_code="SERVICE_DOWN",
            error_message=str(e),
            duration_sec=time.monotonic() - start,
        )
    except Exception as e:
        return StepResult(
            step="facebook", success=False,
            error_code="SERVICE_DOWN",
            error_message=str(e),
            duration_sec=time.monotonic() - start,
        )


def _find_facebook_url(business: str, location: str) -> Optional[str]:
    """Brave Search for the Facebook page."""
    api_key = os.environ.get("BRAVE_API_KEY", "")
    query = f"{business} {location} site:facebook.com".strip()

    try:
        resp = httpx.get(
            BRAVE_SEARCH_URL,
            headers={"Accept": "application/json", "X-Subscription-Token": api_key},
            params={"q": query, "count": 5},
            timeout=10,
        )
    except httpx.TimeoutException:
        raise ServiceDownError("Brave Search timed out")

    if resp.status_code == 401:
        raise CredentialInvalidError(
            "BRAVE_API_KEY is invalid or expired. Update in .env"
        )
    if not resp.is_success:
        raise ServiceDownError(f"Brave Search returned {resp.status_code}")

    results = resp.json().get("web", {}).get("results", [])
    for r in results:
        url = r.get("url", "")
        if "facebook.com" in url and "/groups/" not in url and "/events/" not in url:
            return url
    return None


def _scrape_facebook_page(fb_url: str) -> Optional[dict]:
    """Run Apify Facebook Pages Scraper and wait for result."""
    api_key = os.environ.get("APIFY_API_KEY", "")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        run_resp = httpx.post(
            APIFY_RUN_URL,
            headers=headers,
            json={
                "startUrls": [{"url": fb_url}],
                "maxPosts": 0,           # no posts — just page info/about
                "maxReviews": 0,
                "maxEvents": 0,
            },
            timeout=30,
        )
    except httpx.TimeoutException:
        raise ApifyTimeoutError("Apify run start timed out")

    if run_resp.status_code == 401:
        raise CredentialInvalidError(
            "APIFY_API_KEY is invalid or expired. Update in .env"
        )
    if run_resp.status_code == 402:
        raise CreditsLowError(
            "Apify credits exhausted. Top up at https://console.apify.com/billing"
        )
    if not run_resp.ok:
        raise ServiceDownError(f"Apify run start returned {run_resp.status_code}")

    run_id = run_resp.json()["data"]["id"]
    dataset_id = run_resp.json()["data"]["defaultDatasetId"]

    # Poll for completion
    waited = 0
    while waited < APIFY_MAX_WAIT_SEC:
        time.sleep(APIFY_POLL_INTERVAL_SEC)
        waited += APIFY_POLL_INTERVAL_SEC

        status_resp = httpx.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            headers=headers,
            timeout=10,
        )
        status = status_resp.json().get("data", {}).get("status", "")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break

    if status != "SUCCEEDED":
        raise ApifyTimeoutError(f"Apify actor finished with status: {status}")

    # Fetch dataset items
    items_resp = httpx.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        headers=headers,
        params={"limit": 1},
        timeout=15,
    )
    items = items_resp.json()
    return items[0] if items else None


def _extract_email(data: dict) -> Optional[str]:
    """Extract an email from the Facebook page About data."""
    # Apify Facebook scraper returns various fields
    candidates = [
        data.get("email"),
        data.get("about"),
        data.get("description"),
        str(data.get("contact", "")),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        matches = EMAIL_REGEX.findall(str(candidate))
        for email in matches:
            # Reject Facebook's own addresses
            if "facebook.com" not in email and "fb.com" not in email:
                return email.lower()
    return None
