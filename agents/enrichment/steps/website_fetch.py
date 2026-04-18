"""
Step 2 — Website Intelligence via WebFetch.

Goal: Read the actual business website (homepage, /about, /team) to extract:
- Founder/owner name and title directly from page content
- LinkedIn URL if linked from the site
- Any contact email visible in footer or contact page

Skipped automatically when lead.website is null/empty (no-website leads).
Uses: Claude Agent SDK with computer_use or fetch tool.
"""
from __future__ import annotations

import logging
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

import anthropic

from ._base import StepResult
from agents.config import EnrichmentConfig

log = logging.getLogger(__name__)

PAGES_TO_CHECK = ["/", "/about", "/about-us", "/team", "/our-team", "/contact", "/contact-us"]

SYSTEM_PROMPT = """You are a web researcher extracting business contact information.

You will be given a business website URL. Fetch and read the homepage, /about, /about-us,
/team, and /contact pages. Extract:

1. The owner, founder, director, or principal decision-maker's full name and title
2. Their LinkedIn profile URL (if linked from the site)
3. Any email address visible on the site (in the footer, contact page, or header)

Return ONLY valid JSON with these exact keys:
{
  "name": "Full Name or null",
  "title": "Job title or null",
  "linkedin_url": "https://linkedin.com/in/... or null",
  "email": "email@domain.com or null",
  "email_source": "footer" or "contact_page" or "header" or null,
  "pages_checked": ["list of URLs you actually fetched"],
  "notes": "brief note on what you found"
}

If a page 404s or fails to load, skip it and try the next one."""


def run_website_fetch(
    lead: dict,
    anthropic_client: anthropic.Anthropic,
    config: EnrichmentConfig,
    prior_name: Optional[str] = None,
) -> StepResult:
    """
    Fetch business website pages and extract DM info and any visible email.
    Returns early (skipped) if no website URL is available.
    """
    start = time.monotonic()
    website = lead.get("website", "")

    if not website:
        return StepResult(
            step="website_fetch", success=False,
            error_code="NO_DOMAIN",
            error_message="No website URL for this lead — step skipped",
            duration_sec=time.monotonic() - start,
        )

    # Normalise URL
    if not website.startswith(("http://", "https://")):
        website = "https://" + website

    business = lead.get("business_name", "this business")
    name_context = f"The person we're looking for may be named {prior_name}." if prior_name else ""

    user_message = (
        f"Fetch the website for '{business}' at {website}.\n"
        f"{name_context}\n"
        f"Check: {website}, {website}/about, {website}/about-us, {website}/team, {website}/contact\n"
        f"Extract the owner/founder/director name, title, LinkedIn URL, and any visible email."
    )

    try:
        response = anthropic_client.messages.create(
            model=config.anthropic_model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": user_message}],
            metadata={"user_id": f"enrichment-website-{lead.get('id', 'unknown')}"},
        )

        result_text = _extract_text(response)
        if not result_text:
            return StepResult(
                step="website_fetch", success=False,
                error_code="NO_RESULT",
                error_message="Claude returned no text",
                duration_sec=time.monotonic() - start,
            )

        data = _parse_json_response(result_text)
        if not data:
            return StepResult(
                step="website_fetch", success=False,
                error_code="PARSE_ERROR",
                error_message=result_text[:200],
                duration_sec=time.monotonic() - start,
            )

        input_tokens = response.usage.input_tokens if response.usage else 0
        output_tokens = response.usage.output_tokens if response.usage else 0
        cost = (input_tokens * 3 + output_tokens * 15) / 1_000_000

        email = data.get("email")
        if email:
            email = _validate_email(email)

        return StepResult(
            step="website_fetch",
            success=True,
            name=data.get("name"),
            title=data.get("title"),
            linkedin_url=data.get("linkedin_url"),
            email=email,
            email_confidence="medium" if email else None,
            email_source=data.get("email_source") or ("website_direct" if email else None),
            notes=data.get("notes"),
            raw=data,
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    except anthropic.AuthenticationError:
        return StepResult(
            step="website_fetch", success=False,
            error_code="CREDENTIAL_INVALID",
            error_message="ANTHROPIC_API_KEY is invalid or expired. Update in .env",
            duration_sec=time.monotonic() - start,
        )
    except anthropic.RateLimitError:
        raise
    except Exception as e:
        return StepResult(
            step="website_fetch", success=False,
            error_code="SERVICE_DOWN",
            error_message=str(e),
            duration_sec=time.monotonic() - start,
        )


def _extract_text(response) -> Optional[str]:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return None


def _parse_json_response(text: str) -> Optional[dict]:
    import json, re
    text = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return None


def _validate_email(email: str) -> Optional[str]:
    import re
    pattern = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    email = email.strip().lower()
    return email if re.match(pattern, email) else None
