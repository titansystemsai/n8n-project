"""
Step 3 — LinkedIn Profile Verification.

Goal: Confirm name, title, and tenure from the actual public LinkedIn profile.
Fetches the real profile page — not just a snippet.

Skipped if no linkedin_url was found in steps 1 or 2.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import anthropic

from ._base import StepResult
from agents.config import EnrichmentConfig

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are verifying a LinkedIn profile for a specific person.

Fetch the LinkedIn URL provided and read the public profile page. Extract:
1. Full name (confirm or correct)
2. Current job title at the business in question
3. Approximate tenure (how long they've been at this business)
4. Whether they appear to be the owner/founder/director (not just an employee)

Return ONLY valid JSON:
{
  "name": "Confirmed Full Name or null",
  "title": "Current title or null",
  "tenure_years": 3.5 or null,
  "is_decision_maker": true or false,
  "confidence": "high" or "medium" or "low",
  "notes": "brief note on what you found"
}

If the profile is private, returns a 404, or you cannot access it, return:
{"error": "PROFILE_UNAVAILABLE", "notes": "reason"}"""


def run_linkedin_verify(
    lead: dict,
    anthropic_client: anthropic.Anthropic,
    config: EnrichmentConfig,
    linkedin_url: Optional[str] = None,
    candidate_name: Optional[str] = None,
) -> StepResult:
    """
    Fetch the LinkedIn public profile and verify name/title/tenure.
    Skipped if no linkedin_url is available from prior steps.
    """
    start = time.monotonic()

    if not linkedin_url:
        return StepResult(
            step="linkedin", success=False,
            error_code="NO_URL",
            error_message="No LinkedIn URL from prior steps — step skipped",
            duration_sec=time.monotonic() - start,
        )

    business = lead.get("business_name", "this business")
    name_context = f"We believe this person is named {candidate_name}." if candidate_name else ""

    user_message = (
        f"Verify this LinkedIn profile for a decision maker at '{business}'.\n"
        f"{name_context}\n"
        f"LinkedIn URL: {linkedin_url}\n\n"
        f"Fetch the profile page and confirm their name, title, and how long they've worked there."
    )

    try:
        response = anthropic_client.messages.create(
            model=config.icp_model,  # cheaper model — classification task
            max_tokens=512,
            system=SYSTEM_PROMPT,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": user_message}],
            metadata={"user_id": f"enrichment-linkedin-{lead.get('id', 'unknown')}"},
        )

        result_text = _extract_text(response)
        if not result_text:
            return StepResult(
                step="linkedin", success=False,
                error_code="NO_RESULT",
                error_message="No response from Claude",
                duration_sec=time.monotonic() - start,
            )

        data = _parse_json_response(result_text)
        if not data:
            return StepResult(
                step="linkedin", success=False,
                error_code="PARSE_ERROR",
                error_message=result_text[:200],
                duration_sec=time.monotonic() - start,
            )

        if "error" in data:
            return StepResult(
                step="linkedin", success=False,
                error_code=data["error"],
                error_message=data.get("notes", "Profile unavailable"),
                duration_sec=time.monotonic() - start,
            )

        input_tokens = response.usage.input_tokens if response.usage else 0
        output_tokens = response.usage.output_tokens if response.usage else 0
        # Haiku pricing: ~$0.25/MTok input, $1.25/MTok output
        cost = (input_tokens * 0.25 + output_tokens * 1.25) / 1_000_000

        return StepResult(
            step="linkedin",
            success=True,
            name=data.get("name"),
            title=data.get("title"),
            linkedin_url=linkedin_url,
            notes=data.get("notes"),
            raw=data,
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    except anthropic.AuthenticationError:
        return StepResult(
            step="linkedin", success=False,
            error_code="CREDENTIAL_INVALID",
            error_message="ANTHROPIC_API_KEY is invalid or expired. Update in .env",
            duration_sec=time.monotonic() - start,
        )
    except anthropic.RateLimitError:
        raise
    except Exception as e:
        return StepResult(
            step="linkedin", success=False,
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
