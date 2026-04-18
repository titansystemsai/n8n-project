"""
Step 5 — ICP Scoring.

Uses Claude Haiku (cheaper model — structured classification, not research)
to score how well this lead fits the Ideal Customer Profile.

Score 0–100 based on:
- Independent vs franchise/chain
- Location match
- Business type match
- Active and established (not closed, not just launched)
- Google rating (if available)
- Any negative keywords in business name/category
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Optional

import anthropic

from ._base import StepResult
from agents.config import EnrichmentConfig, ICPConfig

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an ICP (Ideal Customer Profile) scorer for a B2B outreach pipeline.

Score this lead from 0 to 100 based on how well it matches the given ICP criteria.

Scoring guide:
- 80–100: Strong match — independent, right location, active, decision maker reachable
- 60–79: Good match — meets most criteria, minor doubts
- 40–59: Weak match — missing 1–2 key criteria
- 0–39: Poor match — franchise, wrong location, closed, or large chain

Return ONLY valid JSON:
{
  "score": 75,
  "is_independent": true or false,
  "location_match": true or false,
  "is_active": true or false,
  "is_franchise": true or false,
  "reasoning": "2-3 sentence explanation of the score"
}"""


def run_icp_score(
    lead: dict,
    anthropic_client: anthropic.Anthropic,
    config: EnrichmentConfig,
    research_summary: str = "",
) -> StepResult:
    """
    Score this lead's ICP fit. Uses the cheaper Haiku model.
    research_summary is built by the orchestrator from all prior step notes.
    """
    start = time.monotonic()
    icp = config.icp
    business = lead.get("business_name", "Unknown")

    icp_criteria = (
        f"Target business types: {', '.join(icp.target_business_types)}\n"
        f"Target locations: {', '.join(icp.target_locations)}\n"
        f"Exclude franchises: {icp.exclude_franchises}\n"
        f"Exclude large chains: {icp.exclude_large_chains}\n"
        f"Minimum Google rating: {icp.min_google_rating}\n"
        f"Negative keywords (automatic disqualifier): {', '.join(icp.negative_keywords) or 'none'}"
    )

    user_message = (
        f"Score this lead:\n\n"
        f"Business: {business}\n"
        f"Location: {lead.get('location', 'unknown')}\n"
        f"Category: {lead.get('category', 'unknown')}\n"
        f"Has website: {'yes' if lead.get('website') else 'no'}\n\n"
        f"Research findings:\n{research_summary or 'No additional research available.'}\n\n"
        f"ICP criteria:\n{icp_criteria}"
    )

    # Auto-disqualify if negative keyword in business name
    if icp.negative_keywords:
        name_lower = business.lower()
        for kw in icp.negative_keywords:
            if kw.lower() in name_lower:
                return StepResult(
                    step="icp_score",
                    success=True,
                    notes=f"Auto-disqualified: negative keyword '{kw}' in business name",
                    raw={"score": 0, "auto_disqualified": True, "keyword": kw},
                    duration_sec=time.monotonic() - start,
                    cost_usd=0.0,
                )

    try:
        response = anthropic_client.messages.create(
            model=config.icp_model,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
            metadata={"user_id": f"enrichment-icp-{lead.get('id', 'unknown')}"},
        )

        result_text = _extract_text(response)
        if not result_text:
            return StepResult(
                step="icp_score", success=False,
                error_code="NO_RESULT",
                error_message="No ICP response from Claude",
                duration_sec=time.monotonic() - start,
            )

        data = _parse_json(result_text)
        if not data:
            return StepResult(
                step="icp_score", success=False,
                error_code="PARSE_ERROR",
                error_message=result_text[:200],
                duration_sec=time.monotonic() - start,
            )

        score = int(data.get("score", 0))
        score = max(0, min(100, score))  # clamp to [0, 100]

        # Haiku pricing
        input_tokens = response.usage.input_tokens if response.usage else 0
        output_tokens = response.usage.output_tokens if response.usage else 0
        cost = (input_tokens * 0.25 + output_tokens * 1.25) / 1_000_000

        return StepResult(
            step="icp_score",
            success=True,
            notes=data.get("reasoning", ""),
            raw={**data, "score": score},
            duration_sec=time.monotonic() - start,
            cost_usd=cost,
        )

    except anthropic.AuthenticationError:
        return StepResult(
            step="icp_score", success=False,
            error_code="CREDENTIAL_INVALID",
            error_message="ANTHROPIC_API_KEY is invalid or expired. Update in .env",
            duration_sec=time.monotonic() - start,
        )
    except anthropic.RateLimitError:
        raise
    except Exception as e:
        return StepResult(
            step="icp_score", success=False,
            error_code="SERVICE_DOWN",
            error_message=str(e),
            duration_sec=time.monotonic() - start,
        )


def extract_icp_score(result: StepResult) -> int:
    """Extract the numeric score from a completed ICP step result."""
    if result.success and result.raw:
        return int(result.raw.get("score", 0))
    return 0


def _extract_text(response) -> Optional[str]:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return None


def _parse_json(text: str) -> Optional[dict]:
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
