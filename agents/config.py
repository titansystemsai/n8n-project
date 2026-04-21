"""
Campaign configuration — loaded from Supabase campaigns.config JSONB at runtime.
No YAML files. Each campaign carries its own parameters.

Usage:
    cfg = load_campaign_config(supabase_client, campaign_id)
    print(cfg.enrichment.research_depth)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional

from supabase import Client


# ---------------------------------------------------------------------------
# Config dataclasses — these define the defaults for every campaign
# ---------------------------------------------------------------------------

@dataclass
class LeadFilterConfig:
    """Controls which leads are picked up by the enrichment agent."""
    has_website: Optional[bool] = None     # None=all, True=website-only, False=no-website only
    batch_size: int = 20
    sources: List[str] = field(default_factory=lambda: ["google_maps"])
    min_icp_score_for_email: int = 65


@dataclass
class ICPConfig:
    """Ideal Customer Profile criteria — used by the ICP scoring step."""
    target_business_types: List[str] = field(default_factory=lambda: ["independent small business"])
    target_locations: List[str] = field(default_factory=lambda: ["Adelaide, SA"])
    exclude_franchises: bool = True
    exclude_large_chains: bool = True
    min_google_rating: float = 3.5
    target_titles: List[str] = field(default_factory=lambda: [
        "owner", "director", "founder", "principal", "partner", "manager"
    ])
    negative_keywords: List[str] = field(default_factory=list)


@dataclass
class EnrichmentConfig:
    """Controls how enrichment_agent.py runs per lead."""
    research_depth: str = "standard"           # quick (2 searches) | standard (4) | deep (8)
    email_sources: List[str] = field(default_factory=lambda: [
        "hunter_io", "website_scrape", "facebook", "domain_guess"
    ])
    stop_after_first_email: bool = True         # skip remaining sources once an email is found
    max_cost_per_lead_usd: float = 0.05
    max_concurrent_workers: int = 3             # simultaneous Anthropic sessions
    anthropic_model: str = "claude-sonnet-4-6"
    icp_model: str = "claude-haiku-4-5-20251001"  # cheaper model for classification tasks
    icp: ICPConfig = field(default_factory=ICPConfig)


@dataclass
class OutreachConfig:
    """Controls how email_agent.py writes drafts."""
    tone: str = "direct and personal"
    length: str = "short"                       # short | medium
    call_to_action: str = "15-minute call"
    sender_name: str = "Ajaay"
    sender_company: str = "Titan Systems"
    sender_context: str = "B2B lead generation"
    avoid_phrases: List[str] = field(default_factory=lambda: [
        "I hope this email finds you well",
        "touching base",
        "synergy",
        "circle back",
        "reaching out",
    ])
    anthropic_model: str = "claude-sonnet-4-6"
    instantly_campaign_id: Optional[str] = None   # set when exporting to Instantly
    include_gmaps_context: bool = True    # reference rating/reviews when available
    include_linkedin_context: bool = True # reference DM's LinkedIn profile when available


@dataclass
class CampaignConfig:
    """Full configuration for one campaign run. Loaded from Supabase."""
    campaign_id: str
    campaign_name: str
    org_id: str
    lead_filter: LeadFilterConfig = field(default_factory=LeadFilterConfig)
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)
    outreach: OutreachConfig = field(default_factory=OutreachConfig)


# ---------------------------------------------------------------------------
# Loader — merges Supabase JSONB overrides onto dataclass defaults
# ---------------------------------------------------------------------------

def _apply_overrides(obj: object, overrides: dict) -> None:
    """Recursively apply a flat dict of overrides to a dataclass instance."""
    for key, value in overrides.items():
        if hasattr(obj, key):
            current = getattr(obj, key)
            if isinstance(current, (LeadFilterConfig, EnrichmentConfig,
                                    OutreachConfig, ICPConfig)) and isinstance(value, dict):
                _apply_overrides(current, value)
            else:
                setattr(obj, key, value)


def load_campaign_config(supabase: Client, campaign_id: str) -> CampaignConfig:
    """
    Fetch campaign row from Supabase and merge its config JSONB onto defaults.

    Raises:
        ValueError: if campaign_id not found.
    """
    result = (
        supabase.table("campaigns")
        .select("id, name, org_id, config")
        .eq("id", campaign_id)
        .single()
        .execute()
    )
    if not result.data:
        raise ValueError(f"Campaign not found: {campaign_id}")

    row = result.data
    cfg = CampaignConfig(
        campaign_id=row["id"],
        campaign_name=row["name"],
        org_id=row["org_id"],
    )

    overrides = row.get("config") or {}
    if "lead_filter" in overrides:
        _apply_overrides(cfg.lead_filter, overrides["lead_filter"])
    if "enrichment" in overrides:
        icp_overrides = overrides["enrichment"].pop("icp", {})
        _apply_overrides(cfg.enrichment, overrides["enrichment"])
        if icp_overrides:
            _apply_overrides(cfg.enrichment.icp, icp_overrides)
    if "outreach" in overrides:
        _apply_overrides(cfg.outreach, overrides["outreach"])

    return cfg
