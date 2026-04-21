"""
Review functions — approve or flag leads and outreach drafts.

These are the canonical write paths for all approval actions. Used by:
  - CLI: direct calls from a Python REPL or helper script
  - Future CRM API: route handlers will call these functions

Every mutation writes an audit_log row so the approval trail is queryable.
"""
from __future__ import annotations

import logging
from typing import Optional

from supabase import Client

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _audit(
    supabase: Client,
    org_id: str,
    table_name: str,
    record_id: str,
    action: str,
    old_data: Optional[dict],
    new_data: dict,
    changed_by: Optional[str] = None,
) -> None:
    """Write one row to audit_log. Never raises — audit failure must not block the main action."""
    try:
        supabase.table("audit_log").insert({
            "org_id": org_id,
            "table_name": table_name,
            "record_id": record_id,
            "action": action,
            "old_data": old_data,
            "new_data": new_data,
            "changed_by": changed_by,
        }).execute()
    except Exception as e:
        log.warning("audit_log write failed (non-fatal): %s", e)


def _get_lead(supabase: Client, lead_id: str) -> Optional[dict]:
    result = (
        supabase.table("leads")
        .select("id, org_id, enrichment_status, campaign_id")
        .eq("id", lead_id)
        .single()
        .execute()
    )
    return result.data or None


def _get_outreach(supabase: Client, outreach_id: str) -> Optional[dict]:
    result = (
        supabase.table("outreach")
        .select("id, campaign_id, status, decision_makers!inner(lead_id, leads!inner(org_id))")
        .eq("id", outreach_id)
        .single()
        .execute()
    )
    return result.data or None


# ---------------------------------------------------------------------------
# Lead review actions
# ---------------------------------------------------------------------------

def approve_lead(
    supabase: Client,
    lead_id: str,
    changed_by: Optional[str] = None,
) -> bool:
    """
    Mark a lead's enrichment result as approved for email drafting.

    Sets decision_makers.status = 'approved' for all DMs on this lead.
    Returns True on success.
    """
    lead = _get_lead(supabase, lead_id)
    if not lead:
        log.error("approve_lead: lead %s not found", lead_id)
        return False

    old_status = lead.get("enrichment_status")

    # Approve all decision makers for this lead
    result = (
        supabase.table("decision_makers")
        .update({"status": "approved"})
        .eq("lead_id", lead_id)
        .execute()
    )

    _audit(
        supabase,
        org_id=lead["org_id"],
        table_name="decision_makers",
        record_id=lead_id,
        action="approve_lead",
        old_data={"status": old_status},
        new_data={"status": "approved"},
        changed_by=changed_by,
    )

    approved_count = len(result.data or [])
    log.info("approve_lead: %d DM(s) approved for lead %s", approved_count, lead_id)
    return True


def flag_lead(
    supabase: Client,
    lead_id: str,
    reason: Optional[str] = None,
    changed_by: Optional[str] = None,
) -> bool:
    """
    Flag a lead for manual review (e.g. wrong business, no relevant DM found).

    Sets decision_makers.status = 'flagged' and records the reason in research_notes.
    Returns True on success.
    """
    lead = _get_lead(supabase, lead_id)
    if not lead:
        log.error("flag_lead: lead %s not found", lead_id)
        return False

    update_payload: dict = {"status": "flagged"}
    if reason:
        update_payload["research_notes"] = f"[FLAGGED] {reason}"

    result = (
        supabase.table("decision_makers")
        .update(update_payload)
        .eq("lead_id", lead_id)
        .execute()
    )

    _audit(
        supabase,
        org_id=lead["org_id"],
        table_name="decision_makers",
        record_id=lead_id,
        action="flag_lead",
        old_data=None,
        new_data={"status": "flagged", "reason": reason},
        changed_by=changed_by,
    )

    flagged_count = len(result.data or [])
    log.info("flag_lead: %d DM(s) flagged for lead %s (reason: %s)", flagged_count, lead_id, reason)
    return True


# ---------------------------------------------------------------------------
# Outreach draft review actions
# ---------------------------------------------------------------------------

def approve_outreach(
    supabase: Client,
    outreach_id: str,
    changed_by: Optional[str] = None,
) -> bool:
    """
    Approve an outreach draft for sending.

    Sets outreach.status = 'approved_for_send'. This is the gate before CSV export.
    Returns True on success.
    """
    row = _get_outreach(supabase, outreach_id)
    if not row:
        log.error("approve_outreach: outreach %s not found", outreach_id)
        return False

    org_id = row.get("decision_makers", {}).get("leads", {}).get("org_id")
    old_status = row.get("status")

    supabase.table("outreach").update(
        {"status": "approved_for_send"}
    ).eq("id", outreach_id).execute()

    _audit(
        supabase,
        org_id=org_id,
        table_name="outreach",
        record_id=outreach_id,
        action="approve_outreach",
        old_data={"status": old_status},
        new_data={"status": "approved_for_send"},
        changed_by=changed_by,
    )

    log.info("approve_outreach: outreach %s approved for send", outreach_id)
    return True


def reject_outreach(
    supabase: Client,
    outreach_id: str,
    reason: Optional[str] = None,
    changed_by: Optional[str] = None,
) -> bool:
    """
    Reject an outreach draft (e.g. tone wrong, wrong person).

    Sets outreach.status = 'rejected'. The email agent can re-draft if needed.
    Returns True on success.
    """
    row = _get_outreach(supabase, outreach_id)
    if not row:
        log.error("reject_outreach: outreach %s not found", outreach_id)
        return False

    org_id = row.get("decision_makers", {}).get("leads", {}).get("org_id")
    old_status = row.get("status")

    supabase.table("outreach").update(
        {"status": "rejected"}
    ).eq("id", outreach_id).execute()

    _audit(
        supabase,
        org_id=org_id,
        table_name="outreach",
        record_id=outreach_id,
        action="reject_outreach",
        old_data={"status": old_status},
        new_data={"status": "rejected", "reason": reason},
        changed_by=changed_by,
    )

    log.info("reject_outreach: outreach %s rejected (reason: %s)", outreach_id, reason)
    return True
