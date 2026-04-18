"""
Instantly CSV Exporter — idempotent.

Queries outreach rows where:
  - status = 'approved_for_send'
  - exported_at IS NULL
  - campaign matches

Writes a CSV in Instantly's import format, then marks the rows as exported.
Re-running produces an empty file (idempotent — already-exported rows are skipped).

Usage:
    python -m agents.export.instantly_export --campaign <uuid> [--output <path>]
    python -m agents.export.instantly_export --campaign <uuid> --dry-run
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

# Instantly expects these exact column names
INSTANTLY_COLUMNS = [
    "Email",
    "First Name",
    "Last Name",
    "Company Name",
    "Website",
    "Title",
    "Personalization",
    "Personalization 2",
    "Personalization 3",
    "Campaign Tag",
]


def _split_name(full_name: Optional[str]) -> tuple[str, str]:
    if not full_name:
        return "", ""
    parts = full_name.strip().split()
    first = parts[0] if parts else ""
    last = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first, last


def _validate_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    email = email.strip().lower()
    return email if EMAIL_REGEX.match(email) else None


def _row_to_instantly(record: dict, campaign_name: str) -> Optional[dict]:
    """Map a Supabase record to an Instantly CSV row. Returns None if email is invalid."""
    email = _validate_email(record.get("email"))
    if not email:
        return None  # Never export rows without a valid email

    first, last = _split_name(record.get("name"))

    return {
        "Email":             email,
        "First Name":        first,
        "Last Name":         last,
        "Company Name":      record.get("business_name") or "",
        "Website":           record.get("website") or "",
        "Title":             record.get("title") or "",
        "Personalization":   record.get("personalisation_line_1") or "",
        "Personalization 2": record.get("personalisation_line_2") or "",
        "Personalization 3": record.get("personalisation_line_3") or "",
        "Campaign Tag":      campaign_name,
    }


def fetch_unexported_rows(supabase: Client, campaign_id: str) -> list[dict]:
    """
    Fetch outreach rows approved for send, not yet exported.
    Paginated to handle 500+ leads without memory issues.
    """
    BATCH = 200
    offset = 0
    all_rows: list[dict] = []

    while True:
        resp = (
            supabase.table("outreach")
            .select(
                "id, subject, body, "
                "personalisation_line_1, personalisation_line_2, personalisation_line_3, "
                "decision_makers!inner(name, title, email, linkedin_url, "
                "  leads!inner(business_name, website, campaign_id))"
            )
            .eq("campaign_id", campaign_id)
            .eq("status", "approved_for_send")
            .is_("exported_at", "null")
            .range(offset, offset + BATCH - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        all_rows.extend(batch)
        offset += BATCH
        if len(batch) < BATCH:
            break

    return all_rows


def _flatten(row: dict) -> dict:
    """Flatten nested Supabase join result into a single dict."""
    dm = row.get("decision_makers") or {}
    lead = dm.get("leads") or {}
    return {
        "outreach_id":             row["id"],
        "subject":                 row.get("subject"),
        "body":                    row.get("body"),
        "personalisation_line_1":  row.get("personalisation_line_1"),
        "personalisation_line_2":  row.get("personalisation_line_2"),
        "personalisation_line_3":  row.get("personalisation_line_3"),
        "name":                    dm.get("name"),
        "title":                   dm.get("title"),
        "email":                   dm.get("email"),
        "linkedin_url":            dm.get("linkedin_url"),
        "business_name":           lead.get("business_name"),
        "website":                 lead.get("website"),
    }


def export(
    supabase: Client,
    campaign_id: str,
    campaign_name: str,
    output_path: str,
    dry_run: bool = False,
) -> int:
    """
    Export approved outreach rows to a CSV file in Instantly format.

    Returns:
        Number of rows written (0 if nothing to export).
    """
    raw_rows = fetch_unexported_rows(supabase, campaign_id)

    if not raw_rows:
        print("  No rows to export (all already exported or none approved).")
        return 0

    # Flatten and map
    instantly_rows: list[dict] = []
    skipped_emails: list[str] = []
    outreach_ids: list[str] = []

    for raw in raw_rows:
        flat = _flatten(raw)
        mapped = _row_to_instantly(flat, campaign_name)
        if mapped:
            instantly_rows.append(mapped)
            outreach_ids.append(flat["outreach_id"])
        else:
            skipped_emails.append(flat.get("business_name", "unknown"))

    total = len(instantly_rows)
    print(f"\n  Instantly CSV Export {'— DRY RUN' if dry_run else ''}")
    print(f"  {'─' * 50}")
    print(f"  Campaign:       {campaign_name}")
    print(f"  Rows to export: {total}")
    if skipped_emails:
        print(f"  Skipped (invalid email): {len(skipped_emails)} — {', '.join(skipped_emails[:5])}")
    print(f"  Output file:    {output_path}")
    print()

    if dry_run:
        print("  Dry run — no file written, no rows marked.")
        return 0

    if total == 0:
        print("  Nothing to export.")
        return 0

    answer = input("  Proceed? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("  Aborted.")
        return 0

    # Write CSV
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INSTANTLY_COLUMNS)
        writer.writeheader()
        writer.writerows(instantly_rows)

    # Create export batch record
    batch_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    campaign_result = supabase.table("campaigns").select("org_id").eq("id", campaign_id).single().execute()
    org_id = campaign_result.data["org_id"] if campaign_result.data else None

    supabase.table("export_batches").insert({
        "id": batch_id,
        "campaign_id": campaign_id,
        "org_id": org_id,
        "row_count": total,
        "file_name": os.path.basename(output_path),
        "exported_at": timestamp,
    }).execute()

    # Mark outreach rows as exported (idempotency guard)
    # Batch update in chunks of 100 to avoid URL length limits
    for i in range(0, len(outreach_ids), 100):
        chunk = outreach_ids[i:i + 100]
        supabase.table("outreach").update({
            "exported_at": timestamp,
            "export_batch_id": batch_id,
            "status": "exported",
        }).in_("id", chunk).execute()

    print(f"  ✓ Exported {total} rows to {output_path}")
    print(f"  Batch ID: {batch_id}")
    print(f"  Upload this file to Instantly: Leads → Import Leads → CSV")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Export approved outreach to Instantly CSV")
    parser.add_argument("--campaign", required=True, help="Campaign UUID")
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: exports/<campaign-name>-<date>.csv)")
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )

    # Get campaign name for the output filename and CSV column
    campaign_result = (
        supabase.table("campaigns")
        .select("name")
        .eq("id", args.campaign)
        .single()
        .execute()
    )
    if not campaign_result.data:
        print(f"Campaign not found: {args.campaign}")
        return

    campaign_name = campaign_result.data["name"]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_name = re.sub(r"[^a-zA-Z0-9\-_]", "_", campaign_name)
    output_path = args.output or f"exports/{safe_name}_{date_str}.csv"

    export(supabase, args.campaign, campaign_name, output_path, args.dry_run)


if __name__ == "__main__":
    main()
