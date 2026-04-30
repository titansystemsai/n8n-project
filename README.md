# Lead Enrichment Pipeline

A fully automated B2B lead generation pipeline built for Titan Systems. Scrapes raw business listings from Google Maps, LinkedIn, Facebook, or CSV files; enriches each lead with decision-maker names, email addresses, and ICP scores; and stores everything in Supabase for downstream outreach.

**Stack:** Python 3.11+ · Supabase (Postgres + RLS) · Anthropic Claude · OpenAI GPT-4.1-mini · Brave Search · Hunter.io · Apify

**No LangChain.** Every AI call is a direct SDK call with a manually constructed message list. No framework abstractions.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Ingestion Agent                      │
│  Google Maps · LinkedIn · Facebook · CSV/XLSX           │
│  (Apify actors → LeadRecord → Supabase leads table)     │
└─────────────────────────┬───────────────────────────────┘
                          │  enrichment_status = 'queued'
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   Enrichment Agent                      │
│                                                         │
│  LEAD LEVEL (once per lead):                            │
│   1. web_search       3× GPT-4.1-mini + Brave           │
│   2. website_fetch    Claude Sonnet (skipped: no site)  │
│   3. company_email    3× GPT-4.1-mini + Brave           │
│   4. hunter_io        Domain search (skipped: no site)  │
│   5. facebook         Apify scraper                     │
│   6. icp_score        Claude Haiku                      │
│                                                         │
│  PER DM (up to 5 DMs per lead):                         │
│   7. personal_email   3× GPT-4.1-mini + Brave           │
│   8. linkedin_verify  Claude Haiku (primary DM only)    │
│   9. pick_best_email  Priority waterfall                 │
│  10. write DM row     Supabase upsert                   │
│                                                         │
│  11. write remaining unclaimed emails                   │
└─────────────────────────┬───────────────────────────────┘
                          │  decision_makers rows
                          ▼
┌─────────────────────────────────────────────────────────┐
│                  Supabase (Postgres)                    │
│  organisations · campaigns · leads · decision_makers    │
│  outreach · enrichment_jobs · api_quota_usage           │
│  export_batches · audit_log                             │
│  RLS enforced · multi-tenant · GDPR purge support       │
└─────────────────────────────────────────────────────────┘
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r agents/requirements.txt
```

### 2. Configure environment variables

Copy the template and fill in all values:

```bash
cp /dev/null .env   # create a blank .env — do NOT copy .env.example (contains real values)
```

Required variables:

| Variable | Purpose | Used By |
|---|---|---|
| `SUPABASE_URL` | Your Supabase project URL | All agents |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key — bypasses RLS | All agents |
| `ANTHROPIC_API_KEY` | Claude Sonnet (website_fetch, linkedin_verify) + Haiku (icp_score) | Enrichment |
| `OPENAI_API_KEY` | GPT-4.1-mini for DM discovery, personal/company email search | Enrichment |
| `BRAVE_API_KEY` | All web searches — Brave Search API | Enrichment |
| `HUNTER_IO_API_KEY` | Domain email lookup — 100 req/month free tier | Enrichment (website leads only) |
| `APIFY_API_KEY` | Google Maps, LinkedIn, Facebook scraping | Ingestion + Enrichment |
| `LINKEDIN_COOKIE` | LinkedIn `li_at` session cookie — required for LinkedIn sources | Ingestion |

> **HUNTER_IO_API_KEY** is only called when enriching leads that have a website. If you are only processing no-website leads, this key is not checked at startup.

### 3. Apply database migrations

Run the SQL migrations against your Supabase project in order:

```
supabase/migrations/001_initial_schema.sql    — all tables, functions, RLS
supabase/migrations/002_add_email_source_data.sql
```

You can run these via the Supabase SQL editor or the Supabase CLI:

```bash
supabase db push
```

### 4. Create a campaign

In your Supabase dashboard, insert a row into `campaigns`:

```sql
INSERT INTO campaigns (org_id, name, vertical, target_city, status)
VALUES ('<your-org-id>', 'My First Campaign', 'plumbing', 'Adelaide', 'active');
```

---

## Running the Agents

### Ingestion — scrape leads into Supabase

Opens an interactive browser UI (recommended):

```bash
python -m agents.ingestion.agent
```

Force the terminal form instead of the browser UI:

```bash
python -m agents.ingestion.agent --no-ui
```

Scripted (non-interactive) examples:

```bash
# Google Maps
python -m agents.ingestion.agent --campaign <uuid> \
  --source google_maps --keyword "plumbers" --location "Adelaide, SA" \
  --limit 50 --no-dry-run

# LinkedIn company search
python -m agents.ingestion.agent --campaign <uuid> \
  --source linkedin_company --query "plumbing companies Adelaide" \
  --limit 50 --no-dry-run

# LinkedIn people search (maps person's company as lead)
python -m agents.ingestion.agent --campaign <uuid> \
  --source linkedin_profile --query "plumbers Adelaide" \
  --limit 50 --no-dry-run

# Facebook Pages
python -m agents.ingestion.agent --campaign <uuid> \
  --source facebook --query "plumbers Adelaide" \
  --limit 50 --no-dry-run

# CSV / XLSX / XLS file
python -m agents.ingestion.agent --campaign <uuid> \
  --source csv --file /path/to/leads.xlsx --no-dry-run
```

Omit `--no-dry-run` to preview without writing. Add `--json-log <path>` for a JSONL run summary.

After a successful real run, the agent asks if you want to launch the enrichment agent immediately for those leads.

**Deduplication:** Leads are upserted with `on_conflict = (campaign_id, business_name, location)`. Re-running the same scrape is safe.

**CSV column detection:** Headers are auto-mapped by alias. If your file has a column called `company`, `company name`, `merchant`, `client`, etc., it maps to `business_name` automatically. Supported aliases cover 40+ common column names across all six schema fields.

---

### Enrichment — find decision-maker emails and ICP scores

Interactive (recommended):

```bash
python -m agents.enrichment.agent
```

Scripted:

```bash
# All queued leads in a campaign
python -m agents.enrichment.agent --campaign <uuid> --no-dry-run

# Limit to N leads
python -m agents.enrichment.agent --campaign <uuid> --no-dry-run --limit 10

# Dry-run: show estimated cost and lead count, write nothing
python -m agents.enrichment.agent --campaign <uuid> --dry-run

# Only process leads that have a website
python -m agents.enrichment.agent --campaign <uuid> --has-website true --no-dry-run

# Only process leads without a website
python -m agents.enrichment.agent --campaign <uuid> --has-website false --no-dry-run

# Headless — no confirmation prompt, for cron/automation
python -m agents.enrichment.agent --campaign <uuid> --headless --no-dry-run
```

**Context-aware pre-flight:** The startup credential check is scoped to what the run actually needs. When `--has-website false` is passed, Hunter.io is skipped from the check entirely (it is never called for no-website leads, so a bad or missing key should never block the run).

---

### Scheduler — run all active campaigns unattended

```bash
python -m agents.scheduler --run-all-active
```

Options:

```
--org <uuid>          Scope to a single org
--dry-run             Preview without processing leads
--json-log <path>     Append a JSONL run summary (e.g. logs/scheduler.jsonl)
```

Exit codes: `0` = success · `1` = blocking credential failure or hard error · `2` = partial (some leads marked dead after exhausting retries).

The scheduler is designed to be called by a cron job or a Claude Code `CronCreate` routine.

---

## Enrichment Pipeline — Detailed Walkthrough

Leads are processed in `enrichment_status = 'queued'` order (FIFO). Concurrency is controlled by `max_concurrent_workers` in the campaign config (default: 1). Each lead runs the following two phases.

### Phase 1 — Lead-level research

#### Step 1 — DM Discovery (`web_search`)

Three GPT-4.1-mini agents run in parallel, each with Brave Search as a tool. Each agent independently searches for the owner, director, or founder of the business and returns a JSON list of names. The agents issue up to 6 Brave queries each and decide what to search autonomously.

Consensus pass: if 2 or more agents agree on the same person (name match), that person leads the list. Otherwise all unique names found are kept (up to 5). This mirrors the original DMOS n8n workflow's 3-run consensus node exactly.

Cost: ~$0.003–$0.006/lead.

#### Step 2 — Website Fetch (`website_fetch`)

Claude Sonnet reads the business website — homepage, `/about`, `/about-us`, `/team`, `/contact` — and extracts the owner/founder name, job title, any LinkedIn URL, and any email address.

**Skipped automatically** when `has_website is False` (batch filter) or when the individual lead has no `website` field.

#### Step 3 — Company Email Search (`company_email_search`)

Three more GPT-4.1-mini+Brave agents search specifically for the business's public contact email (e.g. `hello@`, `info@`, `bookings@`). All unique emails found across the three runs are pooled and assigned to DM rows later.

Runs for **all** leads — no website required. Searches Google Maps listings, Yellow Pages, True Local, and other directories.

#### Step 4 — Hunter.io Domain Search (`hunter_io`)

Calls Hunter.io's domain search endpoint. Returns every email address Hunter has indexed for the business domain, each with a confidence score (0–100), first/last name, and job title.

**Skipped** when the lead has no website, or when the org's monthly quota is exhausted (100 requests/month on the free tier, tracked in `api_quota_usage`). Quota is claimed atomically via a Postgres function to prevent double-spend under concurrency.

#### Step 5 — Facebook Scrape (`facebook`)

Searches Brave for `<business name> <location> site:facebook.com`, then runs the Apify Facebook Pages Scraper against the first non-group, non-event result. Extracts any email from the About section.

Runs for **all** leads regardless of website filter — small business owners frequently list personal email addresses on Facebook that appear nowhere else online.

#### Step 6 — ICP Scoring (`icp_score`)

Claude Haiku classifies the lead against the campaign's ICP criteria (stored in `campaigns.config` JSONB, documented in `agents/config.py`). Returns an integer score from 0–100.

Default criteria: independent small business · Adelaide SA · minimum 3.5 Google rating · owner/director/founder title · excludes franchises and large chains.

---

### Phase 2 — Per-DM enrichment loop

Runs for each of up to 5 decision makers found in Step 1.

#### Step 7 — Personal Email Search (`personal_email_search`)

Three GPT-4.1-mini+Brave agents search specifically for this named person's email address (e.g. `john.smith@businessdomain.com.au`). The system prompt explicitly forbids returning fabricated addresses.

#### Step 8 — LinkedIn Verify (`linkedin_verify`)

Only runs for the **primary DM** (index 0) if a LinkedIn URL was found in Step 2. Claude Haiku confirms the person's title and whether they are the genuine decision maker. Used to correct or fill in title data.

**Skipped** for no-website batches (Step 2 never ran, so there is no LinkedIn URL to verify).

#### Step 9 — Email Selection

Best available email is chosen from all sources in strict priority order. The same address is never assigned to two DM rows for the same lead (enforced by a `used_emails` set):

| Priority | Source | Confidence | Condition |
|---|---|---|---|
| 1 | Personal email (Step 7) | `medium` | Always considered |
| 2 | Hunter match by name | mapped from Hunter score | Name first+last match |
| 3 | Website email (Step 2) | `medium` | Skipped for no-website batches |
| 4 | Facebook email (Step 5) | `medium` | Always considered |
| 5 | Company email (Step 3) | `medium` | Always considered |
| 6 | Remaining Hunter email | mapped from Hunter score | No name match required |

**Domain guessing is permanently disabled.** The pipeline never constructs `firstname@domain.com` style addresses. If no source yields an email, the DM row is written with `email = null`.

#### Step 10 — Write DM row

Upserts one row into `decision_makers` with `on_conflict = (lead_id, email)`.

---

### After the DM loop

Any Hunter or company emails not claimed by a named DM row are written as additional `decision_makers` rows (no name, no title). No found email is discarded.

The lead is marked `enrichment_status = 'enriched'`. The `enrichment_jobs` audit row is completed with the per-step result map, total cost, and duration.

---

### Error handling and retries

| Scenario | Behaviour |
|---|---|
| Anthropic / OpenAI rate limit | Exponential backoff up to 5 retries (1s → 2s → 4s → 8s → 16s). After all retries, step returns a failed `StepResult`; remaining steps for the lead continue. |
| Brave 429 | Per-request backoff (1s → 2s → 4s). After 3 attempts, returns empty results; the GPT agent continues without that search result. |
| Hunter quota exhausted | Hard stop: pauses the entire run, polls credentials every 30s until resolved, then resumes. |
| Apify / Hunter / Anthropic credential invalid | Hard stop with console message. Run resumes automatically once the credential is fixed in `.env`. |
| Single step failure | Isolated — does not kill the lead. Step is logged in `enrichment_jobs.steps_completed` as an error code; remaining steps continue. |
| Lead fails 3 times total | Marked `enrichment_status = 'dead'` by the `increment_lead_retry` Postgres function. |
| Stale in-progress locks | The `reap_stale_locks` Postgres function runs at the start of every batch. Any lead locked for more than 15 minutes is re-queued automatically (crash recovery). |

---

### Cost estimates

| Step | Model | Cost per lead |
|---|---|---|
| web_search | GPT-4.1-mini × 3 | ~$0.003–0.006 |
| website_fetch | Claude Sonnet | ~$0.005–0.015 |
| company_email_search | GPT-4.1-mini × 3 | ~$0.003–0.006 |
| hunter_io | Hunter.io API | free tier (100/month) |
| facebook | Apify actor | ~$0.01 |
| icp_score | Claude Haiku | ~$0.001 |
| personal_email_search × DMs | GPT-4.1-mini × 3 per DM | ~$0.003–0.006 per DM |
| linkedin_verify | Claude Haiku | ~$0.001 |
| **Total (typical lead, 1 DM)** | | **~$0.03–0.05** |

---

## Database Schema

All tables are multi-tenant (scoped by `org_id`), RLS-enforced, and support GDPR PII deletion.

### Tables

| Table | Purpose |
|---|---|
| `organisations` | Top-level tenants. Plans: `solo`, `team`, `agency`. |
| `org_members` | User membership with roles: `admin`, `operator`, `viewer`. |
| `campaigns` | A named enrichment run with config overrides stored as JSONB. |
| `leads` | Raw business listings from ingestion. Status machine: `queued → in_progress → enriched / failed / dead`. |
| `decision_makers` | One row per email found per lead. Status machine: `pending → approved / flagged / skipped`. |
| `outreach` | Email drafts generated by the email agent. Status machine: `drafted → approved → approved_for_send → exported → sent`. |
| `enrichment_jobs` | Audit trail — one row per agent session per lead. Stores step-by-step result codes, cost, and duration. |
| `api_quota_usage` | Tracks Hunter.io (and other API) usage per org per billing month. |
| `export_batches` | Tracks CSV/Instantly export runs for idempotency. |
| `audit_log` | Immutable log of all status changes on `leads` and `outreach`. |

### Key Postgres functions

| Function | Description |
|---|---|
| `claim_leads_for_enrichment(campaign_id, agent_id, batch_size)` | Atomically locks leads using `SELECT FOR UPDATE SKIP LOCKED`. Prevents double-processing under concurrency. |
| `reap_stale_locks(campaign_id)` | Re-queues leads locked for >15 minutes (crash recovery). Called at the start of every batch. |
| `increment_lead_retry(lead_id, error)` | Increments retry count; re-queues if <3, marks dead if ≥3. |
| `claim_api_quota(org_id, service, limit)` | Atomic Hunter.io quota claim. Returns `TRUE` if granted, `FALSE` if exhausted. |
| `purge_lead_pii(lead_id)` | GDPR right-to-deletion: nulls PII fields on `leads` and `decision_makers`. |

### Lead status machine

```
queued
  │
  ▼ (agent claims via claim_leads_for_enrichment)
in_progress
  │
  ├─── enriched      (all steps completed, at least one email or name found)
  ├─── dead          (retry_count >= 3)
  └─── failed → reap_stale_locks → queued (up to 3 retries)
```

### RLS summary

Python agents always use the **service role key** which bypasses RLS entirely. The anon key is for n8n workflows and browser clients, which see only their org's data.

| Role | Can read | Can write |
|---|---|---|
| `viewer` | Own org's leads, DMs, campaigns | Nothing |
| `operator` | Own org's leads, DMs, campaigns | Leads, DMs, outreach |
| `admin` | Everything in org | Everything, including quota and audit log |

---

## Campaign Configuration

Each campaign stores a `config` JSONB column that overrides the Python defaults in `agents/config.py`. Example:

```json
{
  "lead_filter": {
    "has_website": null,
    "batch_size": 20
  },
  "enrichment": {
    "research_depth": "standard",
    "max_concurrent_workers": 3,
    "max_cost_per_lead_usd": 0.05,
    "icp": {
      "target_business_types": ["independent small business"],
      "target_locations": ["Adelaide, SA"],
      "exclude_franchises": true,
      "exclude_large_chains": true,
      "min_google_rating": 3.5,
      "target_titles": ["owner", "director", "founder", "principal"]
    }
  }
}
```

Any key not present in the JSONB falls back to the Python default.

---

## Output — Decision Maker Rows

Each enriched lead produces one or more rows in `decision_makers`:

| Column | Description |
|---|---|
| `name` | Full name of the decision maker (null if none found) |
| `title` | Job title (owner, director, founder, etc.) |
| `linkedin_url` | LinkedIn profile URL if found via website fetch |
| `email` | Best email found, or null |
| `email_confidence` | `high` / `medium` / `low` |
| `email_source` | `gpt_brave` / `contact_page` / `hunter_io` / `facebook` / `company_email_search` |
| `icp_score` | 0–100 ICP fit score for the lead |
| `research_notes` | Concatenated notes from each step that found data |
| `status` | `pending` (default) — set to `approved` or `flagged` before email export |

---

## Project Structure

```
agents/
  config.py                     — CampaignConfig, EnrichmentConfig, ICPConfig dataclasses
  quota.py                      — Credential preflight, Hunter quota, Apify credit check
  results.py                    — EnrichmentRunResult, IngestionRunResult dataclasses
  scheduler.py                  — Multi-campaign dispatcher for cron / Claude Code routines
  enrichment/
    agent.py                    — Main enrichment orchestrator (asyncio + Semaphore)
    steps/
      _base.py                  — StepResult dataclass
      _brave.py                 — Shared Brave Search HTTP client with rate limiting
      web_search.py             — Step 1: 3× GPT-4.1-mini + Brave DM discovery
      website_fetch.py          — Step 2: Claude Sonnet website scrape
      company_email_search.py   — Step 3: 3× GPT-4.1-mini + Brave company email search
      hunter.py                 — Step 4: Hunter.io domain email lookup
      facebook.py               — Step 5: Apify Facebook Pages scrape
      icp_score.py              — Step 6: Claude Haiku ICP classification
      personal_email_search.py  — Step 7: 3× GPT-4.1-mini + Brave personal email search
      linkedin.py               — Step 8: Claude Haiku LinkedIn verification
      domain_guess.py           — Domain guess (disabled by default)
  ingestion/
    agent.py                    — Ingestion orchestrator: browser UI + CLI
    sources.py                  — Apify actor adapters + CSV/XLSX reader
    ui.py                       — Flask browser UI with live table
supabase/
  migrations/
    001_initial_schema.sql      — All tables, indexes, functions, triggers, RLS
    002_add_email_source_data.sql
```
