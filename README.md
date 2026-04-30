# n8n-project

Lead enrichment pipeline with n8n workflows, Python agents, and Supabase.

## Setup

```bash
cp .env.example .env
# Fill in all values in .env

pip install -r agents/requirements.txt
```

Required environment variables:

| Variable | Purpose |
|---|---|
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (bypasses RLS) |
| `ANTHROPIC_API_KEY` | Claude Sonnet/Haiku for website fetch, ICP scoring, LinkedIn verify |
| `OPENAI_API_KEY` | GPT-4.1-mini for DM discovery, personal email search, company email search |
| `BRAVE_API_KEY` | Brave Search API — all web searches route through here |
| `HUNTER_IO_API_KEY` | Hunter.io domain email lookup |
| `APIFY_API_KEY` | Apify actors for Google Maps, LinkedIn, Facebook, CSV scraping |

---

## Running the Agents

### Ingestion — scrape leads into Supabase

Opens a browser UI (recommended):
```bash
python -m agents.ingestion.agent
```

Force the old terminal form instead:
```bash
python -m agents.ingestion.agent --no-ui
```

Scripted (Google Maps):
```bash
python -m agents.ingestion.agent --campaign <uuid> \
  --source google_maps --keyword "plumbers" --location "Adelaide, SA" \
  --limit 50 --no-dry-run
```

Scripted (LinkedIn company search):
```bash
python -m agents.ingestion.agent --campaign <uuid> \
  --source linkedin_company --query "plumbing companies Adelaide" \
  --limit 50 --no-dry-run
```

Scripted (LinkedIn people search):
```bash
python -m agents.ingestion.agent --campaign <uuid> \
  --source linkedin_profile --query "plumbers Adelaide" \
  --limit 50 --no-dry-run
```

Scripted (Facebook Pages):
```bash
python -m agents.ingestion.agent --campaign <uuid> \
  --source facebook --query "plumbers Adelaide" \
  --limit 50 --no-dry-run
```

Scripted (CSV / Excel):
```bash
python -m agents.ingestion.agent --campaign <uuid> \
  --source csv --file /path/to/leads.xlsx --no-dry-run
```

Omit `--no-dry-run` to preview without writing to Supabase.

---

### Enrichment — find decision-maker emails and score ICP fit

Interactive (recommended):
```bash
python -m agents.enrichment.agent
```

Scripted — all leads in a campaign:
```bash
python -m agents.enrichment.agent --campaign <uuid> --no-dry-run
```

Scripted — limit to N leads:
```bash
python -m agents.enrichment.agent --campaign <uuid> --no-dry-run --limit 10
```

Dry-run (shows estimated cost and lead count, writes nothing):
```bash
python -m agents.enrichment.agent --campaign <uuid> --dry-run
```

Filter to leads with a website only (skips no-website leads):
```bash
python -m agents.enrichment.agent --campaign <uuid> --has-website true --no-dry-run
```

Filter to leads without a website only:
```bash
python -m agents.enrichment.agent --campaign <uuid> --has-website false --no-dry-run
```

Headless (no confirmation prompt — for cron/automation):
```bash
python -m agents.enrichment.agent --campaign <uuid> --headless --no-dry-run
```

---

### Scheduler — run all active campaigns in one shot

```bash
python -m agents.scheduler --run-all-active
```

Options:
```
--org <uuid>          Scope to a single org
--dry-run             Preview without processing leads
--json-log <path>     Append a JSONL run summary (e.g. logs/scheduler.jsonl)
```

Exit codes: `0` = success, `1` = hard error, `2` = partial (some leads dead/exhausted).

---

## Enrichment Pipeline — How It Works

The enrichment agent processes leads in the `queued` state one at a time (concurrency is configurable per campaign). For each lead it runs two phases sequentially.

### Phase 1 — Lead-level research (run once per lead)

#### Step 1 — DM Discovery (`web_search`)

Three GPT-4.1-mini agents run in parallel, each independently searching Brave for the owner, director, or founder of the business. Each agent runs up to 6 Brave searches, decides what to query, and returns a JSON list of decision makers it found.

A consensus pass then compares results across the three agents: if two or more agree on the same person (name match), that person leads the list. If there is no consensus, all unique names found across runs are kept (up to 5).

All Brave requests across the entire process share a single token-bucket rate limiter (1 req/sec, burst of 3) to avoid Brave's rate limit.

#### Step 2 — Website Fetch (`website_fetch`)

Claude Sonnet (with the `web_search` built-in tool) reads the business website — homepage, `/about`, `/about-us`, `/team`, and `/contact` — looking for:
- The owner/founder/director name and title
- A LinkedIn URL linked from the site
- Any email address in the footer, contact page, or header

Skipped automatically if the lead has no `website` field.

#### Step 3 — Company Email Search (`company_email_search`)

Three more GPT-4.1-mini+Brave agents run in parallel searching specifically for a business contact email (e.g. `hello@`, `info@`, `bookings@`). The same consensus and deduplication logic applies. Results are held in a pool to be assigned to DM rows later.

#### Step 4 — Hunter.io Domain Search (`hunter_io`)

A direct API call to Hunter.io's domain search endpoint. Returns every email address Hunter has indexed for the business domain, each with a confidence score (0–100) and job title. The best match is prioritised by whether the title contains owner/director/founder/principal. Quota is tracked per org per month in `api_quota_usage`.

Skipped if the lead has no website, or if the org has exhausted its Hunter quota for the month. If Hunter returns a `429`, the run hard-stops and waits for credentials to be fixed before resuming.

#### Step 5 — Facebook Scrape (`facebook`)

Searches Brave for the business's Facebook page URL, then runs the Apify Facebook Pages Scraper against it. Extracts any email from the About section. Runs for all leads regardless of whether they have a website.

#### Step 6 — ICP Scoring (`icp_score`)

Claude Haiku classifies the lead against the campaign's ICP criteria (defined in `campaigns.config` JSONB, defaulting to `agents/config.py`). Returns an integer score from 0–100. Key criteria: independent small business, Adelaide SA location, minimum Google rating, owner/director title type, no franchises or large chains.

---

### Phase 2 — Per-DM enrichment loop (runs for each decision maker found in Step 1)

Up to 5 DMs are processed per lead.

#### Step 7 — Personal Email Search (`personal_email_search`)

Three GPT-4.1-mini+Brave agents search specifically for the named individual's personal or professional email address (e.g. `firstname@businessdomain.com.au`). The system prompt explicitly forbids returning fabricated addresses.

#### Step 8 — LinkedIn Verify (`linkedin_verify`)

Only runs for the first (primary) DM if a LinkedIn URL was found in Step 2. Claude Haiku uses the `web_search` tool to search for the person by name and business, confirming their title and whether they are the genuine decision maker. Used to correct or fill in title data.

#### Step 9 — Email Selection (`pick_best_email`)

The best available email for this DM is chosen from all sources, in strict priority order:

1. **Personal email** — found by Step 7 specifically for this person (`gpt_brave`, `medium` confidence)
2. **Hunter match by name** — Hunter returned an email whose first + last name match this DM (`hunter_io`, confidence mapped from Hunter score)
3. **Website email** — extracted from the site in Step 2 (`contact_page`, `medium`)
4. **Facebook email** — from the About section in Step 5 (`facebook`, `medium`)
5. **Company email** — from the pool found in Step 3 (`company_email_search`, `medium`)
6. **Remaining Hunter email** — any unmatched Hunter email not yet assigned to another DM (`hunter_io`)

> **Domain guessing is disabled.** The pipeline never constructs `firstname@domain.com` style addresses. If none of the above sources yield an email, the DM row is written with `email = null`.

Each email is claimed from a shared `used_emails` set so the same address is never written to two DM rows for the same lead.

#### Step 10 — Write DM row

Upserts one row into `decision_makers` with `on_conflict = lead_id, email`. Fields written: `name`, `title`, `linkedin_url`, `email`, `email_confidence`, `email_source`, `icp_score`, `research_notes`.

---

### After the DM loop

Any Hunter or company emails not claimed by a named DM row are written as additional `decision_makers` rows (no name, no title) so no found email is discarded.

The lead is then marked `enrichment_status = enriched` and the `enrichment_jobs` audit row is completed with step results, cost, and duration.

---

### Error handling and retries

| Scenario | Behaviour |
|---|---|
| Anthropic / OpenAI rate limit | Exponential backoff, up to 5 retries (1s → 2s → 4s → 8s → 16s). After all retries, the step returns a failed `StepResult`; the lead is still processed by remaining steps. |
| Brave 429 | Per-request backoff (1s → 2s → 4s). After 3 attempts, returns empty results; the GPT agent continues without that search result. |
| Hunter quota exhausted | Hard stop: pauses the run and polls credentials every 30 seconds until resolved. |
| Apify / Hunter credential invalid | Hard stop with console message; waiting for credential fix. |
| Single step failure | Isolated — does not kill the lead. The step is logged as failed in `enrichment_jobs.steps_completed` and remaining steps continue. |
| Lead fails 3 times total | Marked `enrichment_status = dead` by the `increment_lead_retry` Postgres function. |
| Stale in-progress locks (crash recovery) | The `reap_stale_locks` Postgres function runs at the start of every batch. Any lead locked for more than 15 minutes is re-queued automatically. |

---

### Output schema

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
