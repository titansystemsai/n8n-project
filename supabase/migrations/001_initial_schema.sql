-- =============================================================================
-- Lead Enrichment Pipeline — Initial Schema
-- CRM-ready: multi-tenant, RLS-enforced, audit-logged
-- Run once against a fresh Supabase project.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. CORE TENANCY
-- ---------------------------------------------------------------------------

CREATE TABLE organisations (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name       TEXT NOT NULL,
  plan       TEXT NOT NULL DEFAULT 'solo',  -- solo | team | agency
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE org_members (
  org_id   UUID NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
  user_id  UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  role     TEXT NOT NULL DEFAULT 'viewer',  -- admin | operator | viewer
  PRIMARY KEY (org_id, user_id),
  CONSTRAINT chk_role CHECK (role IN ('admin', 'operator', 'viewer'))
);

-- ---------------------------------------------------------------------------
-- 2. CAMPAIGNS
-- config JSONB stores per-campaign overrides — see agents/config.py for shape
-- ---------------------------------------------------------------------------

CREATE TABLE campaigns (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id       UUID NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
  name         TEXT NOT NULL,           -- "Realtor Adelaide Q2"
  vertical     TEXT,                    -- "real_estate", "plumbing", etc.
  target_city  TEXT,
  status       TEXT NOT NULL DEFAULT 'active',
  config       JSONB NOT NULL DEFAULT '{}',
  created_by   UUID REFERENCES auth.users(id),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_campaign_status CHECK (status IN ('active', 'paused', 'archived'))
);

-- ---------------------------------------------------------------------------
-- 3. LEADS (raw, from Lead Ingestion n8n workflow)
-- ---------------------------------------------------------------------------

CREATE TABLE leads (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id             UUID NOT NULL REFERENCES organisations(id),
  campaign_id        UUID NOT NULL REFERENCES campaigns(id),
  business_name      TEXT NOT NULL,
  source             TEXT NOT NULL DEFAULT 'google_maps', -- google_maps | csv | linkedin
  location           TEXT,
  phone              TEXT,
  website            TEXT,
  category           TEXT,
  apify_raw_json     JSONB,
  -- Status machine: queued → in_progress → enriched | failed | dead
  enrichment_status  TEXT NOT NULL DEFAULT 'queued',
  retry_count        INT  NOT NULL DEFAULT 0,
  locked_at          TIMESTAMPTZ,
  locked_by          TEXT,             -- agent session UUID
  -- lock_expires_at is computed inline as locked_at + INTERVAL '15 minutes'
  -- (generated columns don't support interval arithmetic in Postgres)
  last_error         TEXT,
  enriched_at        TIMESTAMPTZ,
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- PII deletion support
  pii_deleted_at     TIMESTAMPTZ,
  CONSTRAINT chk_enrichment_status CHECK (
    enrichment_status IN ('queued', 'in_progress', 'enriched', 'failed', 'dead', 'skipped')
  ),
  -- Deduplication: same business at same address per campaign
  CONSTRAINT uq_lead_business_campaign UNIQUE (campaign_id, business_name, location)
);

CREATE INDEX idx_leads_campaign_status ON leads(campaign_id, enrichment_status);
CREATE INDEX idx_leads_stale_locks ON leads(locked_at) WHERE enrichment_status = 'in_progress';

-- ---------------------------------------------------------------------------
-- 4. DECISION MAKERS (from Enrichment Agent)
-- ---------------------------------------------------------------------------

CREATE TABLE decision_makers (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id          UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  name             TEXT,
  title            TEXT,
  linkedin_url     TEXT,
  email            TEXT,
  email_confidence TEXT,   -- high | medium | low
  email_source     TEXT,   -- hunter_io | website_direct | facebook | domain_guess
  icp_score        INT,
  research_notes   TEXT,
  enriched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Approval for email building
  status           TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | flagged | skipped
  approved_by      UUID REFERENCES auth.users(id),
  approved_at      TIMESTAMPTZ,
  pii_deleted_at   TIMESTAMPTZ,
  CONSTRAINT chk_email_confidence CHECK (email_confidence IN ('high', 'medium', 'low', NULL)),
  CONSTRAINT chk_dm_status CHECK (status IN ('pending', 'approved', 'flagged', 'skipped')),
  CONSTRAINT chk_icp_score CHECK (icp_score BETWEEN 0 AND 100),
  -- No duplicate email per lead
  CONSTRAINT uq_dm_lead_email UNIQUE (lead_id, email),
  -- Email format guard (permits NULL)
  CONSTRAINT chk_email_format CHECK (
    email IS NULL OR email ~* '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
  )
);

CREATE INDEX idx_dm_lead_id ON decision_makers(lead_id);
CREATE INDEX idx_dm_status_icp ON decision_makers(status, icp_score DESC);

-- ---------------------------------------------------------------------------
-- 5. OUTREACH (from Email Agent)
-- ---------------------------------------------------------------------------

CREATE TABLE outreach (
  id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  decision_maker_id      UUID NOT NULL REFERENCES decision_makers(id) ON DELETE CASCADE,
  campaign_id            UUID NOT NULL REFERENCES campaigns(id),
  subject                TEXT,
  body                   TEXT,
  personalisation_line_1 TEXT,    -- maps to Instantly "Personalization"
  personalisation_line_2 TEXT,    -- maps to Instantly "Personalization 2"
  personalisation_line_3 TEXT,    -- maps to Instantly "Personalization 3"
  -- Status machine: drafted → approved | rejected → approved_for_send → exported → sent
  status                 TEXT NOT NULL DEFAULT 'drafted',
  approved_by            UUID REFERENCES auth.users(id),
  approved_at            TIMESTAMPTZ,
  drafted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sent_at                TIMESTAMPTZ,
  -- Export tracking — idempotency guard
  exported_at            TIMESTAMPTZ,
  export_batch_id        UUID,     -- FK added below after export_batches is created
  instantly_lead_id      TEXT,     -- Instantly's ID after import
  CONSTRAINT chk_outreach_status CHECK (
    status IN ('drafted', 'approved', 'rejected', 'approved_for_send', 'exported', 'sent')
  )
);

CREATE INDEX idx_outreach_campaign_status ON outreach(campaign_id, status);
CREATE INDEX idx_outreach_unexported ON outreach(campaign_id) WHERE exported_at IS NULL AND status = 'approved_for_send';

-- ---------------------------------------------------------------------------
-- 6. ENRICHMENT JOBS (audit trail — one row per agent session per lead)
-- ---------------------------------------------------------------------------

CREATE TABLE enrichment_jobs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id         UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
  campaign_id     UUID NOT NULL REFERENCES campaigns(id),
  org_id          UUID NOT NULL REFERENCES organisations(id),
  status          TEXT NOT NULL DEFAULT 'running',  -- running | done | failed
  failure_reason  TEXT,
  steps_completed JSONB,   -- {"web_search": "ok", "hunter_io": "QUOTA_EXHAUSTED", ...}
  cost_usd        NUMERIC(8,5) DEFAULT 0,
  duration_sec    INT,
  retry_count     INT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  CONSTRAINT chk_job_status CHECK (status IN ('running', 'done', 'failed'))
);

CREATE INDEX idx_jobs_lead_id ON enrichment_jobs(lead_id);
CREATE INDEX idx_jobs_campaign_status ON enrichment_jobs(campaign_id, status);

-- ---------------------------------------------------------------------------
-- 7. API QUOTA USAGE
-- ---------------------------------------------------------------------------

CREATE TABLE api_quota_usage (
  org_id          UUID NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
  service         TEXT NOT NULL,   -- hunter_io | apify | anthropic | brave
  period          DATE NOT NULL,   -- first day of billing month
  requests_used   INT  NOT NULL DEFAULT 0,
  requests_limit  INT,             -- NULL = unlimited / untracked
  cost_usd        NUMERIC(10,4) DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (org_id, service, period)
);

-- ---------------------------------------------------------------------------
-- 8. EXPORT BATCHES
-- ---------------------------------------------------------------------------

CREATE TABLE export_batches (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id  UUID NOT NULL REFERENCES campaigns(id),
  org_id       UUID NOT NULL REFERENCES organisations(id),
  exported_by  UUID REFERENCES auth.users(id),
  row_count    INT,
  file_name    TEXT,
  exported_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Back-fill FK now that export_batches exists
ALTER TABLE outreach
  ADD CONSTRAINT fk_outreach_export_batch
  FOREIGN KEY (export_batch_id) REFERENCES export_batches(id);

-- ---------------------------------------------------------------------------
-- 9. AUDIT LOG
-- ---------------------------------------------------------------------------

CREATE TABLE audit_log (
  id          BIGSERIAL PRIMARY KEY,
  org_id      UUID NOT NULL REFERENCES organisations(id),
  table_name  TEXT NOT NULL,
  record_id   UUID NOT NULL,
  action      TEXT NOT NULL,   -- STATUS_CHANGE | APPROVE | EXPORT | DELETE_PII
  changed_by  UUID REFERENCES auth.users(id),
  old_data    JSONB,
  new_data    JSONB,
  changed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_record ON audit_log(table_name, record_id);
CREATE INDEX idx_audit_org_time ON audit_log(org_id, changed_at DESC);

-- ---------------------------------------------------------------------------
-- 10. POSTGRES FUNCTIONS
-- ---------------------------------------------------------------------------

-- Helper: org IDs for current user (used in RLS policies)
CREATE OR REPLACE FUNCTION get_my_org_ids()
RETURNS UUID[] AS $$
  SELECT ARRAY(SELECT org_id FROM org_members WHERE user_id = auth.uid());
$$ LANGUAGE sql SECURITY DEFINER STABLE;

-- Helper: role of current user in a specific org
CREATE OR REPLACE FUNCTION my_role_in_org(p_org_id UUID)
RETURNS TEXT AS $$
  SELECT role FROM org_members WHERE user_id = auth.uid() AND org_id = p_org_id;
$$ LANGUAGE sql SECURITY DEFINER STABLE;

-- Atomic lead claiming — SELECT FOR UPDATE SKIP LOCKED prevents double-processing
-- Called by Python agents via: supabase.rpc('claim_leads_for_enrichment', {...})
CREATE OR REPLACE FUNCTION claim_leads_for_enrichment(
  p_campaign_id  UUID,
  p_agent_id     TEXT,
  p_batch_size   INT DEFAULT 1
) RETURNS SETOF leads AS $$
BEGIN
  RETURN QUERY
  WITH claimed AS (
    SELECT id FROM leads
    WHERE campaign_id = p_campaign_id
      AND enrichment_status = 'queued'
      AND retry_count < 3
      AND pii_deleted_at IS NULL
    ORDER BY ingested_at ASC
    LIMIT p_batch_size
    FOR UPDATE SKIP LOCKED
  )
  UPDATE leads SET
    enrichment_status = 'in_progress',
    locked_at         = NOW(),
    locked_by         = p_agent_id
  FROM claimed
  WHERE leads.id = claimed.id
  RETURNING leads.*;
END;
$$ LANGUAGE plpgsql;

-- Stale lock reaper — re-queues leads where agent crashed (lock_expires_at < NOW)
-- Run this at the start of each enrichment batch before claiming new leads.
CREATE OR REPLACE FUNCTION reap_stale_locks(p_campaign_id UUID DEFAULT NULL)
RETURNS INT AS $$
DECLARE
  v_requeued INT;
  v_killed   INT;
BEGIN
  -- Re-queue leads that are in_progress but lock has expired and have retries left
  UPDATE leads SET
    enrichment_status = 'queued',
    locked_at         = NULL,
    locked_by         = NULL,
    retry_count       = COALESCE(retry_count, 0) + 1,
    last_error        = 'lock_timeout_requeued'
  WHERE enrichment_status = 'in_progress'
    AND locked_at + INTERVAL '15 minutes' < NOW()
    AND retry_count < 3
    AND (p_campaign_id IS NULL OR campaign_id = p_campaign_id);
  GET DIAGNOSTICS v_requeued = ROW_COUNT;

  -- Mark as dead leads that have exhausted retries
  UPDATE leads SET
    enrichment_status = 'dead',
    locked_at         = NULL,
    locked_by         = NULL
  WHERE enrichment_status = 'in_progress'
    AND locked_at + INTERVAL '15 minutes' < NOW()
    AND retry_count >= 3
    AND (p_campaign_id IS NULL OR campaign_id = p_campaign_id);
  GET DIAGNOSTICS v_killed = ROW_COUNT;

  RAISE NOTICE 'Reaper: % re-queued, % marked dead', v_requeued, v_killed;
  RETURN v_requeued + v_killed;
END;
$$ LANGUAGE plpgsql;

-- Mark a lead as failed: increment retry, re-queue if retries left, mark dead if exhausted
CREATE OR REPLACE FUNCTION increment_lead_retry(p_lead_id UUID, p_error TEXT)
RETURNS VOID AS $$
DECLARE
  v_new_retry INT;
BEGIN
  UPDATE leads SET
    retry_count  = retry_count + 1,
    last_error   = p_error,
    locked_at    = NULL,
    locked_by    = NULL
  WHERE id = p_lead_id
  RETURNING retry_count INTO v_new_retry;

  IF v_new_retry >= 3 THEN
    UPDATE leads SET enrichment_status = 'dead'  WHERE id = p_lead_id;
  ELSE
    UPDATE leads SET enrichment_status = 'queued' WHERE id = p_lead_id;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Atomic API quota claim — returns TRUE if request was claimed, FALSE if exhausted
CREATE OR REPLACE FUNCTION claim_api_quota(
  p_org_id  UUID,
  p_service TEXT,
  p_limit   INT
) RETURNS BOOLEAN AS $$
DECLARE v_used INT;
BEGIN
  -- Ensure row exists for this billing period
  INSERT INTO api_quota_usage(org_id, service, period, requests_used, requests_limit)
  VALUES (p_org_id, p_service, date_trunc('month', NOW())::DATE, 0, p_limit)
  ON CONFLICT (org_id, service, period) DO NOTHING;

  -- Atomically increment if under limit
  UPDATE api_quota_usage
  SET requests_used = requests_used + 1, updated_at = NOW()
  WHERE org_id  = p_org_id
    AND service = p_service
    AND period  = date_trunc('month', NOW())::DATE
    AND requests_used < requests_limit
  RETURNING requests_used INTO v_used;

  RETURN v_used IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- PII purge — call when a lead/DM must be forgotten (GDPR right to deletion)
CREATE OR REPLACE PROCEDURE purge_lead_pii(p_lead_id UUID)
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE leads SET
    phone          = NULL,
    apify_raw_json = NULL,
    pii_deleted_at = NOW()
  WHERE id = p_lead_id;

  UPDATE decision_makers SET
    name           = '[REDACTED]',
    email          = NULL,
    linkedin_url   = NULL,
    research_notes = NULL,
    pii_deleted_at = NOW()
  WHERE lead_id = p_lead_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- 11. AUDIT TRIGGER (fires on status changes to leads and outreach)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION audit_status_change()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF TG_TABLE_NAME = 'leads' THEN
      IF OLD.enrichment_status IS DISTINCT FROM NEW.enrichment_status THEN
        INSERT INTO audit_log(org_id, table_name, record_id, action, old_data, new_data, changed_by)
        VALUES (NEW.org_id, TG_TABLE_NAME, NEW.id, 'STATUS_CHANGE', to_jsonb(OLD), to_jsonb(NEW), auth.uid());
      END IF;
    ELSIF TG_TABLE_NAME = 'outreach' THEN
      IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO audit_log(org_id, table_name, record_id, action, old_data, new_data, changed_by)
        VALUES (NEW.org_id, TG_TABLE_NAME, NEW.id, 'STATUS_CHANGE', to_jsonb(OLD), to_jsonb(NEW), auth.uid());
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER trg_leads_audit
  AFTER UPDATE ON leads FOR EACH ROW EXECUTE FUNCTION audit_status_change();

CREATE TRIGGER trg_outreach_audit
  AFTER UPDATE ON outreach FOR EACH ROW EXECUTE FUNCTION audit_status_change();

-- ---------------------------------------------------------------------------
-- 12. ROW LEVEL SECURITY
-- ---------------------------------------------------------------------------

-- Enable RLS on all tables
ALTER TABLE organisations   ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns       ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_members     ENABLE ROW LEVEL SECURITY;
ALTER TABLE leads           ENABLE ROW LEVEL SECURITY;
ALTER TABLE decision_makers ENABLE ROW LEVEL SECURITY;
ALTER TABLE outreach        ENABLE ROW LEVEL SECURITY;
ALTER TABLE enrichment_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_quota_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE export_batches  ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log       ENABLE ROW LEVEL SECURITY;

-- organisations: members can read their own org
CREATE POLICY org_select ON organisations FOR SELECT
  USING (id = ANY(get_my_org_ids()));

-- org_members: members can see their own membership row
CREATE POLICY members_select ON org_members FOR SELECT
  USING (user_id = auth.uid() OR org_id = ANY(get_my_org_ids()));

-- campaigns: org members read; admin creates/updates
CREATE POLICY campaigns_select ON campaigns FOR SELECT
  USING (org_id = ANY(get_my_org_ids()));
CREATE POLICY campaigns_insert ON campaigns FOR INSERT
  WITH CHECK (my_role_in_org(org_id) = 'admin');
CREATE POLICY campaigns_update ON campaigns FOR UPDATE
  USING (org_id = ANY(get_my_org_ids()))
  WITH CHECK (my_role_in_org(org_id) = 'admin');

-- leads: org members read; admin/operator write
CREATE POLICY leads_select ON leads FOR SELECT
  USING (org_id = ANY(get_my_org_ids()));
CREATE POLICY leads_insert ON leads FOR INSERT
  WITH CHECK (my_role_in_org(org_id) IN ('admin', 'operator'));
CREATE POLICY leads_update ON leads FOR UPDATE
  USING (org_id = ANY(get_my_org_ids()))
  WITH CHECK (my_role_in_org(org_id) IN ('admin', 'operator'));

-- decision_makers: org members read via leads; admin/operator write
CREATE POLICY dm_select ON decision_makers FOR SELECT
  USING (
    lead_id IN (SELECT id FROM leads WHERE org_id = ANY(get_my_org_ids()))
  );
CREATE POLICY dm_insert ON decision_makers FOR INSERT
  WITH CHECK (
    lead_id IN (
      SELECT id FROM leads
      WHERE org_id = ANY(get_my_org_ids())
        AND my_role_in_org(org_id) IN ('admin', 'operator')
    )
  );
CREATE POLICY dm_update ON decision_makers FOR UPDATE
  USING (
    lead_id IN (SELECT id FROM leads WHERE org_id = ANY(get_my_org_ids()))
  );

-- outreach: same pattern
CREATE POLICY outreach_select ON outreach FOR SELECT
  USING (campaign_id IN (SELECT id FROM campaigns WHERE org_id = ANY(get_my_org_ids())));
CREATE POLICY outreach_insert ON outreach FOR INSERT
  WITH CHECK (
    campaign_id IN (
      SELECT id FROM campaigns WHERE org_id = ANY(
        SELECT org_id FROM org_members
        WHERE user_id = auth.uid() AND role IN ('admin', 'operator')
      )
    )
  );
CREATE POLICY outreach_update ON outreach FOR UPDATE
  USING (campaign_id IN (SELECT id FROM campaigns WHERE org_id = ANY(get_my_org_ids())));

-- enrichment_jobs: org members read
CREATE POLICY jobs_select ON enrichment_jobs FOR SELECT
  USING (org_id = ANY(get_my_org_ids()));
CREATE POLICY jobs_insert ON enrichment_jobs FOR INSERT
  WITH CHECK (my_role_in_org(org_id) IN ('admin', 'operator'));

-- api_quota_usage: admin only
CREATE POLICY quota_select ON api_quota_usage FOR SELECT
  USING (org_id = ANY(get_my_org_ids()) AND my_role_in_org(org_id) = 'admin');

-- export_batches: admin/operator
CREATE POLICY export_select ON export_batches FOR SELECT
  USING (org_id = ANY(get_my_org_ids()));
CREATE POLICY export_insert ON export_batches FOR INSERT
  WITH CHECK (my_role_in_org(org_id) IN ('admin', 'operator'));

-- audit_log: admin only
CREATE POLICY audit_select ON audit_log FOR SELECT
  USING (org_id = ANY(get_my_org_ids()) AND my_role_in_org(org_id) = 'admin');

-- NOTE: Python agents use the SERVICE ROLE key which bypasses all RLS.
-- The service role key must NEVER be exposed in the browser or n8n workflow JSON.
