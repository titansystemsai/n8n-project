-- Add email as a first-class column (source-provided contact email, separate from
-- enriched decision_maker emails) and source_data JSONB for arbitrary CSV columns
-- the user maps via the column mapper UI.

ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS email       TEXT,
  ADD COLUMN IF NOT EXISTS source_data JSONB;

CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email) WHERE email IS NOT NULL;
