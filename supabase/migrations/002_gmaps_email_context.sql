-- ---------------------------------------------------------------------------
-- Migration 002: Google Maps context fields for email personalisation
--
-- Adds gmaps_place_id / rating / reviews to leads so the email agent can
-- optionally reference a business's Google Maps presence in outreach.
-- ---------------------------------------------------------------------------

ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS gmaps_place_id TEXT,
  ADD COLUMN IF NOT EXISTS gmaps_rating   NUMERIC(2,1),
  ADD COLUMN IF NOT EXISTS gmaps_reviews  INT;

COMMENT ON COLUMN leads.gmaps_place_id IS 'Google Maps place ID from Apify scrape (e.g. ChIJ...)';
COMMENT ON COLUMN leads.gmaps_rating   IS 'Star rating at time of scrape (1.0–5.0)';
COMMENT ON COLUMN leads.gmaps_reviews  IS 'Review count at time of scrape';
