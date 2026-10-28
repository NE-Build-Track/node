-- UP
ALTER TABLE winning_tickets ADD COLUMN enqueued_at TEXT;
UPDATE winning_tickets SET enqueued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now');
-- DOWN
ALTER TABLE winning_tickets DROP COLUMN enqueued_at;
