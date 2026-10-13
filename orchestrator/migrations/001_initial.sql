-- UP
CREATE TABLE winning_tickets (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 payer_address TEXT NOT NULL, recipient TEXT NOT NULL,
 face_value BLOB NOT NULL, win_prob BLOB NOT NULL,
 ticket_nonce INTEGER NOT NULL, recipient_rand BLOB NOT NULL,
 recipient_rand_hash TEXT NOT NULL, sig BLOB NOT NULL UNIQUE,
 creation_round INTEGER NOT NULL, creation_round_block_hash TEXT NOT NULL,
 params_expiration_block TEXT NOT NULL,
 redeemed_at TEXT, tx_hash TEXT
);
CREATE INDEX winning_tickets_pending ON winning_tickets(payer_address, creation_round, seq) WHERE tx_hash IS NULL;
CREATE INDEX winning_tickets_epoch ON winning_tickets(recipient_rand_hash);
CREATE INDEX winning_tickets_liability ON winning_tickets(payer_address,creation_round) WHERE redeemed_at IS NULL;
CREATE TABLE redemption_attempts (
 sig BLOB PRIMARY KEY, attempted_at TEXT NOT NULL, error TEXT,
 phase TEXT NOT NULL, raw_transaction BLOB, redeemer_address TEXT, nonce TEXT
);
CREATE UNIQUE INDEX redemption_nonce ON redemption_attempts(redeemer_address,nonce) WHERE nonce IS NOT NULL;
-- DOWN
DROP INDEX redemption_nonce;
DROP TABLE redemption_attempts;
DROP INDEX winning_tickets_liability;
DROP INDEX winning_tickets_epoch;
DROP INDEX winning_tickets_pending;
DROP TABLE winning_tickets;
