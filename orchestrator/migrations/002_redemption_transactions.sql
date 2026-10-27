-- UP
DROP INDEX redemption_nonce;
DROP INDEX winning_tickets_pending;
CREATE TABLE redemption_transactions (
 hash TEXT PRIMARY KEY, sig BLOB NOT NULL,
 root_hash TEXT NOT NULL, replaces_hash TEXT,
 raw_transaction BLOB NOT NULL, redeemer_address TEXT NOT NULL, nonce TEXT NOT NULL,
 phase TEXT NOT NULL, error TEXT, prepared_at TEXT NOT NULL, broadcast_at TEXT,
 acknowledged INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX redemption_transactions_ticket ON redemption_transactions(sig);
CREATE INDEX redemption_transactions_root ON redemption_transactions(root_hash);
CREATE INDEX redemption_transactions_nonce ON redemption_transactions(redeemer_address,nonce);
CREATE INDEX redemption_transactions_acknowledged ON redemption_transactions(redeemer_address,length(nonce) DESC,nonce DESC) WHERE acknowledged=1;
INSERT INTO redemption_transactions(hash,sig,root_hash,raw_transaction,redeemer_address,nonce,phase,error,prepared_at,broadcast_at,acknowledged)
 SELECT w.tx_hash,a.sig,w.tx_hash,a.raw_transaction,a.redeemer_address,a.nonce,a.phase,a.error,a.attempted_at,
 CASE WHEN a.phase!='prepared' THEN a.attempted_at END,
 CASE WHEN a.phase IN ('submitted','confirmed','reverted') THEN 1 ELSE 0 END
 FROM redemption_attempts a JOIN winning_tickets w ON w.sig=a.sig
 WHERE w.tx_hash IS NOT NULL AND a.raw_transaction IS NOT NULL AND a.nonce IS NOT NULL;
-- DOWN
-- Refuse to discard identities that the old one-row-per-ticket schema cannot hold.
CREATE TEMP TABLE cannot_downgrade_redemption_transaction_history (preserved INTEGER);
CREATE TEMP TRIGGER protect_redemption_history BEFORE INSERT ON cannot_downgrade_redemption_transaction_history WHEN NEW.preserved=0
 BEGIN SELECT RAISE(ABORT,'cannot_downgrade_redemption_transaction_history: retain the upgraded database and use a compatible binary'); END;
INSERT INTO cannot_downgrade_redemption_transaction_history
 SELECT CASE WHEN EXISTS(SELECT 1 FROM redemption_transactions GROUP BY sig HAVING count(*)>1)
 OR EXISTS(SELECT 1 FROM redemption_attempts WHERE nonce IS NOT NULL GROUP BY redeemer_address,nonce HAVING count(*)>1)
 OR EXISTS(SELECT 1 FROM redemption_attempts WHERE phase NOT IN ('prepared','broadcast','submitted','confirmed','reverted','expired'))
 -- The old schema must reproduce the identity exactly on a later upgrade.
 OR EXISTS(SELECT 1 FROM redemption_transactions t LEFT JOIN redemption_attempts a ON a.sig=t.sig LEFT JOIN winning_tickets w ON w.sig=t.sig
 WHERE a.sig IS NULL OR t.hash IS NOT w.tx_hash OR t.root_hash!=t.hash OR t.replaces_hash IS NOT NULL
 OR t.raw_transaction IS NOT a.raw_transaction OR t.redeemer_address IS NOT a.redeemer_address OR t.nonce IS NOT a.nonce
 OR t.phase IS NOT a.phase OR t.error IS NOT a.error OR t.prepared_at IS NOT a.attempted_at
 OR t.broadcast_at IS NOT (CASE WHEN a.phase!='prepared' THEN a.attempted_at END)
 OR t.acknowledged!=(CASE WHEN a.phase IN ('submitted','confirmed','reverted') THEN 1 ELSE 0 END))
 THEN 0 ELSE 1 END;
DROP TABLE cannot_downgrade_redemption_transaction_history;
DROP TABLE redemption_transactions;
CREATE UNIQUE INDEX redemption_nonce ON redemption_attempts(redeemer_address,nonce) WHERE nonce IS NOT NULL;
CREATE INDEX winning_tickets_pending ON winning_tickets(payer_address,creation_round,seq) WHERE tx_hash IS NULL;
