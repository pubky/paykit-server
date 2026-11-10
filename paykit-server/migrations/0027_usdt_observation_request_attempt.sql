-- Each Payment Request attempt a USDT receipt was verified for, attested
-- once and never changed, sealed to the receipt row and its owner. The
-- receipt keeps its one owner; it pays when any attested attempt's own
-- proposal record time bounds it, and pays nothing with none. Receipts stored
-- before this are attested at startup when their owner has exactly one
-- attempt, and otherwise as they are verified again.
CREATE TABLE usdt_observation_attempts (
    observation_id UUID NOT NULL REFERENCES usdt_observations (id) ON DELETE CASCADE,
    attempt_lookup_hash BYTEA NOT NULL CHECK (octet_length(attempt_lookup_hash) = 32),
    attempt_envelope BYTEA NOT NULL,
    PRIMARY KEY (observation_id, attempt_lookup_hash)
);

CREATE FUNCTION reject_usdt_observation_attempt_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'usdt observation attempts are append-only';
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER usdt_observation_attempts_append_only
BEFORE UPDATE ON usdt_observation_attempts
FOR EACH ROW EXECUTE FUNCTION reject_usdt_observation_attempt_update();
