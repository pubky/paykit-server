-- The Payment Request attempt a USDT receipt was verified for, sealed to the
-- receipt row and its owner. The attempt's own proposal record time bounds
-- the receipt. NULL for a receipt stored before attempts were recorded: the
-- server attributes it at startup when its owner has exactly one attempt, and
-- an unattributed receipt pays nothing.
ALTER TABLE usdt_observations
    ADD COLUMN request_attempt_envelope BYTEA;
