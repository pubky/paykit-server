-- Verified USDT receipts may belong to an active Marketplace settlement as well as
-- to a Locks invoice. A receipt still has exactly one owner for its whole life.
ALTER TABLE usdt_observations
    ALTER COLUMN invoice_id DROP NOT NULL,
    ADD COLUMN marketplace_preparation_id UUID
        REFERENCES marketplace_settlements (preparation_id) ON DELETE RESTRICT,
    ADD CONSTRAINT usdt_observation_exactly_one_owner CHECK (
        num_nonnulls(invoice_id, marketplace_preparation_id) = 1
    );

CREATE INDEX usdt_observations_marketplace_settlement_index
    ON usdt_observations (marketplace_preparation_id)
    WHERE marketplace_preparation_id IS NOT NULL;

CREATE FUNCTION reject_usdt_observation_owner_rewrite() RETURNS trigger AS $$
BEGIN
    IF NEW.invoice_id IS DISTINCT FROM OLD.invoice_id
       OR NEW.marketplace_preparation_id IS DISTINCT FROM OLD.marketplace_preparation_id
       OR NEW.transfer_lookup_hash IS DISTINCT FROM OLD.transfer_lookup_hash THEN
        RAISE EXCEPTION 'usdt observation ownership is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER usdt_observation_owner_immutable
    BEFORE UPDATE OF invoice_id, marketplace_preparation_id, transfer_lookup_hash
    ON usdt_observations
    FOR EACH ROW
    EXECUTE FUNCTION reject_usdt_observation_owner_rewrite();
