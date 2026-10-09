-- Marketplace status attribution and immutable Bitcoin evidence.
ALTER TABLE marketplace_payment_preparations
    ADD COLUMN bitcoin_address_lookup_hash BYTEA
        CHECK (bitcoin_address_lookup_hash IS NULL OR octet_length(bitcoin_address_lookup_hash) = 32);

CREATE UNIQUE INDEX marketplace_payment_preparations_bitcoin_address_key
    ON marketplace_payment_preparations (bitcoin_address_lookup_hash)
    WHERE bitcoin_address_lookup_hash IS NOT NULL;

ALTER TABLE payment_request_lifecycles
    ALTER COLUMN invoice_id DROP NOT NULL,
    ADD COLUMN marketplace_preparation_id UUID
        REFERENCES marketplace_payment_preparations(id) ON DELETE RESTRICT,
    ADD CONSTRAINT payment_request_lifecycle_one_owner CHECK (
        (invoice_id IS NOT NULL) <> (marketplace_preparation_id IS NOT NULL)
    ),
    ADD CONSTRAINT payment_request_lifecycle_marketplace_identity
        UNIQUE (marketplace_preparation_id, sdk_payment_request_id);

CREATE UNIQUE INDEX payment_request_lifecycles_one_marketplace_preparation
    ON payment_request_lifecycles (marketplace_preparation_id)
    WHERE marketplace_preparation_id IS NOT NULL;

ALTER TABLE bitcoin_observations
    ALTER COLUMN invoice_id DROP NOT NULL,
    ADD COLUMN marketplace_preparation_id UUID
        REFERENCES marketplace_payment_preparations(id) ON DELETE RESTRICT,
    ADD COLUMN first_observed_at TIMESTAMPTZ;

UPDATE bitcoin_observations
SET first_observed_at = created_at
WHERE first_observed_at IS NULL;

ALTER TABLE bitcoin_observations
    ALTER COLUMN first_observed_at SET NOT NULL,
    ADD CONSTRAINT bitcoin_observation_one_owner CHECK (
        (invoice_id IS NOT NULL) <> (marketplace_preparation_id IS NOT NULL)
    );

CREATE UNIQUE INDEX bitcoin_observations_one_active_marketplace_invoice
    ON bitcoin_observations (marketplace_preparation_id)
    WHERE active AND marketplace_preparation_id IS NOT NULL;

CREATE FUNCTION reject_bitcoin_observation_evidence_rewrite() RETURNS trigger AS $$
BEGIN
    IF NEW.invoice_id IS DISTINCT FROM OLD.invoice_id
       OR NEW.marketplace_preparation_id IS DISTINCT FROM OLD.marketplace_preparation_id
       OR NEW.observation_envelope IS DISTINCT FROM OLD.observation_envelope
       OR NEW.outpoint_lookup_hash IS DISTINCT FROM OLD.outpoint_lookup_hash
       OR NEW.first_observed_at IS DISTINCT FROM OLD.first_observed_at THEN
        RAISE EXCEPTION 'bitcoin observation evidence is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bitcoin_observation_evidence_immutable
    BEFORE UPDATE OF invoice_id, marketplace_preparation_id, observation_envelope,
        outpoint_lookup_hash, first_observed_at
    ON bitcoin_observations
    FOR EACH ROW
    EXECUTE FUNCTION reject_bitcoin_observation_evidence_rewrite();

CREATE FUNCTION reject_bitcoin_observation_evidence_delete() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'bitcoin observation evidence cannot be deleted'
        USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bitcoin_observation_evidence_delete_protected
    BEFORE DELETE ON bitcoin_observations
    FOR EACH ROW
    EXECUTE FUNCTION reject_bitcoin_observation_evidence_delete();
