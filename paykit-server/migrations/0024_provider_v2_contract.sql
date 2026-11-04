-- Additive BTC-only signed provider-v2 contract.
-- Existing invoices deliberately remain v1-only: no deadline or threshold is guessed.

CREATE TABLE invoice_provider_v2 (
    invoice_id UUID PRIMARY KEY REFERENCES invoices (id) ON DELETE RESTRICT,
    request_hash BYTEA NOT NULL CHECK (octet_length(request_hash) = 32),
    deadline TIMESTAMPTZ NOT NULL,
    minimum_confirmations SMALLINT NOT NULL CHECK (minimum_confirmations BETWEEN 0 AND 6),
    asset TEXT NOT NULL DEFAULT 'BTC' CHECK (asset = 'BTC'),
    proposal_expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT invoice_provider_v2_deadline_matches_invoice CHECK (deadline > '-infinity'::timestamptz),
    CONSTRAINT invoice_provider_v2_proposal_before_deadline CHECK (proposal_expires_at < deadline)
);

CREATE UNIQUE INDEX invoice_provider_v2_request_hash_unique
    ON invoice_provider_v2 (request_hash);

CREATE TABLE invoice_provider_v2_current_sample (
    invoice_id UUID PRIMARY KEY REFERENCES invoice_provider_v2 (invoice_id) ON DELETE RESTRICT,
    observation_id UUID REFERENCES bitcoin_observations (id) ON DELETE RESTRICT,
    observed_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('undetected', 'detected', 'confirmed')),
    confirmations SMALLINT NOT NULL CHECK (confirmations BETWEEN 0 AND 6),
    amount_matched BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT invoice_provider_v2_current_sample_shape CHECK (
        (status = 'undetected' AND observation_id IS NULL AND confirmations = 0 AND NOT amount_matched)
        OR (status = 'detected' AND observation_id IS NOT NULL AND confirmations = 0)
        OR (status = 'confirmed' AND observation_id IS NOT NULL AND confirmations > 0)
    )
);

CREATE TABLE invoice_provider_v2_first_threshold_evidence (
    invoice_id UUID PRIMARY KEY REFERENCES invoice_provider_v2 (invoice_id) ON DELETE CASCADE,
    observation_id UUID NOT NULL REFERENCES bitcoin_observations (id) ON DELETE RESTRICT,
    observed_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('detected', 'confirmed')),
    confirmations SMALLINT NOT NULL CHECK (confirmations BETWEEN 0 AND 6),
    amount_matched BOOLEAN NOT NULL CHECK (amount_matched),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT invoice_provider_v2_first_evidence_status CHECK (
        (status = 'detected' AND confirmations = 0)
        OR (status = 'confirmed' AND confirmations > 0)
    )
);

CREATE FUNCTION reject_provider_v2_first_evidence_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'provider-v2 first threshold evidence is immutable';
END
$$;

CREATE TRIGGER invoice_provider_v2_first_evidence_no_update
BEFORE UPDATE ON invoice_provider_v2_first_threshold_evidence
FOR EACH ROW EXECUTE FUNCTION reject_provider_v2_first_evidence_mutation();

CREATE FUNCTION reject_direct_provider_v2_first_evidence_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM invoice_provider_v2 WHERE invoice_id = OLD.invoice_id
    ) THEN
        RAISE EXCEPTION 'provider-v2 first threshold evidence may only follow parent deletion';
    END IF;
    RETURN OLD;
END
$$;

CREATE TRIGGER invoice_provider_v2_first_evidence_no_direct_delete
BEFORE DELETE ON invoice_provider_v2_first_threshold_evidence
FOR EACH ROW EXECUTE FUNCTION reject_direct_provider_v2_first_evidence_delete();

-- V2 carries an exact caller-owned timestamp, so the legacy whole-second
-- equality checks cannot apply to those rows. Deferred triggers retain the v1
-- equations and require v2 binding timestamps to agree byte-for-byte with the
-- parent invoice at transaction commit.
ALTER TABLE invoices
    DROP CONSTRAINT invoice_proposal_expiry_after_creation,
    DROP CONSTRAINT invoice_payment_deadline_matches_window;

CREATE FUNCTION validate_invoice_deadline_contract()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    binding invoice_provider_v2%ROWTYPE;
BEGIN
    SELECT * INTO binding FROM invoice_provider_v2 WHERE invoice_id = NEW.id;
    IF FOUND THEN
        IF NEW.payment_deadline IS DISTINCT FROM binding.deadline
            OR NEW.proposal_expires_at IS DISTINCT FROM binding.proposal_expires_at THEN
            RAISE EXCEPTION 'provider-v2 invoice deadline binding mismatch';
        END IF;
    ELSE
        IF NEW.proposal_expires_at IS DISTINCT FROM
                NEW.invoice_created_at + NEW.proposal_acceptance_seconds * INTERVAL '1 second'
            OR NEW.payment_deadline IS DISTINCT FROM
                NEW.invoice_created_at + NEW.payment_window_seconds * INTERVAL '1 second' THEN
            RAISE EXCEPTION 'v1 invoice deadline contract mismatch';
        END IF;
    END IF;
    RETURN NEW;
END
$$;

CREATE CONSTRAINT TRIGGER invoices_deadline_contract
AFTER INSERT OR UPDATE OF invoice_created_at, proposal_expires_at, payment_deadline,
    proposal_acceptance_seconds, payment_window_seconds ON invoices
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_invoice_deadline_contract();

CREATE FUNCTION validate_provider_v2_parent_deadline_contract()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    invoice invoices%ROWTYPE;
BEGIN
    SELECT * INTO STRICT invoice FROM invoices WHERE id = NEW.invoice_id;
    IF invoice.payment_deadline IS DISTINCT FROM NEW.deadline
        OR invoice.proposal_expires_at IS DISTINCT FROM NEW.proposal_expires_at THEN
        RAISE EXCEPTION 'provider-v2 parent deadline mismatch';
    END IF;
    RETURN NEW;
END
$$;

CREATE CONSTRAINT TRIGGER invoice_provider_v2_parent_deadline_contract
AFTER INSERT OR UPDATE ON invoice_provider_v2
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_v2_parent_deadline_contract();
