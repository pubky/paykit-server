-- Creator Bitcoin account generations fence stale address issuance and retain
-- durable, creator-wide rotation/cancellation progress.
ALTER TABLE creators
    ADD COLUMN bitcoin_account_generation BIGINT NOT NULL DEFAULT 0
        CHECK (bitcoin_account_generation >= 0);

ALTER TABLE invoices
    ADD COLUMN bitcoin_account_generation BIGINT
        CHECK (bitcoin_account_generation >= 0);

UPDATE invoices
SET bitcoin_account_generation = 0
WHERE bitcoin_address_lookup_hash IS NOT NULL;

-- Encrypted legacy preparations stay NULL here: runtime target discovery
-- authenticates each envelope, backfills only Bitcoin-bearing rows under the
-- Creator fence, commits, then receives remote lifecycle state before cutover.
ALTER TABLE marketplace_payment_preparations
    ADD COLUMN bitcoin_account_generation BIGINT
        CHECK (bitcoin_account_generation >= 0);

-- Rotation can retain one accepted attempt while durably canceling one or more
-- proposed siblings for the same Marketplace owner. Preserve one proposal per
-- preparation while allowing one cancellation per target request.
ALTER TABLE outbox
    DROP CONSTRAINT outbox_marketplace_preparation_id_key;

CREATE UNIQUE INDEX outbox_marketplace_proposal_unique
    ON outbox (marketplace_preparation_id)
    WHERE marketplace_preparation_id IS NOT NULL
      AND intent_kind = 'payment_request_proposal';

CREATE UNIQUE INDEX outbox_marketplace_cancellation_unique
    ON outbox (
        marketplace_preparation_id,
        cancellation_target_payment_request_id,
        intent_kind
    )
    WHERE marketplace_preparation_id IS NOT NULL
      AND intent_kind = 'payment_request_cancellation';

CREATE TABLE creator_bitcoin_account_rotations (
    id UUID PRIMARY KEY,
    creator_id UUID NOT NULL REFERENCES creators (id) ON DELETE RESTRICT,
    from_generation BIGINT NOT NULL CHECK (from_generation >= 0),
    to_generation BIGINT NOT NULL CHECK (to_generation = from_generation + 1),
    cancellation_count BIGINT NOT NULL DEFAULT 0 CHECK (cancellation_count >= 0),
    voided_preparation_count BIGINT NOT NULL DEFAULT 0 CHECK (voided_preparation_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (creator_id, to_generation)
);

CREATE TABLE creator_bitcoin_account_rotation_items (
    rotation_id UUID NOT NULL REFERENCES creator_bitcoin_account_rotations (id) ON DELETE RESTRICT,
    invoice_id UUID REFERENCES invoices (id) ON DELETE RESTRICT,
    marketplace_preparation_id UUID REFERENCES marketplace_payment_preparations (id) ON DELETE RESTRICT,
    sdk_payment_request_id TEXT,
    classification TEXT NOT NULL CHECK (
        classification IN (
            'accepted',
            'terminal',
            'proposal_suppressed',
            'cancellation_pending',
            'preparation_voided'
        )
    ),
    cancellation_outbox_id UUID REFERENCES outbox (id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT creator_bitcoin_rotation_item_one_owner CHECK (
        num_nonnulls(invoice_id, marketplace_preparation_id) = 1
    ),
    CONSTRAINT creator_bitcoin_rotation_item_cancellation_shape CHECK (
        (classification = 'cancellation_pending'
            AND sdk_payment_request_id IS NOT NULL
            AND cancellation_outbox_id IS NOT NULL)
        OR (classification <> 'cancellation_pending'
            AND cancellation_outbox_id IS NULL)
    ),
    UNIQUE NULLS NOT DISTINCT (
        rotation_id, invoice_id, marketplace_preparation_id, sdk_payment_request_id, classification
    )
);

CREATE INDEX creator_bitcoin_rotation_pending_index
    ON creator_bitcoin_account_rotation_items (rotation_id, cancellation_outbox_id)
    WHERE classification = 'cancellation_pending';
