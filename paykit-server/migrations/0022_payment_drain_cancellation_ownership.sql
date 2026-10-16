-- Drain-required cancellations remain deliverable even if acceptance arrives
-- after the frozen snapshot or the operational drain is cleaned up.
ALTER TABLE outbox
    ADD COLUMN cancellation_drain_required BOOLEAN NOT NULL DEFAULT FALSE;

-- Preserve the contract for drains created before this migration. Their
-- operational links may later be deleted by cleanup, so the outbox fact must
-- be durable on the retained cancellation itself.
UPDATE outbox AS cancellation
SET cancellation_drain_required = TRUE
FROM payment_drain_cancellations AS drain_cancellation
WHERE cancellation.id = drain_cancellation.cancellation_outbox_id;

ALTER TABLE outbox
    ADD CONSTRAINT outbox_cancellation_drain_required_shape CHECK (
        NOT cancellation_drain_required
        OR intent_kind = 'payment_request_cancellation'
    );
