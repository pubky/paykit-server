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

-- Cleanup advances the lock generation and keeps its authenticated receipt,
-- but cascades the operational drain rows. A retained cancellation from an
-- older generation that was never owned by account rotation, or that existed
-- before rotation adopted it, therefore has only one valid producer: the
-- cleaned drain. A rotation-created cancellation present before cleanup is
-- ambiguous after the operational links are gone, so refuse the upgrade.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM outbox AS cancellation
        JOIN invoices AS invoice ON invoice.id = cancellation.invoice_id
        JOIN lock_payment_generations AS generation
          ON generation.creator_id = invoice.creator_id
         AND generation.lock_resource_lookup_hash = invoice.lock_resource_lookup_hash
        JOIN creator_bitcoin_account_rotation_items AS rotation_item
          ON rotation_item.cancellation_outbox_id = cancellation.id
        WHERE cancellation.intent_kind = 'payment_request_cancellation'
          AND generation.current_generation > invoice.lock_resource_generation
          AND generation.last_cleanup_token IS NOT NULL
          AND cancellation.created_at < generation.updated_at
          AND rotation_item.created_at <= cancellation.created_at
          AND NOT EXISTS (
              SELECT 1
              FROM payment_drain_cancellations AS drain_cancellation
              WHERE drain_cancellation.cancellation_outbox_id = cancellation.id
          )
    ) THEN
        RAISE EXCEPTION 'ambiguous cleaned cancellation ownership';
    END IF;
END
$$;

UPDATE outbox AS cancellation
SET cancellation_drain_required = TRUE
FROM invoices AS invoice
JOIN lock_payment_generations AS generation
  ON generation.creator_id = invoice.creator_id
 AND generation.lock_resource_lookup_hash = invoice.lock_resource_lookup_hash
WHERE cancellation.invoice_id = invoice.id
  AND cancellation.intent_kind = 'payment_request_cancellation'
  AND generation.current_generation > invoice.lock_resource_generation
  AND generation.last_cleanup_token IS NOT NULL
  AND (
      NOT EXISTS (
          SELECT 1
          FROM creator_bitcoin_account_rotation_items AS rotation_item
          WHERE rotation_item.cancellation_outbox_id = cancellation.id
      )
      OR EXISTS (
          SELECT 1
          FROM creator_bitcoin_account_rotation_items AS rotation_item
          WHERE rotation_item.cancellation_outbox_id = cancellation.id
            AND rotation_item.created_at > cancellation.created_at
      )
  );

ALTER TABLE outbox
    ADD CONSTRAINT outbox_cancellation_drain_required_shape CHECK (
        NOT cancellation_drain_required
        OR intent_kind = 'payment_request_cancellation'
    );
