-- Migration 0023 flags pre-upgrade USDT-only payment markers, and a flagged
-- marker no longer counts as payment. A drain retained from before the upgrade
-- may have counted such an invoice as terminal, so its stored summary no
-- longer matches its canonical counts: replay only lets accepted counts fall,
-- and cleanup rejects the summary. Recount each retained drain holding a
-- flagged invoice under the current rule, reopening it while any accepted
-- invoice is unpaid. Reconciliation then only moves flagged invoices from
-- accepted to terminal, which replay already allows.
--
-- This runs after 0024, whose first-threshold evidence the canonical counts
-- read, on fresh and on upgraded databases alike.
WITH affected AS (
    SELECT DISTINCT item.drain_id
    FROM payment_drain_items item
    JOIN invoices invoice ON invoice.id = item.invoice_id
    WHERE invoice.usdt_credit_reconcile
),
recount AS (
    SELECT
        item.drain_id,
        COUNT(*) FILTER (
            WHERE item.classification = 'accepted'
              AND invoice.payment_expired_at IS NULL
              AND NOT (
                  invoice.first_amount_matched_observed_at IS NOT NULL
                  AND invoice.first_amount_matched_observed_at <= invoice.payment_deadline
                  AND NOT invoice.usdt_credit_reconcile
                  OR EXISTS (
                      SELECT 1
                      FROM invoice_provider_v2_first_threshold_evidence evidence
                      WHERE evidence.invoice_id = invoice.id
                  )
              )
        ) AS accepted_count,
        COUNT(*) FILTER (
            WHERE item.classification IN ('rejected', 'canceled', 'proposal_expired')
               OR (
                   item.classification = 'accepted'
                   AND (
                       invoice.payment_expired_at IS NOT NULL
                       OR (
                           invoice.first_amount_matched_observed_at IS NOT NULL
                           AND invoice.first_amount_matched_observed_at <= invoice.payment_deadline
                           AND NOT invoice.usdt_credit_reconcile
                       )
                       OR EXISTS (
                           SELECT 1
                           FROM invoice_provider_v2_first_threshold_evidence evidence
                           WHERE evidence.invoice_id = invoice.id
                       )
                   )
               )
        ) AS terminal_count
    FROM payment_drain_items item
    JOIN invoices invoice ON invoice.id = item.invoice_id
    WHERE item.drain_id IN (SELECT drain_id FROM affected)
    GROUP BY item.drain_id
)
UPDATE payment_drains drain
SET accepted_count = recount.accepted_count,
    terminal_count = recount.terminal_count,
    status = CASE WHEN recount.accepted_count = 0 THEN 'completed' ELSE 'active' END,
    completed_at = CASE
        WHEN recount.accepted_count = 0
            THEN COALESCE(drain.completed_at, transaction_timestamp())
        ELSE NULL
    END,
    updated_at = transaction_timestamp()
FROM recount
WHERE drain.id = recount.drain_id;
