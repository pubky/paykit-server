-- When the SDK recorded the server's own Payment Request proposal. Payment
-- evidence earlier than this, less the payment method's tolerance, does not
-- pay the request. NULL until the lifecycle projection next records it, and
-- no evidence pays the request meanwhile; its receipts are still kept.
ALTER TABLE payment_request_lifecycles
    ADD COLUMN proposal_recorded_at TIMESTAMPTZ;

-- A payment marker credited from USDT alone (no timely Bitcoin outpoint) before this
-- rule is recomputed under it once the invoice's proposal time is recorded.
-- Until then it stands, so an invoice paid under the old rule is neither
-- expired nor treated as unpaid while its attribution is undecided.
ALTER TABLE invoices
    ADD COLUMN usdt_credit_reconcile BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE invoices
SET usdt_credit_reconcile = TRUE
WHERE first_amount_matched_observed_at IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM invoice_timely_amount_matched_outpoints timely
      WHERE timely.invoice_id = invoices.id
  );
