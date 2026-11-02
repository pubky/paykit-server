-- When the SDK recorded the server's own Payment Request proposal. Payment
-- evidence earlier than this, less the payment method's tolerance, does not
-- pay the request. NULL until the lifecycle projection next records it, and
-- no evidence pays the request meanwhile; its receipts are still kept.
ALTER TABLE payment_request_lifecycles
    ADD COLUMN proposal_recorded_at TIMESTAMPTZ;
