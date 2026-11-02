-- When the SDK recorded the server's own Payment Request proposal. Payment
-- evidence earlier than this, less the payment method's tolerance, does not
-- pay the request. NULL until the lifecycle projection next records it; the
-- existing invoice and activation window applies meanwhile.
ALTER TABLE payment_request_lifecycles
    ADD COLUMN proposal_recorded_at TIMESTAMPTZ;
