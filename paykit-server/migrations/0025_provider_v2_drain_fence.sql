-- Persist the provider-v2 invoice admission fence beyond operational drain cleanup.
-- Existing active or cleaned drain cycles already acknowledged a frozen snapshot,
-- so upgrade them fail-closed.
ALTER TABLE lock_payment_generations
    ADD COLUMN provider_v2_drain_fenced BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE lock_payment_generations
SET provider_v2_drain_fenced = TRUE
WHERE active_drain_id IS NOT NULL OR last_cleanup_token IS NOT NULL;

CREATE FUNCTION reject_provider_v2_drain_fence_removal()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.provider_v2_drain_fenced AND NOT NEW.provider_v2_drain_fenced THEN
        RAISE EXCEPTION 'provider-v2 drain fence is permanent';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER lock_payment_generations_provider_v2_fence_no_removal
BEFORE UPDATE OF provider_v2_drain_fenced ON lock_payment_generations
FOR EACH ROW EXECUTE FUNCTION reject_provider_v2_drain_fence_removal();
