-- no-transaction
-- CREATE INDEX CONCURRENTLY cannot run in a transaction block, and a simple-query
-- file with two statements is one implicit transaction, so this file holds exactly
-- one statement. It waits for older transactions but never blocks Locks reads or writes.
-- After any interrupted deploy, check pg_index.indisvalid; if the index is invalid,
-- DROP INDEX CONCURRENTLY it and rerun.
CREATE INDEX CONCURRENTLY IF NOT EXISTS usdt_observations_marketplace_settlement_index
    ON usdt_observations (marketplace_preparation_id)
    WHERE marketplace_preparation_id IS NOT NULL;
