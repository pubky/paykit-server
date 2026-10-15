-- no-transaction
-- CREATE INDEX CONCURRENTLY cannot run in a transaction block, and a simple-query
-- file with two statements is one implicit transaction, so this file holds exactly
-- one statement. It waits for older transactions but never blocks Locks reads or writes.
-- If an interrupted build leaves an INVALID index, drop it and rerun the migration.
CREATE INDEX CONCURRENTLY IF NOT EXISTS usdt_observations_marketplace_settlement_index
    ON usdt_observations (marketplace_preparation_id)
    WHERE marketplace_preparation_id IS NOT NULL;
