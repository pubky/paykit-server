use std::borrow::Cow;

use sqlx::{PgConnection, PgPool, migrate::Migrator};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// PostgreSQL advisory-lock key serializing forward-only schema migrations.
pub const MIGRATION_ADVISORY_LOCK_KEY: i64 = 4_679_362_128_418_235_709;

/// A detached PostgreSQL session holding the migration advisory lock.
///
/// Dropping this value closes the detached connection, so PostgreSQL releases the
/// session lock even when startup is cancelled or returns an error.
pub struct MigrationLock {
    connection: PgConnection,
}

impl MigrationLock {
    /// Acquires exclusive ownership of schema migration startup.
    pub async fn acquire(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut connection = pool.acquire().await?.detach();
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(MIGRATION_ADVISORY_LOCK_KEY)
            .execute(&mut connection)
            .await?;
        Ok(Self { connection })
    }

    /// Applies all remaining embedded migrations while retaining the outer lock.
    pub async fn run_remaining(&mut self) -> Result<(), sqlx::Error> {
        let mut migrator = Migrator {
            migrations: Cow::Owned(MIGRATOR.iter().cloned().collect()),
            ignore_missing: false,
            locking: false,
            no_tx: false,
        };
        migrator.set_locking(false);
        migrator
            .run_direct(&mut self.connection)
            .await
            .map_err(Into::into)
    }

    /// Releases the advisory lock explicitly after successful startup migration.
    pub async fn release(mut self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(MIGRATION_ADVISORY_LOCK_KEY)
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }
}

/// Applies embedded forward-only PostgreSQL migrations exactly once per database.
///
/// Production and test startup both apply the single pre-production baseline.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut lock = MigrationLock::acquire(pool).await?;
    lock.run_remaining().await?;
    lock.release().await
}
