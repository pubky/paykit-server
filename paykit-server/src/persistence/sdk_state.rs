//! Encrypted full Paykit SDK state snapshots.

use std::sync::Arc;

use async_trait::async_trait;
use paykit_sdk::{
    LinkedPeerState, PaykitSdkError, PubkyPublicKey,
    storage::{
        StorageAdapter, StorageState, StorageTransactionCallback, run_storage_state_transaction,
    },
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    application::connection_status::{ConnectionBinding, PaykitConnectionState},
    crypto::{Crypto, EncryptedEnvelope, EnvelopeContext, LookupHash},
    domain::locks::CreatorPubky,
    persistence::{PersistenceError, creators::CreatorRow},
};

/// Encrypted SDK-state repository with one locked state row per mutation.
#[derive(Clone, Debug)]
pub struct SdkStateStore {
    pool: PgPool,
    crypto: Arc<Crypto>,
}

/// Creator-scoped public Paykit SDK storage adapter backed by one encrypted
/// PostgreSQL `StorageState` row.
#[derive(Clone)]
pub struct PostgresStorageAdapter {
    pool: PgPool,
    crypto: Arc<Crypto>,
    creator_id: Uuid,
}

impl std::fmt::Debug for PostgresStorageAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PostgresStorageAdapter { <redacted> }")
    }
}

impl PostgresStorageAdapter {
    pub fn new(pool: &PgPool, crypto: Arc<Crypto>, creator_id: Uuid) -> Self {
        Self {
            pool: pool.clone(),
            crypto,
            creator_id,
        }
    }

    pub(crate) fn creator_id(&self) -> Uuid {
        self.creator_id
    }
}

#[async_trait]
impl StorageAdapter for PostgresStorageAdapter {
    async fn transaction_erased<'a>(
        &self,
        callback: StorageTransactionCallback<'a>,
    ) -> paykit_sdk::Result<Box<dyn std::any::Any + Send>> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let (creator_id, lookup_hash): (Uuid, Vec<u8>) =
            sqlx::query_as("SELECT id, creator_lookup_hash FROM creators WHERE id = $1")
                .bind(self.creator_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| storage_context("creator SDK state is unavailable"))?;
        let envelope: Vec<u8> = sqlx::query_scalar(
            "SELECT state_envelope FROM sdk_states WHERE creator_id = $1 FOR UPDATE",
        )
        .bind(self.creator_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| storage_context("creator SDK state is unavailable"))?;
        let hash_bytes: [u8; 32] = lookup_hash
            .try_into()
            .map_err(|_| storage_context("creator lookup hash is invalid"))?;
        let hash = LookupHash::from_bytes(hash_bytes);
        let state =
            decrypt_state(&self.crypto, hash, creator_id, &envelope).map_err(persistence_error)?;
        let (updated, result) = run_storage_state_transaction(state, callback)?;
        let encrypted =
            encrypt_state(&self.crypto, hash, creator_id, &updated).map_err(persistence_error)?;
        let changed = sqlx::query(
            "UPDATE sdk_states SET state_envelope = $1, updated_at = NOW() WHERE creator_id = $2",
        )
        .bind(encrypted.as_bytes())
        .bind(self.creator_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        if changed.rows_affected() != 1 {
            return Err(storage_context("creator SDK state update was lost"));
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(result)
    }
}

fn storage_context(context: &str) -> PaykitSdkError {
    PaykitSdkError::Storage {
        context: context.into(),
        source: None,
    }
}

fn storage_error(error: sqlx::Error) -> PaykitSdkError {
    PaykitSdkError::Storage {
        context: "PostgreSQL SDK state transaction failed".into(),
        source: Some(anyhow::anyhow!(error.to_string())),
    }
}

fn persistence_error(_error: PersistenceError) -> PaykitSdkError {
    storage_context("encrypted creator SDK state is invalid")
}

impl SdkStateStore {
    /// Creates an SDK state repository using a deployment-scoped crypto context.
    pub fn new(pool: &PgPool, crypto: Arc<Crypto>) -> Self {
        Self {
            pool: pool.clone(),
            crypto,
        }
    }

    /// Loads the complete SDK state for one creator.
    pub async fn load(&self, creator: &CreatorPubky) -> Result<StorageState, PersistenceError> {
        let row = creator_row(&self.pool, &self.crypto, creator).await?;
        load_state_by_row(&self.pool, &self.crypto, &row).await
    }

    /// Reads the exact persisted peer state without taking a mutation lock or
    /// rewriting the encrypted SDK snapshot.
    pub async fn connection_state(
        &self,
        creator: &CreatorPubky,
        binding: &ConnectionBinding,
    ) -> Result<PaykitConnectionState, PersistenceError> {
        let state = self.load(creator).await?;
        let reader = PubkyPublicKey::from_raw_or_app_key(binding.reader().to_string())
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let peer = state
            .linked_peers
            .get(&(reader, binding.reader_path().clone()));
        map_linked_peer_state(peer.map(|record| &record.state))
    }

    /// Atomically decrypts, synchronously mutates, and replaces one full SDK state snapshot.
    pub async fn update<F>(&self, creator: &CreatorPubky, mutate: F) -> Result<(), PersistenceError>
    where
        F: FnOnce(&mut StorageState) + Send,
    {
        let hash = self.crypto.lookup_hash(creator.to_string().as_bytes());
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let row = sqlx::query_as::<_, CreatorRow>("SELECT id, creator_lookup_hash, credential_envelope FROM creators WHERE creator_lookup_hash = $1")
            .bind(hash.as_bytes().as_slice()).fetch_optional(&mut *tx).await.map_err(|_| PersistenceError::Unavailable)?.ok_or(PersistenceError::CorruptOrMissing)?;
        let state_envelope: Vec<u8> = sqlx::query_scalar(
            "SELECT state_envelope FROM sdk_states WHERE creator_id = $1 FOR UPDATE",
        )
        .bind(row.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?
        .ok_or(PersistenceError::CorruptOrMissing)?;
        let mut state = decrypt_state(&self.crypto, row.lookup_hash()?, row.id, &state_envelope)?;
        mutate(&mut state);
        let envelope = encrypt_state(&self.crypto, row.lookup_hash()?, row.id, &state)?;
        sqlx::query(
            "UPDATE sdk_states SET state_envelope = $1, updated_at = NOW() WHERE creator_id = $2",
        )
        .bind(envelope.as_bytes())
        .bind(row.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        tx.commit().await.map_err(|_| PersistenceError::Unavailable)
    }
}

fn map_linked_peer_state(
    state: Option<&LinkedPeerState>,
) -> Result<PaykitConnectionState, PersistenceError> {
    match state {
        None | Some(LinkedPeerState::NotLinked) => Ok(PaykitConnectionState::None),
        Some(LinkedPeerState::Linking) => Ok(PaykitConnectionState::Handshake),
        Some(LinkedPeerState::Linked) => Ok(PaykitConnectionState::Connected),
        Some(LinkedPeerState::RecoveryRequired) => Ok(PaykitConnectionState::RecoveryRequired),
        Some(LinkedPeerState::Blocked) => Ok(PaykitConnectionState::Blocked),
        // LinkedPeerState is non-exhaustive. Unknown future states fail closed.
        Some(_) => Err(PersistenceError::CorruptOrMissing),
    }
}

/// Plaintext layout of `StorageState` before Allowance accounting: what
/// paykit-sdk v0.1.0-rc48 through rc55 write.
const SDK_STATE_V1: u8 = 1;
/// Plaintext layout of the current `StorageState`. The Allowances SDK added
/// `allowance_accounting` as its first field and left every other field and
/// record type unchanged, so a version 1 body is a version 2 body without the
/// leading `None` tag.
const SDK_STATE_V2: u8 = 2;
/// Postcard encoding of `Option::None`.
const POSTCARD_NONE: u8 = 0;

#[derive(Serialize)]
struct SdkStateRef<'a> {
    version: u8,
    state: &'a StorageState,
}

#[derive(Deserialize)]
struct SdkState {
    version: u8,
    state: StorageState,
}

/// Encodes one SDK state plaintext.
///
/// A state without Allowance accounting is written as version 1, which the
/// rc48 server can still read, so a deployment can move between the two
/// server builds on the same database. A Paykit Server is never the payer, so
/// its accounting stays empty. A state with accounting is written as version 2.
pub(crate) fn encode_state(state: &StorageState) -> Result<Zeroizing<Vec<u8>>, PersistenceError> {
    let mut bytes = Zeroizing::new(
        postcard::to_allocvec(&SdkStateRef {
            version: SDK_STATE_V2,
            state,
        })
        .map_err(|_| PersistenceError::CorruptOrMissing)?,
    );
    if state.allowance_accounting.is_none() {
        if bytes.get(1) != Some(&POSTCARD_NONE) {
            return Err(PersistenceError::CorruptOrMissing);
        }
        bytes.remove(1);
        bytes[0] = SDK_STATE_V1;
    }
    Ok(bytes)
}

/// Decodes a version 1 (rc48 to rc55) or version 2 SDK state plaintext.
pub(crate) fn decode_state(bytes: &[u8]) -> Result<StorageState, PersistenceError> {
    let current = match bytes.split_first() {
        Some((&SDK_STATE_V1, body)) => {
            let mut current = Zeroizing::new(Vec::with_capacity(bytes.len() + 1));
            current.extend_from_slice(&[SDK_STATE_V2, POSTCARD_NONE]);
            current.extend_from_slice(body);
            current
        }
        Some((&SDK_STATE_V2, _)) => Zeroizing::new(bytes.to_vec()),
        _ => return Err(PersistenceError::CorruptOrMissing),
    };
    let wire: SdkState =
        postcard::from_bytes(&current).map_err(|_| PersistenceError::CorruptOrMissing)?;
    if wire.version != SDK_STATE_V2 {
        return Err(PersistenceError::CorruptOrMissing);
    }
    Ok(wire.state)
}

pub(crate) fn encrypt_state(
    crypto: &Crypto,
    hash: LookupHash,
    id: Uuid,
    state: &StorageState,
) -> Result<EncryptedEnvelope, PersistenceError> {
    let bytes = encode_state(state)?;
    crypto
        .encrypt(&EnvelopeContext::sdk_state(hash, id), &bytes)
        .map_err(|_| PersistenceError::CorruptOrMissing)
}

pub(crate) async fn load_state_by_row(
    pool: &PgPool,
    crypto: &Crypto,
    row: &CreatorRow,
) -> Result<StorageState, PersistenceError> {
    let envelope: Vec<u8> =
        sqlx::query_scalar("SELECT state_envelope FROM sdk_states WHERE creator_id = $1")
            .bind(row.id)
            .fetch_optional(pool)
            .await
            .map_err(|_| PersistenceError::Unavailable)?
            .ok_or(PersistenceError::CorruptOrMissing)?;
    decrypt_state(crypto, row.lookup_hash()?, row.id, &envelope)
}

pub(crate) fn decrypt_state(
    crypto: &Crypto,
    hash: LookupHash,
    id: Uuid,
    envelope: &[u8],
) -> Result<StorageState, PersistenceError> {
    let bytes = Zeroizing::new(
        crypto
            .decrypt(
                &EnvelopeContext::sdk_state(hash, id),
                &EncryptedEnvelope::from_bytes(envelope.to_vec()),
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?,
    );
    decode_state(&bytes)
}

async fn creator_row(
    pool: &PgPool,
    crypto: &Crypto,
    creator: &CreatorPubky,
) -> Result<CreatorRow, PersistenceError> {
    let hash = crypto.lookup_hash(creator.to_string().as_bytes());
    sqlx::query_as("SELECT id, creator_lookup_hash, credential_envelope FROM creators WHERE creator_lookup_hash = $1")
        .bind(hash.as_bytes().as_slice()).fetch_optional(pool).await.map_err(|_| PersistenceError::Unavailable)?.ok_or(PersistenceError::CorruptOrMissing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[test]
    fn linked_peer_states_map_to_full_public_vocabulary() {
        use crate::application::connection_status::PaykitConnectionState;

        for (input, expected) in [
            (None, PaykitConnectionState::None),
            (
                Some(&LinkedPeerState::NotLinked),
                PaykitConnectionState::None,
            ),
            (
                Some(&LinkedPeerState::Linking),
                PaykitConnectionState::Handshake,
            ),
            (
                Some(&LinkedPeerState::Linked),
                PaykitConnectionState::Connected,
            ),
            (
                Some(&LinkedPeerState::RecoveryRequired),
                PaykitConnectionState::RecoveryRequired,
            ),
            (
                Some(&LinkedPeerState::Blocked),
                PaykitConnectionState::Blocked,
            ),
        ] {
            assert_eq!(map_linked_peer_state(input), Ok(expected));
        }
    }

    /// State written by paykit-sdk v0.1.0-rc55 in the version 1 envelope: a
    /// linked server receiver after one Locks handoff, and the reader that
    /// received and accepted the request. The rc48 server decodes both and
    /// re-encodes them to the same bytes.
    const RC55_SERVER_STATE: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sdk-state/rc55-server-state.postcard"
    ));
    const RC55_READER_STATE: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sdk-state/rc55-reader-state.postcard"
    ));

    #[test]
    fn rc55_state_is_not_the_current_layout() {
        for fixture in [RC55_SERVER_STATE, RC55_READER_STATE] {
            let mut relabelled = fixture.to_vec();
            relabelled[0] = SDK_STATE_V2;
            assert!(postcard::from_bytes::<SdkState>(&relabelled).is_err());
        }
    }

    #[test]
    fn rc55_state_decodes_and_reencodes_to_the_same_bytes() {
        let server = decode_state(RC55_SERVER_STATE).unwrap();
        assert!(server.allowance_accounting.is_none());
        assert!(server.identity_state.is_some());
        assert_eq!(server.linked_peers.len(), 1);
        assert!(
            server
                .linked_peers
                .values()
                .all(|peer| peer.state == LinkedPeerState::Linked)
        );
        assert_eq!(server.encrypted_link_states.len(), 1);
        assert_eq!(server.outbound_private_messages.len(), 2);

        let reader = decode_state(RC55_READER_STATE).unwrap();
        assert_eq!(reader.private_stream_items.len(), 2);
        assert_eq!(reader.event_dedup_records.len(), 1);
        assert_eq!(reader.outbound_private_messages.len(), 1);

        assert_eq!(*encode_state(&server).unwrap(), RC55_SERVER_STATE);
        assert_eq!(*encode_state(&reader).unwrap(), RC55_READER_STATE);
    }

    #[test]
    fn state_with_allowance_accounting_round_trips_as_version_two() {
        let mut state = decode_state(RC55_READER_STATE).unwrap();
        state.allowance_accounting = Some(paykit_sdk::AllowanceAccountingState {
            revision: 3,
            epoch: "epoch-1".into(),
            requires_reconciliation: true,
            history: Default::default(),
        });
        let bytes = encode_state(&state).unwrap();
        assert_eq!(bytes[0], SDK_STATE_V2);
        assert!(decode_state(&bytes).unwrap() == state);
    }

    #[test]
    fn unknown_empty_and_truncated_state_fails_closed() {
        let mut unknown = RC55_SERVER_STATE.to_vec();
        unknown[0] = 3;
        for bytes in [
            &unknown[..],
            &[],
            &RC55_SERVER_STATE[..1],
            &RC55_SERVER_STATE[..RC55_SERVER_STATE.len() - 1],
        ] {
            assert!(matches!(
                decode_state(bytes),
                Err(PersistenceError::CorruptOrMissing)
            ));
        }
    }

    #[tokio::test]
    async fn postgres_storage_adapter_debug_redacts_creator_identity() {
        let creator_id = Uuid::new_v4();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/paykit_debug_test")
            .unwrap();
        let adapter = PostgresStorageAdapter::new(
            &pool,
            Arc::new(Crypto::from_master_key(&[9; 32]).unwrap()),
            creator_id,
        );

        let debug = format!("{adapter:?}");
        assert!(!debug.contains(&creator_id.to_string()));
        assert_eq!(debug, "PostgresStorageAdapter { <redacted> }");
    }
}
