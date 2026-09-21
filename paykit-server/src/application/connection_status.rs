//! Read-only Paykit Noise connection state bound to a persisted invoice.

use std::sync::Arc;

use async_trait::async_trait;
use paykit_lib::PaykitReceiverPath;
use serde::Serialize;

use crate::{
    domain::locks::{BundleId, CreatorPubky, ReaderPubky},
    persistence::{InvoiceStore, PersistenceError, SdkStateStore},
};

/// Exact reader/path binding accepted when the invoice was created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionBinding {
    reader: ReaderPubky,
    reader_path: PaykitReceiverPath,
}

impl ConnectionBinding {
    pub fn new(reader: ReaderPubky, reader_path: PaykitReceiverPath) -> Self {
        Self {
            reader,
            reader_path,
        }
    }

    pub fn reader(&self) -> &ReaderPubky {
        &self.reader
    }

    pub fn reader_path(&self) -> &PaykitReceiverPath {
        &self.reader_path
    }
}

/// Closed projection of Paykit Server's local persisted peer state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaykitConnectionState {
    None,
    Handshake,
    Connected,
    RecoveryRequired,
    Blocked,
}

#[async_trait]
pub trait ConnectionBindingRepository: Send + Sync {
    async fn binding(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<ConnectionBinding>, PersistenceError>;
}

#[async_trait]
pub trait PeerConnectionStateRepository: Send + Sync {
    async fn connection_state(
        &self,
        creator: &CreatorPubky,
        binding: &ConnectionBinding,
    ) -> Result<PaykitConnectionState, PersistenceError>;
}

#[async_trait]
impl ConnectionBindingRepository for InvoiceStore {
    async fn binding(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<ConnectionBinding>, PersistenceError> {
        self.connection_binding(creator, bundle_id).await
    }
}

#[async_trait]
impl PeerConnectionStateRepository for SdkStateStore {
    async fn connection_state(
        &self,
        creator: &CreatorPubky,
        binding: &ConnectionBinding,
    ) -> Result<PaykitConnectionState, PersistenceError> {
        SdkStateStore::connection_state(self, creator, binding).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStatusError {
    NotFound,
    Unavailable,
}

/// Resolves caller-supplied public identity only to persisted server-owned binding data.
pub struct ConnectionStatusService {
    bindings: Arc<dyn ConnectionBindingRepository>,
    peers: Arc<dyn PeerConnectionStateRepository>,
}

impl ConnectionStatusService {
    pub fn new(
        bindings: Arc<dyn ConnectionBindingRepository>,
        peers: Arc<dyn PeerConnectionStateRepository>,
    ) -> Self {
        Self { bindings, peers }
    }

    pub async fn status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaykitConnectionState, ConnectionStatusError> {
        let binding = self
            .bindings
            .binding(creator, bundle_id)
            .await
            .map_err(|_| ConnectionStatusError::Unavailable)?
            .ok_or(ConnectionStatusError::NotFound)?;
        self.peers
            .connection_state(creator, &binding)
            .await
            .map_err(|_| ConnectionStatusError::Unavailable)
    }
}
