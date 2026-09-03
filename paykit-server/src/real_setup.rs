//! Real server-side orchestration for one Bitkit setup flow.
//!
//! Normal Pubky AUTH is completed before the companion envelope is accepted.
//! The companion signature is verified against that authenticated creator, then
//! xpub validation, marker publish/read-back, encrypted persistence, and relay
//! acknowledgement happen in that order.

use std::{any::Any, sync::Arc, time::Duration};

use async_trait::async_trait;
use bitcoin::bip32::Xpub;
use ed25519_dalek::VerifyingKey;
use paykit_lib::{PaykitReceiverCapabilities, PaykitReceiverMarker, PaykitReceiverPath};
use paykit_sdk::{ReceiverNoiseSecretKey, storage::StorageState};

use crate::{
    application::create_invoice::derive_bip84_p2wpkh_address,
    bitkit_claim::{ClaimError, WatchOnlyAccountClaim},
    bitkit_setup::{BitkitAuthStarter, StartedBitkitAuth},
    config::BitcoinNetwork,
    domain::locks::parse_creator,
    persistence::{CreatorCredentials, CreatorStore},
    setup::{Completion, SetupAttempt, SetupCompleter, StartedSetup},
    setup_diagnostics::{SetupFailureClass, SetupOutcome, SetupStage, emit_setup_stage},
    setup_orchestration::{CompanionRelay, receive_verify_commit},
};

fn default_marker_capabilities() -> PaykitReceiverCapabilities {
    PaykitReceiverCapabilities {
        private_payments: true,
        payment_requests: true,
        receipts: false,
        outgoing_payments: false,
    }
}

fn definitive_setup_failure(stage: SetupStage, class: SetupFailureClass) -> Completion {
    emit_setup_stage(stage, SetupOutcome::Failed, class);
    Completion::DefinitiveFailure
}

/// Marker I/O is a narrow test seam. Production uses [`DirectMarkerPublisher`],
/// which calls Paykit's Pubky helpers directly.
#[async_trait]
pub trait MarkerPublisher: Send + Sync {
    async fn publish_and_readback(
        &self,
        session: &pubky::PubkySession,
        public_storage_client: &pubky::Pubky,
        owner: &paykit_lib::PublicKey,
        marker: &PaykitReceiverMarker,
    ) -> Result<(), ClaimError>;
    async fn remove(
        &self,
        session: &pubky::PubkySession,
        receiver_path: &PaykitReceiverPath,
    ) -> Result<(), ClaimError>;
}

/// Production marker publisher: publish to the authenticated creator's
/// homeserver and independently read it back through public storage.
#[derive(Clone, Default)]
pub struct DirectMarkerPublisher;

#[async_trait]
impl MarkerPublisher for DirectMarkerPublisher {
    async fn publish_and_readback(
        &self,
        session: &pubky::PubkySession,
        public_storage_client: &pubky::Pubky,
        owner: &paykit_lib::PublicKey,
        marker: &PaykitReceiverMarker,
    ) -> Result<(), ClaimError> {
        emit_setup_stage(
            SetupStage::MarkerPublish,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        if paykit_lib::publish_paykit_receiver_marker(session, marker)
            .await
            .is_err()
        {
            emit_setup_stage(
                SetupStage::MarkerPublish,
                SetupOutcome::Failed,
                SetupFailureClass::Transport,
            );
            return Err(ClaimError::InvalidEnvelope);
        }
        emit_setup_stage(
            SetupStage::MarkerPublish,
            SetupOutcome::Succeeded,
            SetupFailureClass::None,
        );
        emit_setup_stage(
            SetupStage::MarkerReadback,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let readback = match paykit_lib::get_paykit_receiver_marker(
            &public_storage_client.public_storage(),
            owner,
            &marker.receiver_path,
        )
        .await
        {
            Ok(readback) => readback,
            Err(_) => {
                emit_setup_stage(
                    SetupStage::MarkerReadback,
                    SetupOutcome::Failed,
                    SetupFailureClass::Transport,
                );
                return Err(ClaimError::InvalidEnvelope);
            }
        };
        if readback != Some(marker.clone()) {
            emit_setup_stage(
                SetupStage::MarkerReadback,
                SetupOutcome::Failed,
                SetupFailureClass::ReadbackMismatch,
            );
            return Err(ClaimError::InvalidEnvelope);
        }
        emit_setup_stage(
            SetupStage::MarkerReadback,
            SetupOutcome::Succeeded,
            SetupFailureClass::None,
        );
        Ok(())
    }

    async fn remove(
        &self,
        session: &pubky::PubkySession,
        receiver_path: &PaykitReceiverPath,
    ) -> Result<(), ClaimError> {
        paykit_lib::remove_paykit_receiver_marker(session, receiver_path)
            .await
            .map_err(|_| ClaimError::InvalidEnvelope)
    }
}

/// Concrete server-owned `SetupCompleter` composed from the normal SDK auth
/// starter, Pubky companion relay, direct marker I/O, and encrypted CreatorStore.
#[derive(Clone)]
pub struct RealSetupCompleter {
    starter: BitkitAuthStarter,
    relay: Arc<dyn CompanionRelay>,
    marker_publisher: Arc<dyn MarkerPublisher>,
    creators: CreatorStore,
    bitcoin_network: BitcoinNetwork,
    receiver_path: PaykitReceiverPath,
    marker_capabilities: PaykitReceiverCapabilities,
    relay_deadline: Duration,
}

impl RealSetupCompleter {
    pub fn new(
        starter: BitkitAuthStarter,
        relay: Arc<dyn CompanionRelay>,
        creators: CreatorStore,
        bitcoin_network: BitcoinNetwork,
        receiver_path: PaykitReceiverPath,
    ) -> Self {
        Self::with_marker_publisher(
            starter,
            relay,
            Arc::new(DirectMarkerPublisher),
            creators,
            bitcoin_network,
            receiver_path,
        )
    }

    pub fn with_marker_publisher(
        starter: BitkitAuthStarter,
        relay: Arc<dyn CompanionRelay>,
        marker_publisher: Arc<dyn MarkerPublisher>,
        creators: CreatorStore,
        bitcoin_network: BitcoinNetwork,
        receiver_path: PaykitReceiverPath,
    ) -> Self {
        Self {
            starter,
            relay,
            marker_publisher,
            creators,
            bitcoin_network,
            receiver_path,
            marker_capabilities: default_marker_capabilities(),
            relay_deadline: Duration::from_secs(30),
        }
    }
}

struct BitkitSetupAttempt(StartedBitkitAuth);

impl SetupAttempt for BitkitSetupAttempt {
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

#[async_trait]
impl SetupCompleter for RealSetupCompleter {
    async fn start(&self) -> Result<StartedSetup, Completion> {
        let started = self
            .starter
            .start()
            .await
            .map_err(|_| Completion::TransientUnavailable)?;
        Ok(StartedSetup::new(
            started.authorization_url.clone(),
            Box::new(BitkitSetupAttempt(started)),
        ))
    }

    async fn complete(&self, attempt: Box<dyn SetupAttempt>) -> Completion {
        let Ok(attempt) = attempt.into_any().downcast::<BitkitSetupAttempt>() else {
            return definitive_setup_failure(
                SetupStage::AuthComplete,
                SetupFailureClass::InvalidRequest,
            );
        };
        let attempt = attempt.0;
        let capabilities = attempt.capabilities().to_owned();

        // The authenticated identity is not available until the normal auth
        // flow completes. Start with a fresh Noise key, then replace it with
        // the persisted key before any marker/persistence work on reauth.
        emit_setup_stage(
            SetupStage::AuthComplete,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let auth = match attempt
            .auth_request
            .complete(None, ReceiverNoiseSecretKey::random(), &capabilities)
            .await
        {
            Ok(auth) => {
                emit_setup_stage(
                    SetupStage::AuthComplete,
                    SetupOutcome::Succeeded,
                    SetupFailureClass::None,
                );
                auth
            }
            Err(_) => {
                return definitive_setup_failure(SetupStage::AuthComplete, SetupFailureClass::Auth);
            }
        };
        emit_setup_stage(
            SetupStage::IdentityValidate,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let owner = match auth.public_key.to_public_key() {
            Ok(owner) => owner,
            Err(_) => {
                return definitive_setup_failure(
                    SetupStage::IdentityValidate,
                    SetupFailureClass::InvalidIdentity,
                );
            }
        };
        let creator = match parse_creator(&auth.public_key.to_app_key()) {
            Ok(creator) => creator,
            Err(_) => {
                return definitive_setup_failure(
                    SetupStage::IdentityValidate,
                    SetupFailureClass::InvalidIdentity,
                );
            }
        };
        let verifying_key = match VerifyingKey::from_bytes(owner.as_bytes()) {
            Ok(key) => key,
            Err(_) => {
                return definitive_setup_failure(
                    SetupStage::IdentityValidate,
                    SetupFailureClass::InvalidIdentity,
                );
            }
        };
        emit_setup_stage(
            SetupStage::IdentityValidate,
            SetupOutcome::Succeeded,
            SetupFailureClass::None,
        );
        emit_setup_stage(
            SetupStage::SessionExport,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let session_secret = match auth.export_session_secret().await {
            Ok(secret) => {
                emit_setup_stage(
                    SetupStage::SessionExport,
                    SetupOutcome::Succeeded,
                    SetupFailureClass::None,
                );
                secret.into_inner()
            }
            Err(_) => {
                return definitive_setup_failure(
                    SetupStage::SessionExport,
                    SetupFailureClass::SessionExport,
                );
            }
        };
        let commit = CreatorSetupCommit {
            session: auth.access.session,
            public_storage_client: auth.access.outbox_client,
            owner,
            creator,
            session_secret,
            initial_noise_secret: auth.access.receiver_noise_secret_key,
            creators: self.creators.clone(),
            marker_publisher: self.marker_publisher.clone(),
            bitcoin_network: self.bitcoin_network.clone(),
            receiver_path: self.receiver_path.clone(),
            marker_capabilities: self.marker_capabilities,
        };
        match receive_verify_commit(
            self.relay.as_ref(),
            &commit,
            &attempt.request,
            &verifying_key,
            self.relay_deadline,
        )
        .await
        {
            Ok(true) => Completion::DurableSuccess,
            // No relay body is not a successful setup and the consumed auth
            // request cannot be safely replayed.
            Ok(false) | Err(_) => Completion::DefinitiveFailure,
        }
    }
}

struct CreatorSetupCommit {
    session: pubky::PubkySession,
    public_storage_client: pubky::Pubky,
    owner: paykit_lib::PublicKey,
    creator: crate::domain::locks::CreatorPubky,
    session_secret: String,
    initial_noise_secret: ReceiverNoiseSecretKey,
    creators: CreatorStore,
    marker_publisher: Arc<dyn MarkerPublisher>,
    bitcoin_network: BitcoinNetwork,
    receiver_path: PaykitReceiverPath,
    marker_capabilities: PaykitReceiverCapabilities,
}

#[async_trait]
impl crate::setup_orchestration::VerifiedSetupCommit for CreatorSetupCommit {
    async fn publish_readback_and_commit(
        &self,
        claim: WatchOnlyAccountClaim,
    ) -> Result<(), ClaimError> {
        emit_setup_stage(
            SetupStage::XpubValidate,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let xpub = match validate_xpub(
            &claim.serialized_xpub,
            claim.account_index,
            &self.bitcoin_network,
        ) {
            Ok(xpub) => {
                emit_setup_stage(
                    SetupStage::XpubValidate,
                    SetupOutcome::Succeeded,
                    SetupFailureClass::None,
                );
                xpub
            }
            Err(error) => {
                emit_setup_stage(
                    SetupStage::XpubValidate,
                    SetupOutcome::Failed,
                    SetupFailureClass::InvalidPayload,
                );
                return Err(error);
            }
        };
        emit_setup_stage(
            SetupStage::LockAcquire,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let setup_lock = match self.creators.acquire_setup_lock(&self.creator).await {
            Ok(lock) => {
                emit_setup_stage(
                    SetupStage::LockAcquire,
                    SetupOutcome::Succeeded,
                    SetupFailureClass::None,
                );
                lock
            }
            Err(_) => {
                emit_setup_stage(
                    SetupStage::LockAcquire,
                    SetupOutcome::Failed,
                    SetupFailureClass::Storage,
                );
                return Err(ClaimError::InvalidEnvelope);
            }
        };
        let commit_result = async {
            emit_setup_stage(
                SetupStage::CreatorLoad,
                SetupOutcome::Started,
                SetupFailureClass::None,
            );
            let existing = match self.creators.load_optional(&self.creator).await {
                Ok(existing) => {
                    emit_setup_stage(
                        SetupStage::CreatorLoad,
                        SetupOutcome::Succeeded,
                        SetupFailureClass::None,
                    );
                    existing
                }
                Err(_) => {
                    emit_setup_stage(
                        SetupStage::CreatorLoad,
                        SetupOutcome::Failed,
                        SetupFailureClass::Storage,
                    );
                    return Err(ClaimError::InvalidEnvelope);
                }
            };
            let noise_secret = existing
                .as_ref()
                .map(|credentials| credentials.receiver_noise_secret().clone())
                .unwrap_or_else(|| self.initial_noise_secret.clone());
            let marker = PaykitReceiverMarker::new(
                self.receiver_path.clone(),
                self.marker_capabilities,
                noise_secret.public_key(),
            );
            self.marker_publisher
                .publish_and_readback(
                    &self.session,
                    &self.public_storage_client,
                    &self.owner,
                    &marker,
                )
                .await?;
            let credentials = CreatorCredentials::new(
                self.creator.clone(),
                self.session_secret.clone(),
                noise_secret,
                xpub,
                claim.account_index,
            );
            emit_setup_stage(
                SetupStage::Persistence,
                SetupOutcome::Started,
                SetupFailureClass::None,
            );
            let persistence = match existing {
                Some(_) => self.creators.reauthenticate(&credentials).await,
                None => self
                    .creators
                    .create(&credentials, &StorageState::default())
                    .await
                    .map(|_| ()),
            };
            if persistence.is_err() {
                emit_setup_stage(
                    SetupStage::Persistence,
                    SetupOutcome::Failed,
                    SetupFailureClass::Storage,
                );
                // Publication and Postgres cannot share a transaction. This
                // creator-scoped lock covers load, publication, persistence,
                // and compensation, so a failed first creator cannot remove a
                // concurrent winner's receiver marker. Reauth never removes
                // its existing marker.
                if existing.is_none() {
                    emit_setup_stage(
                        SetupStage::Compensation,
                        SetupOutcome::Started,
                        SetupFailureClass::None,
                    );
                    let compensation = self
                        .marker_publisher
                        .remove(&self.session, &self.receiver_path)
                        .await;
                    emit_setup_stage(
                        SetupStage::Compensation,
                        if compensation.is_ok() {
                            SetupOutcome::Succeeded
                        } else {
                            SetupOutcome::Failed
                        },
                        if compensation.is_ok() {
                            SetupFailureClass::None
                        } else {
                            SetupFailureClass::Transport
                        },
                    );
                }
                return Err(ClaimError::InvalidEnvelope);
            }
            emit_setup_stage(
                SetupStage::Persistence,
                SetupOutcome::Succeeded,
                SetupFailureClass::None,
            );
            Ok(())
        }
        .await;
        emit_setup_stage(
            SetupStage::LockRelease,
            SetupOutcome::Started,
            SetupFailureClass::None,
        );
        let unlock_result = setup_lock.release().await;
        emit_setup_stage(
            SetupStage::LockRelease,
            if unlock_result.is_ok() {
                SetupOutcome::Succeeded
            } else {
                SetupOutcome::Failed
            },
            if unlock_result.is_ok() {
                SetupFailureClass::None
            } else {
                SetupFailureClass::Storage
            },
        );
        match (commit_result, unlock_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(_)) => Err(ClaimError::InvalidEnvelope),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

/// Validates the exact 78-byte BIP32 account xpub bytes and returns bitcoin's
/// canonical Base58 rendering. Mainnet uses xpub version bytes; testnet,
/// signet, and regtest use tpub version bytes.
pub fn validate_xpub(
    serialized_xpub: &[u8; 78],
    account_index: u32,
    configured_network: &BitcoinNetwork,
) -> Result<String, ClaimError> {
    let xpub = Xpub::decode(serialized_xpub).map_err(|_| ClaimError::InvalidPayload)?;
    let canonical = xpub.to_string();
    derive_bip84_p2wpkh_address(&canonical, account_index, configured_network, 0)
        .map_err(|_| ClaimError::InvalidPayload)?;
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    #[test]
    fn setup_marker_disables_unsupported_receipts_and_outgoing_payments() {
        let capabilities = super::default_marker_capabilities();

        assert!(capabilities.private_payments);
        assert!(capabilities.payment_requests);
        assert!(!capabilities.receipts);
        assert!(!capabilities.outgoing_payments);
    }
}
