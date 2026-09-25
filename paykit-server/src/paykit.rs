//! Concrete per-Creator Paykit SDK boundary used by durable outbox workers.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use async_trait::async_trait;
use paykit_lib::{
    PaykitReceiverPath, PaymentAmount, PaymentEndpointIdentifier, PaymentReference,
    PaymentRequestId, PaymentRequestTerms,
};
use paykit_sdk::{
    LinkedPeerState, OutboundPrivateMessageStatus, PaykitSdk, PaykitSdkConfig, PaykitSdkError,
    PaymentAdapter, PaymentRequestLifecycleState as SdkPaymentRequestLifecycleState,
    PaymentRequestLocalRole, PaymentRequestRecord, PrivateReceivingDetail, PubkyPublicKey,
    PubkySessionAccess, PubkySessionBootstrap, PubkySessionProvider, StorageAdapter,
};
use pubky::Pubky;
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;
use uuid::Uuid;

use crate::{
    application::{
        payment_drain::{PaymentDrainError, PaymentDrainResult},
        payment_request_status::{
            PaymentRequestStatusError, PaymentRequestStatusOperations, PaymentRequestStatusSummary,
        },
        semantic_intent::{DeliveryIntentV1, PaymentTermsV1, ReceivingDetailV1},
    },
    config::PaykitConfig,
    domain::{
        locks::{BundleId, CreatorPubky, PubkyLockResource},
        payment_request_lifecycle::{
            PaymentRequestLifecycleProjection,
            PaymentRequestLifecycleState as PersistedPaymentRequestLifecycleState,
            ProposalCorrelation,
        },
    },
    persistence::{
        ClaimedOutbox, CreatorStore, OutboxStore, PaymentDrainStore, PaymentRequestLifecycleStore,
        PersistenceError, PostgresStorageAdapter,
    },
    workers::outbox::{
        Adapter, HandoffError, HandoffFailure, HandoffResult, RetryableHandoffCause,
        RetryableHandoffStage, handoff_steps,
    },
};

/// Creator-owned live Pubky access restored from encrypted server credentials.
#[derive(Clone, Debug)]
pub struct CreatorSessionProvider {
    creators: CreatorStore,
    creator: CreatorPubky,
    public_client: Pubky,
    client_id: String,
    required_capabilities: String,
}

impl CreatorSessionProvider {
    pub fn new(
        creators: CreatorStore,
        creator: CreatorPubky,
        config: &PaykitConfig,
    ) -> Result<Self, PaykitSdkError> {
        let public_client = Pubky::new().map_err(|error| PaykitSdkError::Identity {
            context: "could not construct Pubky client".into(),
            source: Some(anyhow::anyhow!(error.to_string())),
        })?;
        Ok(Self::with_pubky(creators, creator, public_client, config))
    }

    /// Uses the process-selected Pubky network for this Creator's restored session.
    pub fn with_pubky(
        creators: CreatorStore,
        creator: CreatorPubky,
        public_client: Pubky,
        config: &PaykitConfig,
    ) -> Self {
        Self {
            creators,
            creator,
            public_client,
            client_id: config.client_id.to_string(),
            required_capabilities: PaykitSdkConfig::new(config.receiver_path.clone())
                .required_session_capabilities(),
        }
    }
}

#[async_trait]
impl PubkySessionProvider for CreatorSessionProvider {
    async fn load_session_access(&self) -> paykit_sdk::Result<Option<PubkySessionAccess>> {
        let credentials = self
            .creators
            .load(&self.creator)
            .await
            .map_err(|error| match error {
                crate::persistence::PersistenceError::CorruptOrMissing => {
                    PaykitSdkError::Identity {
                        context: "creator credentials are missing or invalid".into(),
                        source: None,
                    }
                }
                _ => PaykitSdkError::Storage {
                    context: "creator credentials are unavailable".into(),
                    source: None,
                },
            })?;
        let bootstrap =
            PubkySessionBootstrap::with_pubky(self.public_client.clone(), &self.client_id)?;
        let access = bootstrap
            .import_session(
                credentials.session_secret(),
                None,
                credentials.receiver_noise_secret().clone(),
                &self.required_capabilities,
            )
            .await?
            .access;
        bind_session_to_creator(access.public_key()?, &self.creator)?;
        access.validate()?;
        Ok(Some(access))
    }

    async fn load_public_storage(&self) -> paykit_sdk::Result<Option<pubky::PublicStorage>> {
        Ok(Some(self.public_client.public_storage()))
    }

    async fn clear_session_access(&self) -> paykit_sdk::Result<()> {
        Err(PaykitSdkError::Policy {
            context: "server-managed Creator sessions must be replaced through reauthentication"
                .into(),
            source: None,
        })
    }
}

fn bind_session_to_creator(
    actual: PubkyPublicKey,
    expected_creator: &CreatorPubky,
) -> paykit_sdk::Result<()> {
    let expected = PubkyPublicKey::from_raw_or_app_key(expected_creator.to_string())?;
    if actual != expected {
        return Err(PaykitSdkError::Identity {
            context: "restored Pubky session does not match Creator".into(),
            source: None,
        });
    }
    Ok(())
}

/// Minimal adapter required to construct the SDK for explicit server-owned handoff inputs.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExplicitInputsPaymentAdapter;

impl PaymentAdapter for ExplicitInputsPaymentAdapter {}

type CreatorSdk =
    PaykitSdk<PostgresStorageAdapter, CreatorSessionProvider, ExplicitInputsPaymentAdapter>;

/// Public-SDK-only handoff implementation for one Creator.
pub struct PaykitAdapter {
    sdk: CreatorSdk,
    storage: PostgresStorageAdapter,
    creator: CreatorPubky,
    mutation_lock: Arc<TokioMutex<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleSyncError {
    Sdk,
    Persistence,
    Conflict,
    InvalidProjection,
    PartialReceive,
}

impl std::fmt::Debug for PaykitAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PaykitAdapter { .. }")
    }
}

impl PaykitAdapter {
    pub fn new(
        storage: PostgresStorageAdapter,
        sessions: CreatorSessionProvider,
        config: &PaykitConfig,
    ) -> Result<Self, PaykitSdkError> {
        let creator = sessions.creator.clone();
        let sdk = PaykitSdk::new(
            storage.clone(),
            sessions,
            ExplicitInputsPaymentAdapter,
            PaykitSdkConfig::new(config.receiver_path.clone()),
        )?;
        Ok(Self {
            sdk,
            mutation_lock: creator_mutation_lock(storage.creator_id()),
            storage,
            creator,
        })
    }

    /// Receives linked-peer messages and durably projects the SDK's canonical
    /// lifecycle view while serializing Creator-local SDK mutations.
    pub async fn receive_and_project_payment_requests(
        &self,
        lifecycles: &PaymentRequestLifecycleStore,
    ) -> Result<(), LifecycleSyncError> {
        let _guard = self.mutation_lock.lock().await;
        self.refresh_payment_requests_locked(lifecycles).await
    }

    async fn refresh_payment_requests_locked(
        &self,
        lifecycles: &PaymentRequestLifecycleStore,
    ) -> Result<(), LifecycleSyncError> {
        let receive_health = match self.sdk.receive_private_messages_from_linked_peers().await {
            Ok(reports) if reports.iter().all(|report| report.error.is_none()) => {
                ReceiveHealth::Available
            }
            Ok(_) | Err(_) => ReceiveHealth::Unavailable,
        };
        let records = self
            .sdk
            .payment_requests()
            .await
            .map_err(|_| LifecycleSyncError::Sdk)?;
        let projections = records.iter().map(lifecycle_projection).collect();
        let creator_id = self.storage.creator_id();
        project_lifecycles_after_receive(projections, receive_health, |projection| async move {
            lifecycles
                .apply(creator_id, &projection)
                .await
                .map_err(map_projection_persistence_error)?;
            Ok(())
        })
        .await
    }

    /// Receives and projects fresh canonical state before returning status, all
    /// under the same Creator-local mutation fence.
    pub async fn reconcile_and_lookup_payment_request_status(
        &self,
        lifecycles: &PaymentRequestLifecycleStore,
        statuses: &dyn PaymentRequestStatusOperations,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentRequestStatusSummary>, PaymentRequestStatusError> {
        let _guard = self.mutation_lock.lock().await;
        refresh_then(
            || async {
                self.refresh_payment_requests_locked(lifecycles)
                    .await
                    .map_err(map_lifecycle_status_error)
            },
            || async { statuses.lookup(&self.creator, bundle_id).await },
        )
        .await
    }

    /// Reconciles receive and the canonical SDK reducer before atomically
    /// snapshotting one lock's drain under the same Creator-local mutation lock.
    pub async fn reconcile_and_create_payment_drain(
        &self,
        lifecycles: &PaymentRequestLifecycleStore,
        drains: &PaymentDrainStore,
        lock_resource: &PubkyLockResource,
    ) -> Result<PaymentDrainResult, PaymentDrainError> {
        if lock_resource.creator() != &self.creator {
            return Err(PaymentDrainError::CreatorMismatch);
        }
        let _guard = self.mutation_lock.lock().await;
        replay_or_refresh_then(
            || async {
                drains
                    .exact_replay(lock_resource)
                    .await
                    .map_err(map_drain_persistence_error)
            },
            || async {
                self.refresh_payment_requests_locked(lifecycles)
                    .await
                    .map_err(map_lifecycle_drain_error)
            },
            || async {
                drains
                    .create(lock_resource)
                    .await
                    .map_err(map_drain_persistence_error)
            },
        )
        .await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiveHealth {
    Available,
    Unavailable,
}

async fn project_lifecycles_after_receive<F, Fut>(
    projections: Vec<Result<PaymentRequestLifecycleProjection, LifecycleSyncError>>,
    receive_health: ReceiveHealth,
    mut persist: F,
) -> Result<(), LifecycleSyncError>
where
    F: FnMut(PaymentRequestLifecycleProjection) -> Fut,
    Fut: Future<Output = Result<(), LifecycleSyncError>>,
{
    let mut first_error = None;
    for projection in projections {
        match projection {
            Ok(projection) => {
                if let Err(error) = persist(projection).await {
                    first_error.get_or_insert(error);
                }
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    if receive_health == ReceiveHealth::Unavailable {
        return Err(LifecycleSyncError::PartialReceive);
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn replay_or_refresh_then<
    T,
    E,
    Replay,
    ReplayFuture,
    Refresh,
    RefreshFuture,
    Operation,
    OperationFuture,
>(
    replay: Replay,
    refresh: Refresh,
    operation: Operation,
) -> Result<T, E>
where
    Replay: FnOnce() -> ReplayFuture,
    ReplayFuture: Future<Output = Result<Option<T>, E>>,
    Refresh: FnOnce() -> RefreshFuture,
    RefreshFuture: Future<Output = Result<(), E>>,
    Operation: FnOnce() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, E>>,
{
    if let Some(replay) = replay().await? {
        return Ok(replay);
    }
    refresh_then(refresh, operation).await
}

async fn refresh_then<T, E, Refresh, RefreshFuture, Operation, OperationFuture>(
    refresh: Refresh,
    operation: Operation,
) -> Result<T, E>
where
    Refresh: FnOnce() -> RefreshFuture,
    RefreshFuture: Future<Output = Result<(), E>>,
    Operation: FnOnce() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, E>>,
{
    refresh().await?;
    operation().await
}

fn map_projection_persistence_error(error: PersistenceError) -> LifecycleSyncError {
    match error {
        PersistenceError::Conflict => LifecycleSyncError::Conflict,
        _ => LifecycleSyncError::Persistence,
    }
}

fn map_lifecycle_drain_error(error: LifecycleSyncError) -> PaymentDrainError {
    match error {
        LifecycleSyncError::Conflict => PaymentDrainError::Conflict,
        LifecycleSyncError::Sdk
        | LifecycleSyncError::Persistence
        | LifecycleSyncError::InvalidProjection
        | LifecycleSyncError::PartialReceive => PaymentDrainError::Unavailable,
    }
}

fn map_lifecycle_status_error(error: LifecycleSyncError) -> PaymentRequestStatusError {
    match error {
        LifecycleSyncError::Conflict => PaymentRequestStatusError::Conflict,
        LifecycleSyncError::Sdk
        | LifecycleSyncError::Persistence
        | LifecycleSyncError::InvalidProjection
        | LifecycleSyncError::PartialReceive => PaymentRequestStatusError::Unavailable,
    }
}

fn map_drain_persistence_error(error: PersistenceError) -> PaymentDrainError {
    match error {
        PersistenceError::Conflict => PaymentDrainError::Conflict,
        _ => PaymentDrainError::Unavailable,
    }
}

fn recovery_state_event_id<'a>(
    canceled_event_id: Option<&'a str>,
    rejected_event_id: Option<&'a str>,
    latest_payment_proof_event_id: Option<&'a str>,
    accepted_event_id: Option<&'a str>,
    proposal_event_id: Option<&'a str>,
) -> Option<&'a str> {
    canceled_event_id
        .or(rejected_event_id)
        .or(latest_payment_proof_event_id)
        .or(accepted_event_id)
        .or(proposal_event_id)
}

fn lifecycle_projection(
    record: &PaymentRequestRecord,
) -> Result<PaymentRequestLifecycleProjection, LifecycleSyncError> {
    if record.local_role != Some(PaymentRequestLocalRole::Payee) {
        return Err(LifecycleSyncError::InvalidProjection);
    }
    let terms = record
        .terms
        .as_ref()
        .filter(|terms| terms.recurrence.is_none())
        .ok_or(LifecycleSyncError::InvalidProjection)?;
    let request_state = persisted_lifecycle_state(record.state)?;
    let state_event_id = match record.state {
        SdkPaymentRequestLifecycleState::Proposed
        | SdkPaymentRequestLifecycleState::ProposalExpired => record.proposal_event_id.clone(),
        SdkPaymentRequestLifecycleState::Accepted
        | SdkPaymentRequestLifecycleState::ActiveRecurring => record.accepted_event_id.clone(),
        SdkPaymentRequestLifecycleState::Rejected => record.rejected_event_id.clone(),
        SdkPaymentRequestLifecycleState::Canceled => record.canceled_event_id.clone(),
        SdkPaymentRequestLifecycleState::ProofSubmitted => record
            .payment_proofs
            .last()
            .map(|proof| proof.event_id.clone()),
        SdkPaymentRequestLifecycleState::RecoveryRequired => recovery_state_event_id(
            record.canceled_event_id.as_deref(),
            record.rejected_event_id.as_deref(),
            record
                .payment_proofs
                .last()
                .map(|proof| proof.event_id.as_str()),
            record.accepted_event_id.as_deref(),
            record.proposal_event_id.as_deref(),
        )
        .map(str::to_owned),
        SdkPaymentRequestLifecycleState::InvalidConflict => record
            .canceled_event_id
            .clone()
            .or_else(|| record.rejected_event_id.clone())
            .or_else(|| record.accepted_event_id.clone())
            .or_else(|| record.proposal_event_id.clone()),
        _ => return Err(LifecycleSyncError::InvalidProjection),
    };
    let last_event_at = record
        .last_event_at
        .ok_or(LifecycleSyncError::InvalidProjection)?;
    let seconds = i128::from(last_event_at.timestamp());
    let nanos = i128::from(last_event_at.timestamp_subsec_nanos());
    let timestamp_nanos = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos))
        .ok_or(LifecycleSyncError::InvalidProjection)?;
    let last_event_at = time::OffsetDateTime::from_unix_timestamp_nanos(timestamp_nanos)
        .map_err(|_| LifecycleSyncError::InvalidProjection)?;
    Ok(PaymentRequestLifecycleProjection {
        payment_request_id: record.payment_request_id.clone(),
        proposal: ProposalCorrelation {
            reader_pubky: format!("pubky{}", record.counterparty),
            selected_reader_path: record.counterparty_receiver_path.to_string(),
            terms: PaymentTermsV1 {
                amount: terms.amount.value.clone(),
                asset: terms.amount.asset.clone(),
                payment_reference: terms.payment_reference.clone(),
                proposal_expires_at: terms.proposal_expires_at.clone(),
                accepted_endpoint_identifiers: terms.accepted_payment_endpoint_identifiers.clone(),
                metadata: terms.metadata.clone(),
            },
        },
        request_state,
        state_event_id,
        last_stream_item_id: record.last_stream_item_id,
        last_outbound_message_id: record.last_outbound_message_id,
        last_event_at,
    })
}

fn persisted_lifecycle_state(
    state: SdkPaymentRequestLifecycleState,
) -> Result<PersistedPaymentRequestLifecycleState, LifecycleSyncError> {
    match state {
        SdkPaymentRequestLifecycleState::Proposed => {
            Ok(PersistedPaymentRequestLifecycleState::Proposed)
        }
        SdkPaymentRequestLifecycleState::ProposalExpired => {
            Ok(PersistedPaymentRequestLifecycleState::ProposalExpired)
        }
        SdkPaymentRequestLifecycleState::Accepted => {
            Ok(PersistedPaymentRequestLifecycleState::Accepted)
        }
        SdkPaymentRequestLifecycleState::Rejected => {
            Ok(PersistedPaymentRequestLifecycleState::Rejected)
        }
        SdkPaymentRequestLifecycleState::Canceled => {
            Ok(PersistedPaymentRequestLifecycleState::Canceled)
        }
        SdkPaymentRequestLifecycleState::ProofSubmitted => {
            Ok(PersistedPaymentRequestLifecycleState::ProofSubmitted)
        }
        SdkPaymentRequestLifecycleState::ActiveRecurring => {
            Ok(PersistedPaymentRequestLifecycleState::ActiveRecurring)
        }
        SdkPaymentRequestLifecycleState::RecoveryRequired => {
            Ok(PersistedPaymentRequestLifecycleState::RecoveryRequired)
        }
        SdkPaymentRequestLifecycleState::InvalidConflict => {
            Ok(PersistedPaymentRequestLifecycleState::InvalidConflict)
        }
        _ => Err(LifecycleSyncError::InvalidProjection),
    }
}

type CreatorMutationLock = TokioMutex<()>;

fn creator_mutation_lock(creator_id: Uuid) -> Arc<CreatorMutationLock> {
    static LOCKS: OnceLock<StdMutex<HashMap<Uuid, Weak<CreatorMutationLock>>>> = OnceLock::new();
    let registry = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    registry.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = registry.get(&creator_id).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(TokioMutex::new(()));
    registry.insert(creator_id, Arc::downgrade(&lock));
    lock
}

fn parse_peer(
    reader: &str,
    path: &str,
) -> Result<(PubkyPublicKey, PaykitReceiverPath), HandoffError> {
    let reader =
        PubkyPublicKey::from_raw_or_app_key(reader).map_err(|_| HandoffError::Permanent)?;
    let path = PaykitReceiverPath::new(path.to_owned()).map_err(|_| HandoffError::Permanent)?;
    Ok((reader, path))
}

fn classify(error: PaykitSdkError) -> HandoffError {
    match error {
        PaykitSdkError::Protocol { .. } => HandoffError::Permanent,
        PaykitSdkError::Policy { .. } => HandoffError::Retryable(RetryableHandoffCause::Policy),
        PaykitSdkError::Storage { .. } => HandoffError::Retryable(RetryableHandoffCause::Storage),
        PaykitSdkError::Identity { .. } => HandoffError::Retryable(RetryableHandoffCause::Identity),
        PaykitSdkError::Transport { .. } => {
            HandoffError::Retryable(RetryableHandoffCause::Transport)
        }
        PaykitSdkError::NotFound { .. } => HandoffError::Retryable(RetryableHandoffCause::NotFound),
        PaykitSdkError::PaymentAdapter { .. } => {
            HandoffError::Retryable(RetryableHandoffCause::PaymentAdapter)
        }
        PaykitSdkError::RecoveryRequired { .. } => {
            HandoffError::Retryable(RetryableHandoffCause::RecoveryRequired)
        }
        _ => HandoffError::Retryable(RetryableHandoffCause::Other),
    }
}

fn payment_terms(terms: &PaymentTermsV1) -> Result<PaymentRequestTerms, HandoffError> {
    let amount = PaymentAmount::new(terms.amount.clone(), terms.asset.clone())
        .map_err(|_| HandoffError::Permanent)?;
    let payment_reference = PaymentReference::new(terms.payment_reference.clone())
        .map_err(|_| HandoffError::Permanent)?;
    let accepted_payment_endpoint_identifiers = terms
        .accepted_endpoint_identifiers
        .iter()
        .cloned()
        .map(PaymentEndpointIdentifier::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HandoffError::Permanent)?;
    Ok(PaymentRequestTerms {
        amount,
        payment_reference,
        proposal_expires_at: terms.proposal_expires_at.clone(),
        recurrence: None,
        accepted_payment_endpoint_identifiers,
        metadata: terms.metadata.clone(),
    })
}

#[async_trait]
impl Adapter for PaykitAdapter {
    async fn execute_claimed_handoff(
        &self,
        store: &OutboxStore,
        claim: &ClaimedOutbox,
        intent: &DeliveryIntentV1,
    ) -> Result<HandoffResult, HandoffFailure> {
        let _guard = self.mutation_lock.lock().await;
        match store.claim_handoff_eligible(claim).await {
            Ok(true) => handoff_steps(self, intent).await,
            Ok(false) => Err(HandoffFailure::Permanent),
            Err(_) => Err(HandoffFailure::Retryable(
                RetryableHandoffStage::AdapterUnavailable,
            )),
        }
    }

    async fn execute_handoff(
        &self,
        intent: &DeliveryIntentV1,
    ) -> Result<HandoffResult, HandoffFailure> {
        let _guard = self.mutation_lock.lock().await;
        handoff_steps(self, intent).await
    }

    async fn fetch_marker(
        &self,
        reader: &str,
        path: &str,
    ) -> Result<Option<paykit_lib::PaykitReceiverMarker>, HandoffError> {
        let (reader, path) = parse_peer(reader, path)?;
        self.sdk
            .paykit_receiver_marker(reader, path)
            .await
            .map_err(classify)
    }

    async fn ensure_link_with_peer(&self, reader: &str, path: &str) -> Result<(), HandoffError> {
        let (reader, path) = parse_peer(reader, path)?;
        let result = self
            .sdk
            .ensure_link_with_peer(reader, path, 1)
            .await
            .map_err(classify)
            .and_then(|report| require_linked(report.state));
        if let Err(error) = result {
            warn!(
                stage = "link_establishment",
                cause = error.diagnostic_label(),
                "Paykit handoff failed"
            );
        }
        result
    }

    async fn enqueue_private_payment_list_with_receiving_details(
        &self,
        reader: &str,
        path: &str,
        details: &[ReceivingDetailV1],
    ) -> Result<HandoffResult, HandoffError> {
        let (reader, path) = parse_peer(reader, path)?;
        let details = details
            .iter()
            .map(|detail| PrivateReceivingDetail {
                identifier: detail.identifier.clone(),
                payload: detail.payload.clone(),
            })
            .collect();
        let record = self
            .sdk
            .enqueue_private_payment_list_with_receiving_details(reader, path, details)
            .await
            .map_err(classify)?;
        Ok(HandoffResult::EndpointPublication {
            outbound_message_id: record.outbound_message_id,
        })
    }

    async fn propose_payment_request(
        &self,
        reader: &str,
        path: &str,
        terms: &PaymentTermsV1,
    ) -> Result<HandoffResult, HandoffError> {
        let (reader, path) = parse_peer(reader, path)?;
        let record = self
            .sdk
            .propose_payment_request(reader, path, payment_terms(terms)?)
            .await
            .map_err(classify)?;
        Ok(HandoffResult::PaymentRequestProposal {
            outbound_message_id: record
                .proposal_outbound_message_id
                .ok_or(HandoffError::Permanent)?,
            event_id: record.proposal_event_id.ok_or(HandoffError::Permanent)?,
            payment_request_id: record.payment_request_id,
        })
    }

    async fn cancel_payment_request(
        &self,
        reader: &str,
        path: &str,
        payment_request_id: &str,
    ) -> Result<HandoffResult, HandoffError> {
        let (reader, path) = parse_peer(reader, path)?;
        let payment_request_id = PaymentRequestId::new(payment_request_id.to_owned())
            .map_err(|_| HandoffError::Permanent)?;
        let record = self
            .sdk
            .cancel_payment_request(reader, path, &payment_request_id, None)
            .await
            .map_err(classify)?;
        Ok(HandoffResult::PaymentRequestCancellation {
            outbound_message_id: record
                .last_outbound_message_id
                .ok_or(HandoffError::Permanent)?,
            event_id: record.canceled_event_id.ok_or(HandoffError::Permanent)?,
            payment_request_id: record.payment_request_id,
        })
    }

    async fn outbound_status(
        &self,
        outbound_message_id: u64,
    ) -> Result<Option<OutboundPrivateMessageStatus>, HandoffError> {
        let _guard = self.mutation_lock.lock().await;
        let outbound = self
            .storage
            .transaction(move |transaction| {
                Ok(transaction
                    .export_storage_state()
                    .outbound_private_messages
                    .into_iter()
                    .find(|record| record.outbound_message_id == outbound_message_id)
                    .map(|record| {
                        (
                            record.status,
                            record.counterparty,
                            record.counterparty_receiver_path,
                        )
                    }))
            })
            .await
            .map_err(classify)?;
        let Some((status, counterparty, counterparty_receiver_path)) = outbound else {
            return Ok(None);
        };
        if terminal_outbound_status(&status) {
            return Ok(Some(status));
        }
        let report = self
            .sdk
            .ensure_link_with_peer(counterparty.clone(), counterparty_receiver_path.clone(), 1)
            .await
            .map_err(classify)?;
        require_linked(report.state)?;
        self.sdk
            .process_outbound_private_messages(counterparty, counterparty_receiver_path)
            .await
            .map_err(classify)?;
        self.storage
            .transaction(move |transaction| {
                Ok(transaction
                    .export_storage_state()
                    .outbound_private_messages
                    .into_iter()
                    .find(|record| record.outbound_message_id == outbound_message_id)
                    .map(|record| record.status))
            })
            .await
            .map_err(classify)
    }
}

fn terminal_outbound_status(status: &OutboundPrivateMessageStatus) -> bool {
    matches!(
        status,
        OutboundPrivateMessageStatus::Sent
            | OutboundPrivateMessageStatus::Invalid
            | OutboundPrivateMessageStatus::RecoveryRequired
            | OutboundPrivateMessageStatus::Superseded
    )
}

fn require_linked(state: LinkedPeerState) -> Result<(), HandoffError> {
    match state {
        LinkedPeerState::Linked => Ok(()),
        LinkedPeerState::RecoveryRequired => Err(HandoffError::Retryable(
            RetryableHandoffCause::RecoveryRequired,
        )),
        _ => Err(HandoffError::Retryable(RetryableHandoffCause::LinkPending)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

    #[test]
    fn mutation_locks_are_shared_per_creator_and_isolated_between_creators() {
        let creator = Uuid::new_v4();
        let same_creator_first = creator_mutation_lock(creator);
        let same_creator_second = creator_mutation_lock(creator);
        let other_creator = creator_mutation_lock(Uuid::new_v4());

        assert!(Arc::ptr_eq(&same_creator_first, &same_creator_second));
        assert!(!Arc::ptr_eq(&same_creator_first, &other_creator));
    }

    #[test]
    fn incomplete_link_state_is_not_handoff_ready() {
        assert_eq!(require_linked(LinkedPeerState::Linked), Ok(()));
        assert_eq!(
            require_linked(LinkedPeerState::Linking),
            Err(HandoffError::Retryable(RetryableHandoffCause::LinkPending))
        );
        assert_eq!(
            require_linked(LinkedPeerState::RecoveryRequired),
            Err(HandoffError::Retryable(
                RetryableHandoffCause::RecoveryRequired
            ))
        );
    }

    #[test]
    fn handoff_diagnostic_causes_use_closed_secret_free_labels() {
        assert_eq!(RetryableHandoffCause::Storage.diagnostic_label(), "storage");
        assert_eq!(
            RetryableHandoffCause::Identity.diagnostic_label(),
            "identity"
        );
        assert_eq!(
            RetryableHandoffCause::Transport.diagnostic_label(),
            "transport"
        );
        assert_eq!(
            RetryableHandoffCause::NotFound.diagnostic_label(),
            "not_found"
        );
        assert_eq!(
            RetryableHandoffCause::PaymentAdapter.diagnostic_label(),
            "payment_adapter"
        );
        assert_eq!(
            RetryableHandoffCause::RecoveryRequired.diagnostic_label(),
            "recovery_required"
        );
        assert_eq!(RetryableHandoffCause::Policy.diagnostic_label(), "policy");
        assert_eq!(
            RetryableHandoffCause::LinkPending.diagnostic_label(),
            "link_pending"
        );
        assert_eq!(RetryableHandoffCause::Other.diagnostic_label(), "other");
    }

    #[test]
    fn sdk_policy_errors_remain_retryable_because_they_include_lease_contention() {
        let error = PaykitSdkError::Policy {
            context: "peer link operation already in progress".into(),
            source: None,
        };

        assert_eq!(
            classify(error),
            HandoffError::Retryable(RetryableHandoffCause::Policy)
        );
    }

    #[test]
    fn terminal_outbound_status_does_not_require_peer_processing() {
        for status in [
            OutboundPrivateMessageStatus::Sent,
            OutboundPrivateMessageStatus::Invalid,
            OutboundPrivateMessageStatus::RecoveryRequired,
            OutboundPrivateMessageStatus::Superseded,
        ] {
            assert!(terminal_outbound_status(&status));
        }
        for status in [
            OutboundPrivateMessageStatus::Pending,
            OutboundPrivateMessageStatus::Sending,
            OutboundPrivateMessageStatus::Failed,
        ] {
            assert!(!terminal_outbound_status(&status));
        }
    }

    #[test]
    fn restored_session_identity_must_match_selected_creator() {
        let expected = crate::domain::locks::parse_creator(CREATOR).unwrap();
        let expected_key = PubkyPublicKey::from_raw_or_app_key(CREATOR).unwrap();
        let actual = "ybndrfg8ejkmcpqxot1uwisza345h769"
            .chars()
            .find_map(|replacement| {
                let mut candidate = CREATOR.to_owned();
                candidate.replace_range(5..6, &replacement.to_string());
                PubkyPublicKey::from_raw_or_app_key(&candidate)
                    .ok()
                    .filter(|candidate| candidate != &expected_key)
            })
            .expect("valid second Pubky fixture");

        assert!(matches!(
            bind_session_to_creator(actual, &expected),
            Err(PaykitSdkError::Identity { .. })
        ));
    }

    #[test]
    fn every_known_sdk_lifecycle_state_maps_one_to_one() {
        let cases = [
            (
                SdkPaymentRequestLifecycleState::Proposed,
                PersistedPaymentRequestLifecycleState::Proposed,
            ),
            (
                SdkPaymentRequestLifecycleState::ProposalExpired,
                PersistedPaymentRequestLifecycleState::ProposalExpired,
            ),
            (
                SdkPaymentRequestLifecycleState::Accepted,
                PersistedPaymentRequestLifecycleState::Accepted,
            ),
            (
                SdkPaymentRequestLifecycleState::Rejected,
                PersistedPaymentRequestLifecycleState::Rejected,
            ),
            (
                SdkPaymentRequestLifecycleState::Canceled,
                PersistedPaymentRequestLifecycleState::Canceled,
            ),
            (
                SdkPaymentRequestLifecycleState::ProofSubmitted,
                PersistedPaymentRequestLifecycleState::ProofSubmitted,
            ),
            (
                SdkPaymentRequestLifecycleState::ActiveRecurring,
                PersistedPaymentRequestLifecycleState::ActiveRecurring,
            ),
            (
                SdkPaymentRequestLifecycleState::RecoveryRequired,
                PersistedPaymentRequestLifecycleState::RecoveryRequired,
            ),
            (
                SdkPaymentRequestLifecycleState::InvalidConflict,
                PersistedPaymentRequestLifecycleState::InvalidConflict,
            ),
        ];
        for (sdk, persisted) in cases {
            assert_eq!(persisted_lifecycle_state(sdk), Ok(persisted));
        }
    }

    #[test]
    fn recovery_projection_uses_the_latest_underlying_event_identity() {
        assert_eq!(
            recovery_state_event_id(
                Some("cancel"),
                Some("reject"),
                Some("proof"),
                Some("accept"),
                Some("proposal"),
            ),
            Some("cancel")
        );
        assert_eq!(
            recovery_state_event_id(
                None,
                Some("reject"),
                Some("proof"),
                Some("accept"),
                Some("proposal"),
            ),
            Some("reject")
        );
        assert_eq!(
            recovery_state_event_id(None, None, Some("proof"), Some("accept"), Some("proposal")),
            Some("proof")
        );
        assert_eq!(
            recovery_state_event_id(None, None, None, Some("accept"), Some("proposal")),
            Some("accept")
        );
        assert_eq!(
            recovery_state_event_id(None, None, None, None, Some("proposal")),
            Some("proposal")
        );
    }

    #[test]
    fn drain_refresh_error_mapping_preserves_conflicts_and_degrades_malformed_records() {
        assert_eq!(
            map_projection_persistence_error(PersistenceError::Conflict),
            LifecycleSyncError::Conflict
        );
        assert_eq!(
            map_lifecycle_drain_error(LifecycleSyncError::Conflict),
            PaymentDrainError::Conflict
        );
        assert_eq!(
            map_lifecycle_drain_error(LifecycleSyncError::InvalidProjection),
            PaymentDrainError::Unavailable
        );
        assert_eq!(
            map_lifecycle_status_error(LifecycleSyncError::Conflict),
            PaymentRequestStatusError::Conflict
        );
        assert_eq!(
            map_lifecycle_status_error(LifecycleSyncError::InvalidProjection),
            PaymentRequestStatusError::Unavailable
        );
    }

    fn projection(
        index: u64,
        request_state: PersistedPaymentRequestLifecycleState,
    ) -> PaymentRequestLifecycleProjection {
        PaymentRequestLifecycleProjection {
            payment_request_id: Uuid::new_v4().to_string(),
            proposal: ProposalCorrelation {
                reader_pubky: CREATOR.into(),
                selected_reader_path: "bitkit/wallet".into(),
                terms: PaymentTermsV1 {
                    amount: "1".into(),
                    asset: "btc".into(),
                    payment_reference: Uuid::new_v4().to_string(),
                    proposal_expires_at: None,
                    accepted_endpoint_identifiers: vec!["btc-bitcoin-p2wpkh".into()],
                    metadata: serde_json::Map::new(),
                },
            },
            request_state,
            state_event_id: Some(Uuid::new_v4().to_string()),
            last_stream_item_id: Some(index),
            last_outbound_message_id: None,
            last_event_at: time::OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap(),
        }
    }

    #[tokio::test]
    async fn top_level_receive_failure_projects_every_canonical_record_before_returning_degraded() {
        let projections = vec![
            Ok(projection(
                1,
                PersistedPaymentRequestLifecycleState::Proposed,
            )),
            Ok(projection(
                2,
                PersistedPaymentRequestLifecycleState::Accepted,
            )),
        ];
        let persisted = Arc::new(StdMutex::new(Vec::new()));
        let captured = persisted.clone();

        let result = project_lifecycles_after_receive(
            projections,
            ReceiveHealth::Unavailable,
            move |projection| {
                captured.lock().unwrap().push(projection.request_state);
                std::future::ready(Ok(()))
            },
        )
        .await;

        assert_eq!(result, Err(LifecycleSyncError::PartialReceive));
        assert_eq!(
            *persisted.lock().unwrap(),
            vec![
                PersistedPaymentRequestLifecycleState::Proposed,
                PersistedPaymentRequestLifecycleState::Accepted,
            ]
        );
    }

    #[tokio::test]
    async fn malformed_canonical_record_does_not_prevent_later_valid_projection() {
        let projections = vec![
            Ok(projection(
                1,
                PersistedPaymentRequestLifecycleState::Proposed,
            )),
            Err(LifecycleSyncError::InvalidProjection),
            Ok(projection(
                3,
                PersistedPaymentRequestLifecycleState::Accepted,
            )),
        ];
        let persisted = Arc::new(StdMutex::new(Vec::new()));
        let captured = persisted.clone();

        let result = project_lifecycles_after_receive(
            projections,
            ReceiveHealth::Available,
            move |projection| {
                captured.lock().unwrap().push(projection.request_state);
                std::future::ready(Ok(()))
            },
        )
        .await;

        assert_eq!(result, Err(LifecycleSyncError::InvalidProjection));
        assert_eq!(
            *persisted.lock().unwrap(),
            vec![
                PersistedPaymentRequestLifecycleState::Proposed,
                PersistedPaymentRequestLifecycleState::Accepted,
            ]
        );
    }

    #[tokio::test]
    async fn exact_replay_precedes_and_bypasses_fresh_mutable_work() {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let replay_calls = calls.clone();
        let refresh_calls = calls.clone();
        let operation_calls = calls.clone();

        let result = replay_or_refresh_then(
            move || {
                replay_calls.lock().unwrap().push("replay");
                std::future::ready(Ok::<_, PaymentDrainError>(Some(7_u8)))
            },
            move || {
                refresh_calls.lock().unwrap().push("refresh");
                std::future::ready(Ok::<_, PaymentDrainError>(()))
            },
            move || {
                operation_calls.lock().unwrap().push("operation");
                std::future::ready(Ok::<_, PaymentDrainError>(9_u8))
            },
        )
        .await;

        assert_eq!(result, Ok(7));
        assert_eq!(*calls.lock().unwrap(), vec!["replay"]);
    }

    #[tokio::test]
    async fn unavailable_refresh_prevents_stale_operation_result() {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let replay_calls = calls.clone();
        let refresh_calls = calls.clone();
        let operation_calls = calls.clone();

        let result = replay_or_refresh_then(
            move || {
                replay_calls.lock().unwrap().push("replay");
                std::future::ready(Ok::<_, PaymentDrainError>(None))
            },
            move || {
                refresh_calls.lock().unwrap().push("refresh");
                std::future::ready(Err(PaymentDrainError::Unavailable))
            },
            move || {
                operation_calls.lock().unwrap().push("operation");
                std::future::ready(Ok::<_, PaymentDrainError>(9_u8))
            },
        )
        .await;

        assert_eq!(result, Err(PaymentDrainError::Unavailable));
        assert_eq!(*calls.lock().unwrap(), vec!["replay", "refresh"]);
    }
}
