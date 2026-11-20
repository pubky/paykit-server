//! Semantic outbox handoff policy.
//!
//! A call can commit to the SDK queue and the process can crash before the
//! fenced database transition. The durable outbox UUID binds every retry to one
//! SDK Payment Request proposal with the original terms. Transport remains
//! **at-least-once**; the SDK owns its queue and encrypted-link retry state.

use async_trait::async_trait;
use paykit_lib::PaykitAppRegistry;
use paykit_sdk::OutboundPrivateMessageStatus;

use crate::{
    application::{
        create_invoice::ReaderAuthorization,
        semantic_intent::{DeliveryIntentV1, DeliveryOperationV1},
    },
    persistence::{ClaimedHandoff, ClaimedOutbox, OutboxStore, PersistenceError},
};
use std::time::Duration;

pub use crate::persistence::{HandoffResult, OutboxRetryClass as RetryableHandoffStage};

pub(crate) fn payment_correlation(
    claim: &ClaimedOutbox,
    intent: &DeliveryIntentV1,
) -> Result<(String, Option<String>), PersistenceError> {
    match intent.operation() {
        DeliveryOperationV1::PaymentRequestProposal { terms } => {
            let payment_request_id = paykit_lib::PaymentRequestId::new(claim.id().to_string())
                .map_err(|_| PersistenceError::CorruptOrMissing)?;
            let payment_reference =
                paykit_lib::PaymentReference::new(terms.payment_reference.clone())
                    .map_err(|_| PersistenceError::CorruptOrMissing)?;
            Ok((
                payment_request_id.to_string(),
                Some(payment_reference.to_string()),
            ))
        }
        DeliveryOperationV1::PaymentRequestCancellation { payment_request_id } => {
            let payment_request_id = paykit_lib::PaymentRequestId::new(payment_request_id.clone())
                .map_err(|_| PersistenceError::CorruptOrMissing)?;
            Ok((payment_request_id.to_string(), None))
        }
    }
}

pub(crate) fn emit_outbox_attempt_started(
    claim: &ClaimedOutbox,
    payment_request_id: &str,
    payment_reference: Option<&str>,
) {
    if let Some(payment_reference) = payment_reference {
        tracing::info!(
            event = "paykit_outbox_attempt_started",
            truth = "persisted",
            creator_id = %claim.creator_id().hyphenated(),
            outbox_job_id = %claim.id().hyphenated(),
            invoice_id = claim.invoice_id().map(|id| id.hyphenated().to_string()),
            payment_request_id,
            payment_reference,
            attempt = claim.attempt_count(),
            state = "leased",
            "Paykit outbox attempt started"
        );
    } else {
        tracing::info!(
            event = "paykit_outbox_attempt_started",
            truth = "persisted",
            creator_id = %claim.creator_id().hyphenated(),
            outbox_job_id = %claim.id().hyphenated(),
            invoice_id = claim.invoice_id().map(|id| id.hyphenated().to_string()),
            payment_request_id,
            attempt = claim.attempt_count(),
            state = "leased",
            "Paykit outbox attempt started"
        );
    }
}

fn emit_uncorrelated_outbox_attempt_started(claim: &ClaimedOutbox) {
    tracing::info!(
        event = "paykit_outbox_attempt_started",
        truth = "persisted",
        creator_id = %claim.creator_id().hyphenated(),
        outbox_job_id = %claim.id().hyphenated(),
        invoice_id = claim.invoice_id().map(|id| id.hyphenated().to_string()),
        attempt = claim.attempt_count(),
        state = "leased",
        correlation = "unavailable",
        "Paykit outbox attempt started"
    );
}

fn emit_corrupt_outbox_transition(claim: &ClaimedOutbox) {
    tracing::warn!(
        event = "paykit_outbox_transition",
        truth = "persisted",
        creator_id = %claim.creator_id().hyphenated(),
        outbox_job_id = %claim.id().hyphenated(),
        invoice_id = claim.invoice_id().map(|id| id.hyphenated().to_string()),
        attempt = claim.attempt_count(),
        previous_state = "leased",
        new_state = "permanently_failed",
        outcome = "corrupt_intent",
        category = "corrupt_or_missing",
        "Paykit outbox state transitioned"
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_outbox_transition(
    claim: &ClaimedOutbox,
    payment_request_id: &str,
    payment_reference: Option<&str>,
    new_state: &'static str,
    outcome: &'static str,
    retry: Option<(RetryableHandoffStage, Duration)>,
    outbound_message_id: Option<u64>,
) {
    let backoff_ms = retry.map(|(_, delay)| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    let retry_class = retry.map(|(stage, _)| stage.diagnostic_label());
    if let Some(payment_reference) = payment_reference {
        tracing::info!(
            event = "paykit_outbox_transition",
            truth = "persisted",
            creator_id = %claim.creator_id().hyphenated(),
            outbox_job_id = %claim.id().hyphenated(),
            invoice_id = claim.invoice_id().map(|id| id.hyphenated().to_string()),
            payment_request_id,
            payment_reference,
            attempt = claim.attempt_count(),
            previous_state = "leased",
            new_state,
            outcome,
            retry_class,
            backoff_ms,
            outbound_message_id,
            "Paykit outbox state transitioned"
        );
    } else {
        tracing::info!(
            event = "paykit_outbox_transition",
            truth = "persisted",
            creator_id = %claim.creator_id().hyphenated(),
            outbox_job_id = %claim.id().hyphenated(),
            invoice_id = claim.invoice_id().map(|id| id.hyphenated().to_string()),
            payment_request_id,
            attempt = claim.attempt_count(),
            previous_state = "leased",
            new_state,
            outcome,
            retry_class,
            backoff_ms,
            outbound_message_id,
            "Paykit outbox state transitioned"
        );
    }
}

pub(crate) fn emit_reconciliation_transition(
    claim: &ClaimedHandoff,
    outbound_message_id: u64,
    new_state: &'static str,
    outcome: &'static str,
    retry: Option<(RetryableHandoffStage, Duration)>,
) {
    let backoff_ms = retry.map(|(_, delay)| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    let retry_class = retry.map(|(stage, _)| stage.diagnostic_label());
    tracing::info!(
        event = "paykit_outbox_transition",
        truth = "persisted",
        creator_id = %claim.creator_id().hyphenated(),
        outbox_job_id = %claim.id().hyphenated(),
        outbound_message_id,
        attempt = claim.attempt_count(),
        previous_state = "handed_off",
        new_state,
        outcome,
        retry_class,
        backoff_ms,
        "Paykit outbox reconciliation transitioned"
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoffError {
    Retryable(RetryableHandoffCause),
    Permanent,
}

impl HandoffError {
    pub const fn diagnostic_label(self) -> &'static str {
        match self {
            Self::Retryable(cause) => cause.diagnostic_label(),
            Self::Permanent => "permanent",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryableHandoffCause {
    Storage,
    Identity,
    Transport,
    NotFound,
    PaymentAdapter,
    RecoveryRequired,
    LinkObservation,
    Policy,
    LinkPending,
    Other,
}

impl RetryableHandoffCause {
    pub const fn diagnostic_label(self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Identity => "identity",
            Self::Transport => "transport",
            Self::NotFound => "not_found",
            Self::PaymentAdapter => "payment_adapter",
            Self::RecoveryRequired => "recovery_required",
            Self::LinkObservation => "link_observation",
            Self::Policy => "policy",
            Self::LinkPending => "link_pending",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoffFailure {
    Retryable(RetryableHandoffStage),
    ProposalExpired,
    CancellationObsolete,
    Permanent,
}

#[derive(Clone, Copy)]
struct DeliveryExpiration(time::OffsetDateTime);

impl DeliveryExpiration {
    const fn deadline(self) -> time::OffsetDateTime {
        self.0
    }

    const fn failure(self) -> HandoffFailure {
        HandoffFailure::ProposalExpired
    }
}

fn at_stage(error: HandoffError, stage: RetryableHandoffStage) -> HandoffFailure {
    match error {
        HandoffError::Retryable(RetryableHandoffCause::LinkPending) => {
            HandoffFailure::Retryable(RetryableHandoffStage::LinkPending)
        }
        HandoffError::Retryable(_) => HandoffFailure::Retryable(stage),
        HandoffError::Permanent => HandoffFailure::Permanent,
    }
}

fn at_link_stage(error: HandoffError) -> HandoffFailure {
    match error {
        HandoffError::Retryable(RetryableHandoffCause::LinkPending) => {
            HandoffFailure::Retryable(RetryableHandoffStage::LinkPending)
        }
        HandoffError::Retryable(
            RetryableHandoffCause::Storage
            | RetryableHandoffCause::Identity
            | RetryableHandoffCause::Transport
            | RetryableHandoffCause::PaymentAdapter
            | RetryableHandoffCause::Other,
        ) => HandoffFailure::Retryable(RetryableHandoffStage::LinkDependency),
        HandoffError::Retryable(_) => {
            HandoffFailure::Retryable(RetryableHandoffStage::LinkEstablishment)
        }
        HandoffError::Permanent => HandoffFailure::Permanent,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessingHealth {
    Available,
    Retryable(Duration),
    PermanentFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetrySchedule {
    default: Duration,
    link_establishment: Duration,
}

impl RetrySchedule {
    pub fn new(default: Duration, link_establishment: Duration) -> Self {
        Self {
            default,
            link_establishment,
        }
    }

    pub fn default_delay(self) -> Duration {
        self.default
    }

    pub(crate) fn delay_for(self, stage: RetryableHandoffStage) -> Duration {
        if stage == RetryableHandoffStage::LinkPending {
            self.link_establishment
        } else {
            self.default
        }
    }
}

/// Public-SDK-only adapter. Production implementations must persist SDK state
/// through identity-wide Pubky shared storage; an in-memory runtime is test-only.
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Revalidates a claimed row immediately before its external SDK effect.
    /// Production overrides hold the same Creator mutation fence as drain creation.
    async fn execute_claimed_handoff(
        &self,
        store: &OutboxStore,
        claim: &ClaimedOutbox,
        intent: &DeliveryIntentV1,
    ) -> Result<HandoffResult, HandoffFailure> {
        claimed_handoff_steps(self, intent, store, claim).await
    }

    /// Executes one complete semantic handoff. Concrete adapters may override
    /// this to serialize a multi-call SDK operation under one Creator lock.
    /// `outbox_id` must be the persisted UUID-v4 row ID, unchanged on every retry.
    async fn execute_handoff(
        &self,
        outbox_id: uuid::Uuid,
        intent: &DeliveryIntentV1,
    ) -> Result<HandoffResult, HandoffFailure> {
        handoff_steps(self, outbox_id, intent).await
    }

    async fn fetch_registry(&self, reader: &str)
    -> Result<Option<PaykitAppRegistry>, HandoffError>;
    /// The Reader's signed Noise Key Authorization, checked before link setup,
    /// which verifies it again.
    async fn fetch_authorization(&self, reader: &str) -> Result<ReaderAuthorization, HandoffError>;
    async fn ensure_link_with_peer(&self, reader: &str) -> Result<(), HandoffError>;
    /// Creates or recovers the proposal bound to the durable outbox row ID.
    /// Implementations must reject conflicting input and retain the original SDK IDs.
    async fn propose_payment_request(
        &self,
        reader: &str,
        payment_request_id: paykit_lib::PaymentRequestId,
        terms: &crate::application::semantic_intent::PaymentTermsV1,
    ) -> Result<HandoffResult, HandoffError>;
    async fn cancel_payment_request(
        &self,
        reader: &str,
        payment_request_id: &str,
    ) -> Result<HandoffResult, HandoffError>;
    async fn outbound_status(
        &self,
        outbound_message_id: u64,
    ) -> Result<Option<OutboundPrivateMessageStatus>, HandoffError>;
}

/// Recheck reader capabilities before handing off the persisted intent.
/// The caller supplies the same durable UUID-v4 outbox row ID on every retry.
pub async fn handoff(
    adapter: &dyn Adapter,
    outbox_id: uuid::Uuid,
    intent: &DeliveryIntentV1,
) -> Result<HandoffResult, HandoffFailure> {
    adapter.execute_handoff(outbox_id, intent).await
}

pub(crate) async fn handoff_steps<A: Adapter + ?Sized>(
    adapter: &A,
    outbox_id: uuid::Uuid,
    intent: &DeliveryIntentV1,
) -> Result<HandoffResult, HandoffFailure> {
    handoff_steps_with_claim(adapter, outbox_id, intent, None).await
}

pub(crate) async fn claimed_handoff_steps<A: Adapter + ?Sized>(
    adapter: &A,
    intent: &DeliveryIntentV1,
    store: &OutboxStore,
    claim: &ClaimedOutbox,
) -> Result<HandoffResult, HandoffFailure> {
    handoff_steps_with_claim(adapter, claim.id(), intent, Some((store, claim))).await
}

async fn check_claim(
    claim: Option<(&OutboxStore, &ClaimedOutbox)>,
    expiration: Option<DeliveryExpiration>,
) -> Result<(), HandoffFailure> {
    if let Some((store, claim)) = claim {
        if let Some(expiration) = expiration {
            match store.proposal_expired(expiration.deadline()).await {
                Ok(true) => return Err(expiration.failure()),
                Ok(false) => {}
                Err(_) => {
                    return Err(HandoffFailure::Retryable(
                        RetryableHandoffStage::AdapterUnavailable,
                    ));
                }
            }
        }
        match store.claim_handoff_eligible(claim).await {
            Ok(true) => {}
            Ok(false) => return Err(HandoffFailure::Permanent),
            Err(_) => {
                return Err(HandoffFailure::Retryable(
                    RetryableHandoffStage::AdapterUnavailable,
                ));
            }
        }
    }
    Ok(())
}

async fn handoff_steps_with_claim<A: Adapter + ?Sized>(
    adapter: &A,
    outbox_id: uuid::Uuid,
    intent: &DeliveryIntentV1,
    claim: Option<(&OutboxStore, &ClaimedOutbox)>,
) -> Result<HandoffResult, HandoffFailure> {
    intent.validate().map_err(|_| HandoffFailure::Permanent)?;
    let expiration = match intent.operation() {
        DeliveryOperationV1::PaymentRequestProposal { .. } => intent
            .proposal_expires_at()
            .map_err(|_| HandoffFailure::Permanent)?
            .map(DeliveryExpiration),
        // Cancellation is durable cleanup. A payment deadline must not strand an
        // old-address request after account rotation or drain creation.
        DeliveryOperationV1::PaymentRequestCancellation { .. } => None,
    };
    let payment_request_id = paykit_lib::PaymentRequestId::new(outbox_id.to_string())
        .map_err(|_| HandoffFailure::Permanent)?;
    check_claim(claim, expiration).await?;
    let registry = adapter
        .fetch_registry(intent.reader_pubky())
        .await
        .map_err(|error| at_stage(error, RetryableHandoffStage::RegistryFetch))?
        .ok_or(HandoffFailure::Retryable(
            RetryableHandoffStage::RegistryMissing,
        ))?;
    if !crate::application::reader_registry::reader_is_capable(&registry) {
        return Err(HandoffFailure::Retryable(
            RetryableHandoffStage::RegistryIncapable,
        ));
    }
    check_claim(claim, expiration).await?;
    // Retryable like an incapable registry: the Reader can still publish or fix it.
    match adapter
        .fetch_authorization(intent.reader_pubky())
        .await
        .map_err(|error| at_stage(error, RetryableHandoffStage::ReaderAuthorizationFetch))?
    {
        ReaderAuthorization::Verified => {}
        ReaderAuthorization::Missing => {
            return Err(HandoffFailure::Retryable(
                RetryableHandoffStage::ReaderAuthorizationMissing,
            ));
        }
        ReaderAuthorization::Invalid => {
            return Err(HandoffFailure::Retryable(
                RetryableHandoffStage::ReaderAuthorizationInvalid,
            ));
        }
    }
    check_claim(claim, expiration).await?;
    adapter
        .ensure_link_with_peer(intent.reader_pubky())
        .await
        .map_err(at_link_stage)?;
    check_claim(claim, expiration).await?;
    match intent.operation() {
        DeliveryOperationV1::PaymentRequestProposal { terms } => adapter
            .propose_payment_request(intent.reader_pubky(), payment_request_id, terms)
            .await
            .map_err(|error| at_stage(error, RetryableHandoffStage::PaymentRequestProposal)),
        DeliveryOperationV1::PaymentRequestCancellation { payment_request_id } => adapter
            .cancel_payment_request(intent.reader_pubky(), payment_request_id)
            .await
            .map_err(|error| at_stage(error, RetryableHandoffStage::PaymentRequestCancellation)),
    }
}

/// Renews an admitted handoff without cancelling its in-flight SDK work on lease loss.
/// Subsequent SDK effects and the final database transition must revalidate the claim.
pub async fn with_claim_renewal<F: std::future::Future>(
    store: &OutboxStore,
    claim: &ClaimedOutbox,
    lease: Duration,
    operation: F,
) -> F::Output {
    let renewal = async {
        loop {
            tokio::time::sleep((lease / 3).max(Duration::from_millis(1))).await;
            if !store.renew_claim(claim, lease).await? {
                return Ok::<(), PersistenceError>(());
            }
        }
    };
    tokio::pin!(operation);
    tokio::select! {
        biased;
        result = &mut operation => result,
        result = renewal => {
            tracing::warn!(storage_error = result.is_err(), "outbox claim renewal stopped; draining admitted work");
            operation.await
        }
    }
}

/// Executes one already-fenced claim. Enqueue is only `handed_off`; the SDK
/// outbound record is reconciled separately before publication is acknowledged.
/// A proposal retry after enqueue but before this fenced transition uses the
/// same outbox UUID and terms, recovering the original proposal.
pub async fn process_claim(
    store: &OutboxStore,
    adapter: &dyn Adapter,
    claim: &ClaimedOutbox,
    retry_delay: Duration,
) -> Result<bool, PersistenceError> {
    process_claim_with_health(
        store,
        adapter,
        claim,
        RetrySchedule::new(retry_delay, retry_delay),
    )
    .await
    .map(|(transitioned, _)| transitioned)
}

pub async fn process_claim_with_health(
    store: &OutboxStore,
    adapter: &dyn Adapter,
    claim: &ClaimedOutbox,
    retry_schedule: RetrySchedule,
) -> Result<(bool, ProcessingHealth), PersistenceError> {
    process_claim_with(store, claim, retry_schedule, true, |intent| async move {
        adapter.execute_claimed_handoff(store, claim, &intent).await
    })
    .await
}

pub(crate) async fn process_claim_with<F>(
    store: &OutboxStore,
    claim: &ClaimedOutbox,
    retry_schedule: RetrySchedule,
    emit_start: bool,
    execute: impl FnOnce(DeliveryIntentV1) -> F,
) -> Result<(bool, ProcessingHealth), PersistenceError>
where
    F: std::future::Future<Output = Result<HandoffResult, HandoffFailure>>,
{
    let intent = match store.delivery_intent(claim) {
        Ok(intent) => intent,
        Err(_) => {
            emit_uncorrelated_outbox_attempt_started(claim);
            let transitioned = store.mark_permanently_failed(claim).await?;
            if transitioned {
                emit_corrupt_outbox_transition(claim);
            }
            return Ok((transitioned, ProcessingHealth::PermanentFailure));
        }
    };
    let expiration = match intent.operation() {
        DeliveryOperationV1::PaymentRequestProposal { .. } => intent
            .proposal_expires_at()
            .map_err(|_| PersistenceError::CorruptOrMissing)?
            .map(DeliveryExpiration),
        DeliveryOperationV1::PaymentRequestCancellation { .. } => None,
    };
    let (payment_request_id, payment_reference) = payment_correlation(claim, &intent)?;
    if emit_start {
        emit_outbox_attempt_started(claim, &payment_request_id, payment_reference.as_deref());
    }
    if let Some(expiration) = expiration
        && store.proposal_expired(expiration.deadline()).await?
    {
        let transitioned = store.mark_proposal_expired(claim).await?;
        if transitioned {
            emit_outbox_transition(
                claim,
                &payment_request_id,
                payment_reference.as_deref(),
                "permanently_failed",
                "proposal_expired",
                None,
                None,
            );
        }
        return Ok((transitioned, ProcessingHealth::Available));
    }
    match execute(intent).await {
        Ok(result) => {
            let transitioned = store.mark_handed_off(claim, &result).await?;
            if transitioned {
                emit_outbox_transition(
                    claim,
                    result
                        .payment_request_id()
                        .unwrap_or(payment_request_id.as_str()),
                    payment_reference.as_deref(),
                    "handed_off",
                    "handed_off",
                    None,
                    Some(result.outbound_message_id()),
                );
            }
            Ok((transitioned, ProcessingHealth::Available))
        }
        Err(HandoffFailure::Retryable(stage)) => {
            let delay = retry_schedule.delay_for(stage);
            let transitioned = store.mark_retryable(claim, delay, stage).await?;
            if transitioned {
                emit_outbox_transition(
                    claim,
                    &payment_request_id,
                    payment_reference.as_deref(),
                    "retryable",
                    "deferred",
                    Some((stage, delay)),
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::Retryable(delay)))
        }
        Err(HandoffFailure::ProposalExpired) => {
            let transitioned = store.mark_proposal_expired(claim).await?;
            if transitioned {
                emit_outbox_transition(
                    claim,
                    &payment_request_id,
                    payment_reference.as_deref(),
                    "permanently_failed",
                    "proposal_expired",
                    None,
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::Available))
        }
        Err(HandoffFailure::CancellationObsolete) => {
            let transitioned = store.mark_cancellation_obsolete(claim).await?;
            if transitioned {
                emit_outbox_transition(
                    claim,
                    &payment_request_id,
                    payment_reference.as_deref(),
                    "permanently_failed",
                    "cancellation_obsolete",
                    None,
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::Available))
        }
        Err(HandoffFailure::Permanent) => {
            let transitioned = store.mark_permanently_failed(claim).await?;
            if transitioned {
                emit_outbox_transition(
                    claim,
                    &payment_request_id,
                    payment_reference.as_deref(),
                    "permanently_failed",
                    "permanent_failure",
                    None,
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::PermanentFailure))
        }
    }
}

/// Reconciles one exact persisted SDK outbound event. Only durable `Sent`
/// acknowledges publication. Recoverable states remain `handed_off`; exact
/// `Invalid`, `RecoveryRequired`, or `Superseded` records become retained
/// permanent failures because the SDK will not claim those records again.
pub async fn process_reconciliation(
    store: &OutboxStore,
    adapter: &dyn Adapter,
    claim: &ClaimedHandoff,
    retry_delay: Duration,
) -> Result<bool, PersistenceError> {
    process_reconciliation_with_health(store, adapter, claim, retry_delay)
        .await
        .map(|(transitioned, _)| transitioned)
}

pub async fn process_reconciliation_with_health(
    store: &OutboxStore,
    adapter: &dyn Adapter,
    claim: &ClaimedHandoff,
    retry_delay: Duration,
) -> Result<(bool, ProcessingHealth), PersistenceError> {
    let expiration = match reconciliation_expiration(store, claim) {
        Ok(expiration) => expiration,
        Err(_) => {
            return store
                .mark_reconciliation_permanently_failed(claim)
                .await
                .map(|transitioned| (transitioned, ProcessingHealth::PermanentFailure));
        }
    };

    let outbound_message_id = match claim.sdk_outbound_message_id() {
        Ok(value) => value,
        Err(_) => {
            return store
                .mark_reconciliation_permanently_failed(claim)
                .await
                .map(|transitioned| (transitioned, ProcessingHealth::PermanentFailure));
        }
    };
    match adapter.outbound_status(outbound_message_id).await {
        Ok(Some(OutboundPrivateMessageStatus::Sent)) => {
            let transitioned = store.mark_delivered(claim).await?;
            if transitioned {
                emit_reconciliation_transition(
                    claim,
                    outbound_message_id,
                    "delivered",
                    "acknowledged",
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::Available))
        }
        Ok(Some(
            OutboundPrivateMessageStatus::Invalid
            | OutboundPrivateMessageStatus::RecoveryRequired
            | OutboundPrivateMessageStatus::Superseded,
        ))
        | Ok(None) => {
            let transitioned = store.mark_reconciliation_permanently_failed(claim).await?;
            if transitioned {
                emit_reconciliation_transition(
                    claim,
                    outbound_message_id,
                    "permanently_failed",
                    "permanent_failure",
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::PermanentFailure))
        }
        Ok(_) => {
            retry_reconciliation_after_failure(
                store,
                claim,
                retry_delay,
                expiration,
                RetryableHandoffStage::ReconciliationPending,
            )
            .await
        }
        Err(HandoffError::Retryable(_)) => {
            retry_reconciliation_after_failure(
                store,
                claim,
                retry_delay,
                expiration,
                RetryableHandoffStage::Reconciliation,
            )
            .await
        }
        Err(HandoffError::Permanent) => {
            let transitioned = store.mark_reconciliation_permanently_failed(claim).await?;
            if transitioned {
                emit_reconciliation_transition(
                    claim,
                    outbound_message_id,
                    "permanently_failed",
                    "permanent_failure",
                    None,
                );
            }
            Ok((transitioned, ProcessingHealth::PermanentFailure))
        }
    }
}

fn reconciliation_expiration(
    store: &OutboxStore,
    claim: &ClaimedHandoff,
) -> Result<Option<DeliveryExpiration>, PersistenceError> {
    Ok(match claim.intent_kind() {
        "endpoint_publication" => None,
        "payment_request_proposal" => {
            let intent = store.handed_off_delivery_intent(claim)?;
            if !matches!(
                intent.operation(),
                DeliveryOperationV1::PaymentRequestProposal { .. }
            ) {
                return Err(PersistenceError::CorruptOrMissing);
            }
            intent
                .proposal_expires_at()
                .map_err(|_| PersistenceError::CorruptOrMissing)?
                .map(DeliveryExpiration)
        }
        "payment_request_cancellation" => {
            let intent = store.handed_off_delivery_intent(claim)?;
            if !matches!(
                intent.operation(),
                DeliveryOperationV1::PaymentRequestCancellation { .. }
            ) {
                return Err(PersistenceError::CorruptOrMissing);
            }
            None
        }
        _ => return Err(PersistenceError::CorruptOrMissing),
    })
}

async fn retry_reconciliation_after_failure(
    store: &OutboxStore,
    claim: &ClaimedHandoff,
    retry_delay: Duration,
    expiration: Option<DeliveryExpiration>,
    mut stage: RetryableHandoffStage,
) -> Result<(bool, ProcessingHealth), PersistenceError> {
    if let Some(expiration) = expiration
        && store.proposal_expired(expiration.deadline()).await?
    {
        stage = RetryableHandoffStage::ReconciliationPending;
    }
    let transitioned = store
        .retry_reconciliation(claim, retry_delay, stage)
        .await?;
    if transitioned {
        emit_reconciliation_transition(
            claim,
            claim.sdk_outbound_message_id()?,
            "handed_off",
            "deferred",
            Some((stage, retry_delay)),
        );
    }
    Ok((transitioned, ProcessingHealth::Retryable(retry_delay)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::{EnvFilter, fmt::MakeWriter};
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct JsonWriter(Arc<Mutex<Vec<u8>>>);

    struct Guard(JsonWriter);

    impl Write for Guard {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for JsonWriter {
        type Writer = Guard;

        fn make_writer(&'a self) -> Self::Writer {
            Guard(self.clone())
        }
    }

    #[test]
    fn production_json_exposes_worker_correlation_without_secret_material() {
        let creator_id = Uuid::new_v4();
        let outbox_job_id = Uuid::new_v4();
        let invoice_id = Uuid::new_v4();
        let payment_request_id = outbox_job_id.to_string();
        let payment_reference = Uuid::new_v4().to_string();
        let output = JsonWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_env_filter(EnvFilter::new("off,paykit_server=info"))
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(output.clone())
            .finish();
        let claim =
            ClaimedOutbox::diagnostic_fixture(outbox_job_id, creator_id, Some(invoice_id), 3);
        let handed_off = ClaimedHandoff::diagnostic_fixture(outbox_job_id, creator_id, 4, 91);

        tracing::subscriber::with_default(subscriber, || {
            emit_outbox_attempt_started(&claim, &payment_request_id, Some(&payment_reference));
            emit_outbox_transition(
                &claim,
                &payment_request_id,
                Some(&payment_reference),
                "retryable",
                "deferred",
                Some((
                    RetryableHandoffStage::LinkPending,
                    Duration::from_millis(500),
                )),
                None,
            );
            emit_reconciliation_transition(&handed_off, 91, "delivered", "acknowledged", None);
        });

        let records = String::from_utf8(output.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0]["fields"]["event"],
            "paykit_outbox_attempt_started"
        );
        assert_eq!(records[0]["fields"]["state"], "leased");
        assert_eq!(records[0]["fields"]["creator_id"], creator_id.to_string());
        assert_eq!(
            records[1]["fields"]["outbox_job_id"],
            outbox_job_id.to_string()
        );
        assert_eq!(records[1]["fields"]["invoice_id"], invoice_id.to_string());
        assert_eq!(
            records[1]["fields"]["payment_request_id"],
            payment_request_id
        );
        assert_eq!(records[1]["fields"]["payment_reference"], payment_reference);
        assert_eq!(records[1]["fields"]["retry_class"], "link_pending");
        assert_eq!(records[1]["fields"]["backoff_ms"], 500);
        assert_eq!(records[2]["fields"]["previous_state"], "handed_off");
        assert_eq!(records[2]["fields"]["new_state"], "delivered");
        assert_eq!(records[2]["fields"]["outbound_message_id"], 91);
        let rendered = serde_json::to_string(&records).unwrap();
        for secret in [
            "https://provider.invalid/session=sentinel",
            "token-sentinel",
            "xpub-sentinel",
            "proof-sentinel",
        ] {
            assert!(!rendered.contains(secret));
        }
    }
}
