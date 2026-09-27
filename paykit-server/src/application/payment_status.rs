//! Legacy payment status projection backed by fresh canonical request status.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    application::payment_request_status::{
        PaymentRequestStatusError, PaymentRequestStatusOperations, PaymentRequestStatusSummary,
        PaymentState,
    },
    domain::{
        locks::{BundleId, CreatorPubky},
        payment_request_lifecycle::PaymentRequestLifecycleState,
    },
    persistence::{InvoiceStore, PersistenceError},
};

/// Validated factual Bitcoin state read from one persisted invoice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedPaymentStatus {
    Undetected,
    Detected {
        confirmations: u32,
        amount_matched: bool,
    },
    Confirmed {
        confirmations: u32,
        amount_matched: bool,
    },
}

#[async_trait]
pub trait StatusRepository: Send + Sync {
    async fn status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PersistedPaymentStatus>, PersistenceError>;
}

#[async_trait]
impl StatusRepository for InvoiceStore {
    async fn status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PersistedPaymentStatus>, PersistenceError> {
        InvoiceStore::payment_status(self, creator, bundle_id).await
    }
}

/// The exact, secret-free Locks-facing status response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaymentStatusResponse {
    status: &'static str,
    confirmations: u32,
    amount_matched: bool,
}

impl PaymentStatusResponse {
    fn undetected() -> Self {
        Self {
            status: "undetected",
            confirmations: 0,
            amount_matched: false,
        }
    }

    fn factual(status: &'static str, confirmations: u32, amount_matched: bool) -> Self {
        Self {
            status,
            confirmations,
            amount_matched,
        }
    }

    pub fn status(&self) -> &'static str {
        self.status
    }

    pub fn confirmations(&self) -> u32 {
        self.confirmations
    }

    pub fn amount_matched(&self) -> bool {
        self.amount_matched
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaymentStatusError {
    NotFound,
    Unavailable,
}

/// Projects the legacy label only after the canonical status operation has
/// refreshed linked-peer intake and revalidated required evidence.
pub struct PaymentStatusService {
    source: PaymentStatusSource,
}

enum PaymentStatusSource {
    Factual(Arc<dyn StatusRepository>),
    Canonical(Arc<dyn PaymentRequestStatusOperations>),
}

impl PaymentStatusService {
    /// Retains the factual foundation service for direct repository consumers.
    pub fn new(repository: Arc<dyn StatusRepository>) -> Self {
        Self {
            source: PaymentStatusSource::Factual(repository),
        }
    }

    /// Builds the composed HTTP service that must refresh lifecycle intake.
    pub fn from_canonical(operations: Arc<dyn PaymentRequestStatusOperations>) -> Self {
        Self {
            source: PaymentStatusSource::Canonical(operations),
        }
    }

    pub async fn status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaymentStatusResponse, PaymentStatusError> {
        match &self.source {
            PaymentStatusSource::Factual(repository) => {
                let persisted = repository
                    .status(creator, bundle_id)
                    .await
                    .map_err(|_| PaymentStatusError::Unavailable)?
                    .ok_or(PaymentStatusError::NotFound)?;
                Ok(match persisted {
                    PersistedPaymentStatus::Undetected => PaymentStatusResponse::undetected(),
                    PersistedPaymentStatus::Detected {
                        confirmations,
                        amount_matched,
                    } => PaymentStatusResponse::factual("detected", confirmations, amount_matched),
                    PersistedPaymentStatus::Confirmed {
                        confirmations,
                        amount_matched,
                    } => PaymentStatusResponse::factual("confirmed", confirmations, amount_matched),
                })
            }
            PaymentStatusSource::Canonical(operations) => {
                let canonical = operations
                    .lookup(creator, bundle_id)
                    .await
                    .map_err(|error| match error {
                        PaymentRequestStatusError::Conflict
                        | PaymentRequestStatusError::Unavailable => PaymentStatusError::Unavailable,
                    })?
                    .ok_or(PaymentStatusError::NotFound)?;
                project_legacy_status(canonical)
            }
        }
    }
}

fn project_legacy_status(
    canonical: PaymentRequestStatusSummary,
) -> Result<PaymentStatusResponse, PaymentStatusError> {
    let confirmations = canonical.confirmations();
    let amount_matched = canonical.amount_matched();
    match canonical.request_state() {
        PaymentRequestLifecycleState::Rejected | PaymentRequestLifecycleState::Canceled => Ok(
            PaymentStatusResponse::factual("cancelled", confirmations, amount_matched),
        ),
        PaymentRequestLifecycleState::ProposalExpired => Ok(PaymentStatusResponse::factual(
            "expired",
            confirmations,
            amount_matched,
        )),
        PaymentRequestLifecycleState::RecoveryRequired
        | PaymentRequestLifecycleState::InvalidConflict => Err(PaymentStatusError::Unavailable),
        PaymentRequestLifecycleState::Proposed
        | PaymentRequestLifecycleState::Accepted
        | PaymentRequestLifecycleState::ProofSubmitted
        | PaymentRequestLifecycleState::ActiveRecurring => match canonical.payment_state() {
            PaymentState::Undetected => Ok(PaymentStatusResponse::undetected()),
            PaymentState::Detected => Ok(PaymentStatusResponse::factual(
                "detected",
                confirmations,
                amount_matched,
            )),
            PaymentState::Confirmed => Ok(PaymentStatusResponse::factual(
                "confirmed",
                confirmations,
                amount_matched,
            )),
            PaymentState::Expired => Ok(PaymentStatusResponse::factual(
                "expired",
                confirmations,
                amount_matched,
            )),
        },
    }
}
