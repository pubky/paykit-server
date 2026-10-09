//! Signed Marketplace activation and void lifecycle commands.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    domain::locks::CreatorPubky,
    persistence::{
        MarketplaceActivationInput, MarketplaceActivationResult, MarketplacePreparationStore,
        MarketplaceVoidResult, PersistenceError,
    },
};

pub struct ActivateMarketplaceRequest {
    pub creator: CreatorPubky,
    pub invoice_id: Uuid,
    pub total_sats: u64,
}

pub struct VoidMarketplaceRequest {
    pub creator: CreatorPubky,
    pub invoice_id: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketplaceLifecycleError {
    InvalidRequest,
    Conflict,
    NotFound,
    Unavailable,
}

#[async_trait]
pub trait MarketplaceLifecyclePersistence: Send + Sync {
    async fn activate(
        &self,
        input: MarketplaceActivationInput<'_>,
    ) -> Result<Option<MarketplaceActivationResult>, PersistenceError>;

    async fn void(
        &self,
        creator: &CreatorPubky,
        invoice_id: Uuid,
    ) -> Result<Option<MarketplaceVoidResult>, PersistenceError>;
}

#[async_trait]
impl MarketplaceLifecyclePersistence for MarketplacePreparationStore {
    async fn activate(
        &self,
        input: MarketplaceActivationInput<'_>,
    ) -> Result<Option<MarketplaceActivationResult>, PersistenceError> {
        MarketplacePreparationStore::activate(self, input).await
    }

    async fn void(
        &self,
        creator: &CreatorPubky,
        invoice_id: Uuid,
    ) -> Result<Option<MarketplaceVoidResult>, PersistenceError> {
        MarketplacePreparationStore::void(self, creator, invoice_id).await
    }
}

pub struct MarketplaceLifecycleService {
    store: Arc<dyn MarketplaceLifecyclePersistence>,
    proposal_acceptance_window: Duration,
}

impl MarketplaceLifecycleService {
    pub fn new(
        store: Arc<dyn MarketplaceLifecyclePersistence>,
        proposal_acceptance_window: Duration,
    ) -> Self {
        Self {
            store,
            proposal_acceptance_window,
        }
    }

    pub async fn activate(
        &self,
        request: ActivateMarketplaceRequest,
    ) -> Result<MarketplaceActivationResult, MarketplaceLifecycleError> {
        if request.total_sats == 0 || self.proposal_acceptance_window.is_zero() {
            return Err(MarketplaceLifecycleError::InvalidRequest);
        }
        self.store
            .activate(MarketplaceActivationInput {
                creator: &request.creator,
                invoice_id: request.invoice_id,
                total_sats: request.total_sats,
                proposal_acceptance_seconds: self.proposal_acceptance_window.as_secs(),
            })
            .await
            .map_err(map_store)?
            .ok_or(MarketplaceLifecycleError::NotFound)
    }

    pub async fn void(
        &self,
        request: VoidMarketplaceRequest,
    ) -> Result<MarketplaceVoidResult, MarketplaceLifecycleError> {
        self.store
            .void(&request.creator, request.invoice_id)
            .await
            .map_err(map_store)?
            .ok_or(MarketplaceLifecycleError::NotFound)
    }
}

fn map_store(error: PersistenceError) -> MarketplaceLifecycleError {
    match error {
        PersistenceError::InvalidInput => MarketplaceLifecycleError::InvalidRequest,
        PersistenceError::Conflict => MarketplaceLifecycleError::Conflict,
        PersistenceError::DeploymentMismatch
        | PersistenceError::CorruptOrMissing
        | PersistenceError::ReauthenticationMismatch
        | PersistenceError::Unavailable => MarketplaceLifecycleError::Unavailable,
    }
}
