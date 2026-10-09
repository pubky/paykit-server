//! Signed Marketplace preparation of durable unpublished invoices.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
    time::Duration,
};

use async_trait::async_trait;
use paykit_lib::{
    PaykitAppId, PaymentAmount, PaymentEndpointIdentifier, PaymentEndpointPayload,
    PaymentReference, PaymentRequestTerms,
};

use crate::{
    application::{
        create_invoice::{
            AppRegistryDiscovery, CreatorReceivingProvider, ReaderAuthorization,
            RegistryDiscoveryError, SessionValidationError, SessionValidator,
            derive_bip84_p2wpkh_address,
        },
        reader_registry::reader_is_capable,
        semantic_intent::DeliveryIntentV1,
    },
    config::BitcoinNetwork,
    domain::locks::{CreatorPubky, ReaderPubky},
    persistence::{
        MarketplacePreparationInput, MarketplacePreparationPayloadFactory,
        MarketplacePreparationPayloads, MarketplacePreparationPreflight,
        MarketplacePreparationResult, MarketplacePreparationStore, PersistenceError,
    },
};

pub struct PrepareMarketplaceRequest {
    pub creator: CreatorPubky,
    pub reader: ReaderPubky,
    pub reference: String,
    pub amount_sats: u64,
    pub operation_id: String,
    pub payment_window_seconds: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrepareMarketplaceError {
    InvalidRequest,
    Conflict,
    CreatorSessionInvalid,
    CreatorSessionUnavailable,
    ReaderSetupPending,
    ReaderNotPayable,
    ReaderRegistryUnavailable,
    ReaderRegistryMalformed,
    Unavailable,
}

#[async_trait]
pub trait MarketplacePreparationPersistence: Send + Sync {
    async fn preflight(
        &self,
        creator: &CreatorPubky,
        operation_id: &str,
        request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError>;

    async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError>;
}

#[async_trait]
impl MarketplacePreparationPersistence for MarketplacePreparationStore {
    async fn preflight(
        &self,
        creator: &CreatorPubky,
        operation_id: &str,
        request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError> {
        MarketplacePreparationStore::preflight(self, creator, operation_id, request_binding).await
    }

    async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        MarketplacePreparationStore::prepare(self, input).await
    }
}

pub struct PrepareMarketplaceService {
    sessions: Arc<dyn SessionValidator>,
    registries: Arc<dyn AppRegistryDiscovery>,
    credentials: Arc<dyn CreatorReceivingProvider>,
    store: Arc<dyn MarketplacePreparationPersistence>,
    app_id: PaykitAppId,
    bitcoin_network: BitcoinNetwork,
    payment_window_cap: Duration,
    prepare_ttl: Duration,
}

impl PrepareMarketplaceService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sessions: Arc<dyn SessionValidator>,
        registries: Arc<dyn AppRegistryDiscovery>,
        credentials: Arc<dyn CreatorReceivingProvider>,
        store: Arc<dyn MarketplacePreparationPersistence>,
        app_id: PaykitAppId,
        bitcoin_network: BitcoinNetwork,
        payment_window_cap: Duration,
        prepare_ttl: Duration,
    ) -> Self {
        Self {
            sessions,
            registries,
            credentials,
            store,
            app_id,
            bitcoin_network,
            payment_window_cap,
            prepare_ttl,
        }
    }

    pub async fn prepare(
        &self,
        request: PrepareMarketplaceRequest,
    ) -> Result<MarketplacePreparationResult, PrepareMarketplaceError> {
        let request_binding = request_binding(&request)?;
        // Horizontal replicas are unsupported. Serialize one scoped operation in
        // this process so concurrent retries cannot both observe `New` and repeat
        // mutable external checks before persistence resolves replay or conflict.
        let operation_lock = marketplace_operation_lock(&request.creator, &request.operation_id);
        let _operation_guard = operation_lock.lock().await;
        match self
            .store
            .preflight(&request.creator, &request.operation_id, &request_binding)
            .await
            .map_err(map_store)?
        {
            MarketplacePreparationPreflight::ExactReplay => {
                return self
                    .store
                    .prepare(MarketplacePreparationInput {
                        creator: &request.creator,
                        reader: &request.reader,
                        reference: &request.reference,
                        operation_id: &request.operation_id,
                        request_binding: &request_binding,
                        total_sats: request.amount_sats,
                        payment_window_seconds: request.payment_window_seconds,
                        prepare_ttl_seconds: self.prepare_ttl.as_secs(),
                        payloads: &ReplayOnlyPayloads,
                    })
                    .await
                    .map_err(map_store);
            }
            MarketplacePreparationPreflight::Conflict => {
                return Err(PrepareMarketplaceError::Conflict);
            }
            MarketplacePreparationPreflight::New => {}
        }

        validate_request(&request, self.payment_window_cap)?;

        self.sessions
            .validate(&request.creator)
            .await
            .map_err(|error| match error {
                SessionValidationError::Invalid => PrepareMarketplaceError::CreatorSessionInvalid,
                SessionValidationError::Unavailable => {
                    PrepareMarketplaceError::CreatorSessionUnavailable
                }
            })?;
        let registry = self
            .registries
            .discover(&request.reader)
            .await
            .map_err(map_registry)?
            .ok_or(PrepareMarketplaceError::ReaderSetupPending)?;
        if !reader_is_capable(&registry) {
            return Err(PrepareMarketplaceError::ReaderNotPayable);
        }
        match self
            .registries
            .authorization(&request.reader)
            .await
            .map_err(map_registry)?
        {
            ReaderAuthorization::Verified => {}
            ReaderAuthorization::Missing => {
                return Err(PrepareMarketplaceError::ReaderSetupPending);
            }
            ReaderAuthorization::Invalid => {
                return Err(PrepareMarketplaceError::ReaderNotPayable);
            }
        }
        let receiving = self
            .credentials
            .receiving(&request.creator)
            .await
            .map_err(map_store)?;
        let bitcoin = receiving
            .bitcoin
            .ok_or(PrepareMarketplaceError::InvalidRequest)?;
        let payloads = MarketplacePayloads {
            reader: &request.reader,
            reference: &request.reference,
            amount_sats: request.amount_sats,
            app_id: &self.app_id,
            bitcoin_network: &self.bitcoin_network,
            xpub: &bitcoin.xpub,
            account_index: bitcoin.account_index,
        };
        self.store
            .prepare(MarketplacePreparationInput {
                creator: &request.creator,
                reader: &request.reader,
                reference: &request.reference,
                operation_id: &request.operation_id,
                request_binding: &request_binding,
                total_sats: request.amount_sats,
                payment_window_seconds: request.payment_window_seconds,
                prepare_ttl_seconds: self.prepare_ttl.as_secs(),
                payloads: &payloads,
            })
            .await
            .map_err(map_store)
    }
}

type MarketplaceOperationLock = tokio::sync::Mutex<()>;

fn marketplace_operation_lock(
    creator: &CreatorPubky,
    operation_id: &str,
) -> Arc<MarketplaceOperationLock> {
    type OperationKey = (String, String);
    static LOCKS: OnceLock<StdMutex<HashMap<OperationKey, Weak<MarketplaceOperationLock>>>> =
        OnceLock::new();
    let registry = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let key = (creator.to_string(), operation_id.to_owned());
    let mut registry = registry
        .lock()
        .expect("Marketplace operation lock registry is not poisoned");
    registry.retain(|_, lock| lock.strong_count() != 0);
    if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(MarketplaceOperationLock::new(()));
    registry.insert(key, Arc::downgrade(&lock));
    lock
}

struct ReplayOnlyPayloads;

impl MarketplacePreparationPayloadFactory for ReplayOnlyPayloads {
    fn for_child_index(
        &self,
        _child_index: i64,
    ) -> Result<MarketplacePreparationPayloads, PersistenceError> {
        Err(PersistenceError::CorruptOrMissing)
    }
}

struct MarketplacePayloads<'a> {
    reader: &'a ReaderPubky,
    reference: &'a str,
    amount_sats: u64,
    app_id: &'a PaykitAppId,
    bitcoin_network: &'a BitcoinNetwork,
    xpub: &'a str,
    account_index: u32,
}

impl MarketplacePreparationPayloadFactory for MarketplacePayloads<'_> {
    fn for_child_index(
        &self,
        child_index: i64,
    ) -> Result<MarketplacePreparationPayloads, PersistenceError> {
        let address = derive_bip84_p2wpkh_address(
            self.xpub,
            self.account_index,
            self.bitcoin_network,
            child_index,
        )
        .map_err(|_| PersistenceError::InvalidInput)?;
        let endpoint = PaymentEndpointIdentifier::new(endpoint_identifier(self.bitcoin_network))
            .map_err(|_| PersistenceError::InvalidInput)?;
        let amount = format!(
            "{}.{:08}",
            self.amount_sats / 100_000_000,
            self.amount_sats % 100_000_000
        );
        let terms = PaymentRequestTerms::builder(
            PaymentAmount::new(amount, "btc").map_err(|_| PersistenceError::InvalidInput)?,
            PaymentReference::new(self.reference.to_owned())
                .map_err(|_| PersistenceError::InvalidInput)?,
            vec![endpoint.clone()],
        )
        .required_app_id(Some(self.app_id.clone()))
        .payment_endpoints(Some(HashMap::from([(
            endpoint,
            PaymentEndpointPayload::new(serde_json::json!({"value": address}).to_string()),
        )])))
        .build()
        .map_err(|_| PersistenceError::InvalidInput)?;
        let intent =
            DeliveryIntentV1::payment_request(self.reader.to_string(), self.app_id.clone(), &terms)
                .map_err(|_| PersistenceError::InvalidInput)?;
        Ok(MarketplacePreparationPayloads {
            payment_request_intent: intent,
        })
    }
}

fn validate_request(
    request: &PrepareMarketplaceRequest,
    payment_window_cap: Duration,
) -> Result<(), PrepareMarketplaceError> {
    let reference = uuid::Uuid::parse_str(&request.reference)
        .map_err(|_| PrepareMarketplaceError::InvalidRequest)?;
    if request.operation_id.is_empty()
        || request.amount_sats == 0
        || request.payment_window_seconds == 0
        || request.payment_window_seconds > payment_window_cap.as_secs()
        || reference.get_version_num() != 4
        || reference.get_variant() != uuid::Variant::RFC4122
        || reference.hyphenated().to_string() != request.reference
    {
        return Err(PrepareMarketplaceError::InvalidRequest);
    }
    PaymentReference::new(request.reference.clone())
        .map_err(|_| PrepareMarketplaceError::InvalidRequest)?;
    Ok(())
}

fn request_binding(
    request: &PrepareMarketplaceRequest,
) -> Result<Vec<u8>, PrepareMarketplaceError> {
    serde_json_canonicalizer::to_vec(&serde_json::json!({
        "amount_sats": request.amount_sats,
        "creator": request.creator.to_string(),
        "operation_id": request.operation_id,
        "payment_window_seconds": request.payment_window_seconds,
        "reader": request.reader.to_string(),
        "reference": request.reference,
    }))
    .map_err(|_| PrepareMarketplaceError::InvalidRequest)
}

fn endpoint_identifier(network: &BitcoinNetwork) -> &'static str {
    match network {
        BitcoinNetwork::Mainnet => "btc-bitcoin-p2wpkh",
        BitcoinNetwork::Testnet => "btc-testnet-p2wpkh",
        BitcoinNetwork::Signet => "btc-signet-p2wpkh",
        BitcoinNetwork::Regtest => "btc-regtest-p2wpkh",
    }
}

fn map_registry(error: RegistryDiscoveryError) -> PrepareMarketplaceError {
    match error {
        RegistryDiscoveryError::InvalidRequest => PrepareMarketplaceError::InvalidRequest,
        RegistryDiscoveryError::Unavailable => PrepareMarketplaceError::ReaderRegistryUnavailable,
        RegistryDiscoveryError::Malformed => PrepareMarketplaceError::ReaderRegistryMalformed,
    }
}

fn map_store(error: PersistenceError) -> PrepareMarketplaceError {
    match error {
        PersistenceError::Conflict => PrepareMarketplaceError::Conflict,
        PersistenceError::InvalidInput => PrepareMarketplaceError::InvalidRequest,
        PersistenceError::DeploymentMismatch
        | PersistenceError::CorruptOrMissing
        | PersistenceError::ReauthenticationMismatch
        | PersistenceError::Unavailable => PrepareMarketplaceError::Unavailable,
    }
}
