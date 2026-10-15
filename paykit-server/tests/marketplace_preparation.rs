use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    Extension,
    body::Body,
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use paykit_server::{
    application::{
        create_invoice::{
            AppRegistryDiscovery, CreateInvoiceError, CreatorReceivingProvider,
            ReaderAuthorization, ReceivingDetails, RegistryDiscoveryError, RegistryRetryDelay,
            SessionValidationError, SessionValidator,
        },
        invoice_pricing::ExchangeRates,
        marketplace_preparation::{
            MarketplacePreparationPersistence, MissingSellerRails, PrepareMarketplaceError,
            PrepareMarketplaceRequest, PrepareMarketplaceService,
        },
        marketplace_terms::MarketplaceQuote,
        setup_status::AcceptedAsset,
    },
    config::{BitcoinNetwork, Config, ConfigEnvironment},
    domain::{
        invoice::CriterionAsset,
        locks::{CreatorPubky, parse_creator, parse_reader},
    },
    http::{
        auth::{SignedServiceAuth, signature_preimage},
        marketplace_preparation::marketplace_preparation_router,
    },
    persistence::{
        MarketplacePreparationInput, MarketplacePreparationPreflight, MarketplacePreparationResult,
        PersistenceError,
    },
    runtime::{DependencyCheck, Runtime, operational_router},
};
use tower::ServiceExt;
use tracing::{Event, Subscriber, instrument::WithSubscriber};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const REFERENCE: &str = "7cceb26d-9042-4ea6-bfcb-01bbd778d76e";
const PATH: &str = "/marketplace/payment-requests/prepare";

type CapturedEvents = Vec<Vec<(String, String)>>;

#[derive(Clone, Default)]
struct EventCapture(Arc<Mutex<CapturedEvents>>);

impl<S> Layer<S> for EventCapture
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut fields = Vec::new();
        event.record(&mut FieldVisitor(&mut fields));
        self.0.lock().unwrap().push(fields);
    }
}

struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

fn event_field<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.trim_matches('"'))
}

struct ReadyDependency;

#[async_trait]
impl DependencyCheck for ReadyDependency {
    async fn postgres_ready(&self) -> bool {
        true
    }
}

fn reader() -> String {
    for replacement in "ybndrfg8ejkmcpqxot1uwisza345h769".chars() {
        let mut candidate = CREATOR.to_owned();
        candidate.replace_range(5..6, &replacement.to_string());
        if parse_reader(&candidate).is_ok() {
            return candidate;
        }
    }
    panic!("valid reader fixture")
}

fn request(window: u64) -> PrepareMarketplaceRequest {
    PrepareMarketplaceRequest {
        creator: parse_creator(CREATOR).unwrap(),
        reader: parse_reader(&reader()).unwrap(),
        reference: REFERENCE.into(),
        amount: 50_000,
        asset: CriterionAsset::Btc,
        accepted_assets: vec![AcceptedAsset::Btc],
        operation_id: "marketplace-payment:order-1:attempt-1".into(),
        payment_window_seconds: window,
        description: None,
    }
}

struct FakeSession {
    calls: AtomicUsize,
}

#[async_trait]
impl SessionValidator for FakeSession {
    async fn validate(&self, _creator: &CreatorPubky) -> Result<(), SessionValidationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct PendingSession {
    entered: tokio::sync::Notify,
}

#[async_trait]
impl SessionValidator for PendingSession {
    async fn validate(&self, _creator: &CreatorPubky) -> Result<(), SessionValidationError> {
        self.entered.notify_waiters();
        std::future::pending().await
    }
}

fn capable_registry() -> paykit_lib::PaykitAppRegistry {
    let mut registry = paykit_lib::PaykitAppRegistry::new(Some(
        paykit_lib::derive_paykit_noise_public_key(&[8; 32]),
    ));
    registry
        .register_app(
            paykit_lib::PaykitAppId::new("bitkit").unwrap(),
            paykit_lib::PaykitApp::new(
                "Bitkit",
                paykit_lib::PaykitAppCapabilities {
                    private_payments: false,
                    payment_requests: true,
                    receipts: false,
                    outgoing_payments: true,
                },
            )
            .unwrap(),
        )
        .unwrap();
    registry
}

struct FakeRegistries {
    calls: AtomicUsize,
}

#[async_trait]
impl AppRegistryDiscovery for FakeRegistries {
    async fn discover(
        &self,
        _reader: &paykit_server::domain::locks::ReaderPubky,
    ) -> Result<Option<paykit_lib::PaykitAppRegistry>, RegistryDiscoveryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(capable_registry()))
    }

    async fn authorization(
        &self,
        _reader: &paykit_server::domain::locks::ReaderPubky,
    ) -> Result<ReaderAuthorization, RegistryDiscoveryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReaderAuthorization::Verified)
    }
}

struct MissingThenPresentRegistries {
    discoveries: AtomicUsize,
    present_after: usize,
}

#[async_trait]
impl AppRegistryDiscovery for MissingThenPresentRegistries {
    async fn discover(
        &self,
        _reader: &paykit_server::domain::locks::ReaderPubky,
    ) -> Result<Option<paykit_lib::PaykitAppRegistry>, RegistryDiscoveryError> {
        let attempt = self.discoveries.fetch_add(1, Ordering::SeqCst) + 1;
        Ok((attempt > self.present_after).then(capable_registry))
    }

    async fn authorization(
        &self,
        _reader: &paykit_server::domain::locks::ReaderPubky,
    ) -> Result<ReaderAuthorization, RegistryDiscoveryError> {
        Ok(ReaderAuthorization::Verified)
    }
}

struct NoRegistryRetryDelay;

#[async_trait]
impl RegistryRetryDelay for NoRegistryRetryDelay {
    async fn wait(&self, _retry_index: usize) {}
}

struct FakeCredentials {
    calls: AtomicUsize,
}

#[async_trait]
impl CreatorReceivingProvider for FakeCredentials {
    async fn receiving(
        &self,
        _creator: &CreatorPubky,
    ) -> Result<ReceivingDetails, PersistenceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReceivingDetails {
            bitcoin: Some(paykit_server::domain::receiving::BitcoinAccount {
                xpub: account_xpub().into(),
                account_index: 0,
            }),
            usdt: None,
        })
    }
}

struct MissingBitcoinCredentials;

#[async_trait]
impl CreatorReceivingProvider for MissingBitcoinCredentials {
    async fn receiving(
        &self,
        _creator: &CreatorPubky,
    ) -> Result<ReceivingDetails, PersistenceError> {
        Ok(ReceivingDetails {
            bitcoin: None,
            usdt: None,
        })
    }
}

fn account_xpub() -> String {
    use bitcoin::{
        Network,
        bip32::{ChildNumber, Xpriv, Xpub},
        secp256k1::Secp256k1,
    };
    let secp = Secp256k1::new();
    let account = Xpriv::new_master(Network::Bitcoin, &[42; 32])
        .unwrap()
        .derive_priv(
            &secp,
            &[
                ChildNumber::from_hardened_idx(84).unwrap(),
                ChildNumber::from_hardened_idx(0).unwrap(),
                ChildNumber::from_hardened_idx(0).unwrap(),
            ],
        )
        .unwrap();
    Xpub::from_priv(&secp, &account).to_string()
}

struct FakeStore {
    preflight: MarketplacePreparationPreflight,
    prepare_error: Option<PersistenceError>,
    preflight_calls: AtomicUsize,
    prepare_calls: AtomicUsize,
    captured_reference: Mutex<Option<String>>,
    captured_metadata: Mutex<Option<serde_json::Map<String, serde_json::Value>>>,
    captured_binding: Mutex<Option<Vec<u8>>>,
}

impl FakeStore {
    fn new(preflight: MarketplacePreparationPreflight) -> Self {
        Self {
            preflight,
            prepare_error: None,
            preflight_calls: AtomicUsize::default(),
            prepare_calls: AtomicUsize::default(),
            captured_reference: Mutex::new(None),
            captured_metadata: Mutex::new(None),
            captured_binding: Mutex::new(None),
        }
    }

    fn with_prepare_error(error: PersistenceError) -> Self {
        Self {
            preflight: MarketplacePreparationPreflight::New,
            prepare_error: Some(error),
            preflight_calls: AtomicUsize::default(),
            prepare_calls: AtomicUsize::default(),
            captured_reference: Mutex::new(None),
            captured_metadata: Mutex::new(None),
            captured_binding: Mutex::new(None),
        }
    }
}

#[async_trait]
impl MarketplacePreparationPersistence for FakeStore {
    async fn preflight(
        &self,
        _creator: &CreatorPubky,
        _operation_id: &str,
        request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError> {
        self.preflight_calls.fetch_add(1, Ordering::SeqCst);
        *self.captured_binding.lock().unwrap() = Some(request_binding.to_vec());
        Ok(self.preflight)
    }

    async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.prepare_error {
            return Err(error);
        }
        if self.preflight == MarketplacePreparationPreflight::New {
            let payloads = input.payloads.for_child_index(0)?;
            *self.captured_reference.lock().unwrap() = Some(
                payloads
                    .payment_request_intent
                    .terms()
                    .unwrap()
                    .payment_reference
                    .clone(),
            );
            *self.captured_metadata.lock().unwrap() = Some(
                payloads
                    .payment_request_intent
                    .terms()
                    .unwrap()
                    .metadata
                    .clone(),
            );
        }
        Ok(MarketplacePreparationResult::new(
            uuid::Uuid::from_u128(1),
            input.terms.clone(),
            MarketplaceQuote::new(vec![(AcceptedAsset::Btc, input.terms.amount())], Vec::new()),
            time::OffsetDateTime::UNIX_EPOCH,
            time::OffsetDateTime::UNIX_EPOCH + time::Duration::minutes(15),
            self.preflight == MarketplacePreparationPreflight::ExactReplay,
        ))
    }
}

struct ConcurrentStore {
    created: AtomicBool,
    prepare_calls: AtomicUsize,
}

#[async_trait]
impl MarketplacePreparationPersistence for ConcurrentStore {
    async fn preflight(
        &self,
        _creator: &CreatorPubky,
        _operation_id: &str,
        _request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError> {
        let created = self.created.load(Ordering::SeqCst);
        if !created {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(if created {
            MarketplacePreparationPreflight::ExactReplay
        } else {
            MarketplacePreparationPreflight::New
        })
    }

    async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        let replayed = self.created.swap(true, Ordering::SeqCst);
        if !replayed {
            input.payloads.for_child_index(0)?;
        }
        Ok(MarketplacePreparationResult::new(
            uuid::Uuid::from_u128(1),
            input.terms.clone(),
            MarketplaceQuote::new(vec![(AcceptedAsset::Btc, input.terms.amount())], Vec::new()),
            time::OffsetDateTime::UNIX_EPOCH,
            time::OffsetDateTime::UNIX_EPOCH + time::Duration::minutes(15),
            replayed,
        ))
    }
}

struct Deps {
    service: Arc<PrepareMarketplaceService>,
    session: Arc<FakeSession>,
    registries: Arc<FakeRegistries>,
    credentials: Arc<FakeCredentials>,
    store: Arc<FakeStore>,
}

fn service(preflight: MarketplacePreparationPreflight) -> Deps {
    let session = Arc::new(FakeSession {
        calls: AtomicUsize::default(),
    });
    let registries = Arc::new(FakeRegistries {
        calls: AtomicUsize::default(),
    });
    let credentials = Arc::new(FakeCredentials {
        calls: AtomicUsize::default(),
    });
    let store = Arc::new(FakeStore::new(preflight));
    let service = Arc::new(PrepareMarketplaceService::new(
        session.clone(),
        registries.clone(),
        credentials.clone(),
        store.clone(),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    ));
    Deps {
        service,
        session,
        registries,
        credentials,
        store,
    }
}

fn service_with_prepare_error(error: PersistenceError) -> Deps {
    let session = Arc::new(FakeSession {
        calls: AtomicUsize::default(),
    });
    let registries = Arc::new(FakeRegistries {
        calls: AtomicUsize::default(),
    });
    let credentials = Arc::new(FakeCredentials {
        calls: AtomicUsize::default(),
    });
    let store = Arc::new(FakeStore::with_prepare_error(error));
    let service = Arc::new(PrepareMarketplaceService::new(
        session.clone(),
        registries.clone(),
        credentials.clone(),
        store.clone(),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    ));
    Deps {
        service,
        session,
        registries,
        credentials,
        store,
    }
}

#[tokio::test]
async fn exact_replay_and_conflict_precede_mutable_dependencies() {
    let replay = service(MarketplacePreparationPreflight::ExactReplay);
    assert!(
        replay
            .service
            .prepare(request(86_400))
            .await
            .unwrap()
            .replayed()
    );
    assert_eq!(replay.session.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay.registries.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay.credentials.calls.load(Ordering::SeqCst), 0);

    let conflict = service(MarketplacePreparationPreflight::Conflict);
    assert_eq!(
        conflict.service.prepare(request(86_400)).await,
        Err(PrepareMarketplaceError::Conflict)
    );
    assert_eq!(conflict.session.calls.load(Ordering::SeqCst), 0);
    assert_eq!(conflict.store.prepare_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn new_preparation_validates_dependencies_and_builds_caller_reference() {
    let deps = service(MarketplacePreparationPreflight::New);
    let result = deps.service.prepare(request(86_400)).await.unwrap();
    assert!(!result.replayed());
    assert_eq!(deps.session.calls.load(Ordering::SeqCst), 1);
    assert_eq!(deps.registries.calls.load(Ordering::SeqCst), 2);
    assert_eq!(deps.credentials.calls.load(Ordering::SeqCst), 1);
    assert_eq!(deps.store.prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        deps.store.captured_reference.lock().unwrap().as_deref(),
        Some(REFERENCE)
    );
    assert_eq!(
        deps.store.captured_metadata.lock().unwrap().as_ref(),
        Some(&serde_json::Map::new())
    );
}

#[tokio::test]
async fn omitted_description_keeps_exact_legacy_binding() {
    let deps = service(MarketplacePreparationPreflight::New);
    deps.service.prepare(request(86_400)).await.unwrap();

    let expected = serde_json_canonicalizer::to_vec(&serde_json::json!({
        "accepted_assets": ["BTC"],
        "amount": "50000",
        "asset": "BTC",
        "creator": CREATOR,
        "operation_id": "marketplace-payment:order-1:attempt-1",
        "payment_window_seconds": 86_400,
        "reader": reader(),
        "reference": REFERENCE,
    }))
    .unwrap();
    assert_eq!(
        deps.store.captured_binding.lock().unwrap().as_deref(),
        Some(expected.as_slice())
    );
}

#[tokio::test]
async fn present_description_is_bound_and_copied_exactly_to_note_metadata() {
    let deps = service(MarketplacePreparationPreflight::New);
    let mut described = request(86_400);
    described.description = Some("Café order 🛒".into());
    deps.service.prepare(described).await.unwrap();

    let binding: serde_json::Value = serde_json::from_slice(
        deps.store
            .captured_binding
            .lock()
            .unwrap()
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(binding["description"], "Café order 🛒");
    assert_eq!(
        deps.store.captured_metadata.lock().unwrap().as_ref(),
        Some(&serde_json::Map::from_iter([(
            "note".into(),
            serde_json::Value::String("Café order 🛒".into()),
        )]))
    );

    let replay = service(MarketplacePreparationPreflight::ExactReplay);
    let mut same = request(86_400);
    same.description = Some("Café order 🛒".into());
    assert!(replay.service.prepare(same).await.unwrap().replayed());
    assert_eq!(replay.session.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay.store.prepare_calls.load(Ordering::SeqCst), 1);
    assert!(replay.store.captured_metadata.lock().unwrap().is_none());
}

#[tokio::test]
async fn description_validation_uses_unicode_scalars_and_rejects_unsafe_text() {
    let valid = ["é".repeat(140), "商品 42 🛒".into()];
    for description in valid {
        let deps = service(MarketplacePreparationPreflight::New);
        let mut candidate = request(86_400);
        candidate.description = Some(description.clone());
        assert!(
            deps.service.prepare(candidate).await.is_ok(),
            "valid description rejected: {description:?}"
        );
    }

    let invalid = [
        String::new(),
        " \t\n".into(),
        "a".repeat(141),
        "control\u{0000}".into(),
        "direction\u{061c}".into(),
        "direction\u{200e}".into(),
        "direction\u{202a}".into(),
        "direction\u{2066}".into(),
        "line\u{2028}separator".into(),
        "paragraph\u{2029}separator".into(),
    ];
    for description in invalid {
        let deps = service(MarketplacePreparationPreflight::New);
        let mut candidate = request(86_400);
        candidate.description = Some(description);
        assert_eq!(
            deps.service.prepare(candidate).await,
            Err(PrepareMarketplaceError::InvalidRequest)
        );
        assert_eq!(deps.store.prepare_calls.load(Ordering::SeqCst), 0);
    }

    let deps = service(MarketplacePreparationPreflight::New);
    let mut oversized = request(86_400);
    oversized.description = Some("🛒".repeat(140));
    assert_eq!(
        deps.service.prepare(oversized).await,
        Err(PrepareMarketplaceError::InvalidRequest),
        "scalar-valid descriptions must still fit the actual serialized Payment Request frame"
    );
    assert_eq!(deps.store.prepare_calls.load(Ordering::SeqCst), 1);
    assert!(deps.store.captured_metadata.lock().unwrap().is_none());
}

#[tokio::test]
async fn changed_description_conflicts_before_mutable_dependencies() {
    let deps = service(MarketplacePreparationPreflight::Conflict);
    let mut changed = request(86_400);
    changed.description = Some("changed".into());

    assert_eq!(
        deps.service.prepare(changed).await,
        Err(PrepareMarketplaceError::Conflict)
    );
    assert_eq!(deps.session.calls.load(Ordering::SeqCst), 0);
    assert_eq!(deps.registries.calls.load(Ordering::SeqCst), 0);
    assert_eq!(deps.credentials.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn registry_discovery_retries_missing_records_three_times() {
    for (present_after, expected) in [
        (1, Ok(())),
        (usize::MAX, Err(PrepareMarketplaceError::ReaderSetupPending)),
    ] {
        let session = Arc::new(FakeSession {
            calls: AtomicUsize::default(),
        });
        let registries = Arc::new(MissingThenPresentRegistries {
            discoveries: AtomicUsize::default(),
            present_after,
        });
        let credentials = Arc::new(FakeCredentials {
            calls: AtomicUsize::default(),
        });
        let store = Arc::new(FakeStore::new(MarketplacePreparationPreflight::New));
        let service = PrepareMarketplaceService::new(
            session,
            registries.clone(),
            credentials,
            store,
            paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
            BitcoinNetwork::Mainnet,
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(15 * 60),
        )
        .with_registry_retry_delay(Arc::new(NoRegistryRetryDelay));

        assert_eq!(service.prepare(request(86_400)).await.map(|_| ()), expected);
        assert_eq!(
            registries.discoveries.load(Ordering::SeqCst),
            if present_after == 1 { 2 } else { 3 }
        );
    }
}

#[tokio::test]
async fn pending_external_work_obeys_request_deadline() {
    let service = PrepareMarketplaceService::new(
        Arc::new(PendingSession {
            entered: tokio::sync::Notify::new(),
        }),
        Arc::new(FakeRegistries {
            calls: AtomicUsize::default(),
        }),
        Arc::new(FakeCredentials {
            calls: AtomicUsize::default(),
        }),
        Arc::new(FakeStore::new(MarketplacePreparationPreflight::New)),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    )
    .with_request_deadline(Duration::from_millis(20));

    assert_eq!(
        service.prepare(request(86_400)).await,
        Err(PrepareMarketplaceError::DeadlineExceeded)
    );
}

#[tokio::test]
async fn pending_external_route_returns_stable_dependency_timeout() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let service = Arc::new(
        PrepareMarketplaceService::new(
            Arc::new(PendingSession {
                entered: tokio::sync::Notify::new(),
            }),
            Arc::new(FakeRegistries {
                calls: AtomicUsize::default(),
            }),
            Arc::new(FakeCredentials {
                calls: AtomicUsize::default(),
            }),
            Arc::new(FakeStore::new(MarketplacePreparationPreflight::New)),
            paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
            BitcoinNetwork::Mainnet,
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(15 * 60),
        )
        .with_request_deadline(Duration::from_millis(20)),
    );
    let router = marketplace_preparation_router(service).layer(Extension(signed_auth(&key)));

    let response = router
        .oneshot(signed_request(&key, body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response_body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response_body).unwrap(),
        serde_json::json!({"error":{"code":"dependency_timeout","message":"request deadline exceeded"}})
    );
}

#[tokio::test]
async fn operation_lock_wait_obeys_request_deadline() {
    let session = Arc::new(PendingSession {
        entered: tokio::sync::Notify::new(),
    });
    let registries = Arc::new(FakeRegistries {
        calls: AtomicUsize::default(),
    });
    let credentials = Arc::new(FakeCredentials {
        calls: AtomicUsize::default(),
    });
    let store = Arc::new(FakeStore::new(MarketplacePreparationPreflight::New));
    let build = |deadline| {
        PrepareMarketplaceService::new(
            session.clone(),
            registries.clone(),
            credentials.clone(),
            store.clone(),
            paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
            BitcoinNetwork::Mainnet,
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(15 * 60),
        )
        .with_request_deadline(deadline)
    };
    let holder = Arc::new(build(Duration::from_secs(1)));
    let waiter = build(Duration::from_millis(20));
    let holder_task = tokio::spawn({
        let holder = holder.clone();
        async move { holder.prepare(request(86_400)).await }
    });
    session.entered.notified().await;

    assert_eq!(
        waiter.prepare(request(86_400)).await,
        Err(PrepareMarketplaceError::DeadlineExceeded)
    );
    assert_eq!(store.preflight_calls.load(Ordering::SeqCst), 1);
    holder_task.abort();
}

#[tokio::test]
async fn missing_seller_bitcoin_details_is_typed_setup_pending() {
    let service = PrepareMarketplaceService::new(
        Arc::new(FakeSession {
            calls: AtomicUsize::default(),
        }),
        Arc::new(FakeRegistries {
            calls: AtomicUsize::default(),
        }),
        Arc::new(MissingBitcoinCredentials),
        Arc::new(FakeStore::new(MarketplacePreparationPreflight::New)),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    );

    assert_eq!(
        service.prepare(request(86_400)).await,
        Err(PrepareMarketplaceError::SellerSetupPending(
            MissingSellerRails::Bitcoin
        ))
    );
}

#[tokio::test]
async fn invalid_or_over_cap_window_fails_before_preflight() {
    for (window, expected) in [
        (0, PrepareMarketplaceError::InvalidRequest),
        (86_401, PrepareMarketplaceError::PaymentWindowExceedsCap),
    ] {
        let deps = service(MarketplacePreparationPreflight::New);
        assert_eq!(deps.service.prepare(request(window)).await, Err(expected));
        assert_eq!(deps.store.prepare_calls.load(Ordering::SeqCst), 0);
        assert_eq!(deps.session.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn persisted_operation_wins_over_current_window_policy() {
    let replay = service(MarketplacePreparationPreflight::ExactReplay);
    assert!(
        replay
            .service
            .prepare(request(86_401))
            .await
            .unwrap()
            .replayed()
    );
    assert_eq!(replay.session.calls.load(Ordering::SeqCst), 0);

    let conflict = service(MarketplacePreparationPreflight::Conflict);
    assert_eq!(
        conflict.service.prepare(request(86_401)).await,
        Err(PrepareMarketplaceError::Conflict)
    );
    assert_eq!(conflict.session.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_exact_retry_runs_mutable_dependencies_once() {
    let session = Arc::new(FakeSession {
        calls: AtomicUsize::default(),
    });
    let registries = Arc::new(FakeRegistries {
        calls: AtomicUsize::default(),
    });
    let credentials = Arc::new(FakeCredentials {
        calls: AtomicUsize::default(),
    });
    let store = Arc::new(ConcurrentStore {
        created: AtomicBool::default(),
        prepare_calls: AtomicUsize::default(),
    });
    let service = Arc::new(PrepareMarketplaceService::new(
        session.clone(),
        registries.clone(),
        credentials.clone(),
        store.clone(),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    ));

    let first = tokio::spawn({
        let service = service.clone();
        async move { service.prepare(request(86_400)).await.unwrap() }
    });
    let second = tokio::spawn({
        let service = service.clone();
        async move { service.prepare(request(86_400)).await.unwrap() }
    });
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();

    assert_ne!(first.replayed(), second.replayed());
    assert_eq!(session.calls.load(Ordering::SeqCst), 1);
    assert_eq!(registries.calls.load(Ordering::SeqCst), 2);
    assert_eq!(credentials.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.prepare_calls.load(Ordering::SeqCst), 2);
}

fn signed_auth(key: &SigningKey) -> Arc<SignedServiceAuth> {
    let key = pubky::PublicKey::from(
        pubky::pkarr::PublicKey::try_from(key.verifying_key().as_bytes()).unwrap(),
    )
    .to_string();
    let config = Config::from_toml_and_environment(
        &format!(
            r#"
[http]
listen_addr = "127.0.0.1:8080"
[signed_services]
trusted_public_keys = ["{key}"]
[setup]
allowed_origins = ["https://app.example"]
[paykit]
client_id = "app.paykit.server"
app_id = "paykit-server"
network = "testnet"
[bitcoin]
network = "mainnet"
[electrum]
endpoint = "ssl://electrum.example:50002"
[outbox]
poll_interval = "5s"
"#
        ),
        ConfigEnvironment {
            database_url: Some("postgres://paykit:***@localhost/paykit".into()),
            master_key: Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE".into()),
        },
    )
    .unwrap();
    Arc::new(SignedServiceAuth::from_config(&config))
}

fn body(extra: bool) -> Vec<u8> {
    let mut value = serde_json::json!({
        "accepted_assets": ["BTC"],
        "amount": "50000",
        "asset": "BTC",
        "creator": CREATOR,
        "operation_id": "marketplace-payment:order-1:attempt-1",
        "payment_window_seconds": 86_400,
        "reader": reader(),
        "reference": REFERENCE,
    });
    if extra {
        value
            .as_object_mut()
            .unwrap()
            .insert("stack_id".into(), serde_json::json!("forbidden"));
    }
    serde_json_canonicalizer::to_vec(&value).unwrap()
}

fn body_with_description(description: serde_json::Value) -> Vec<u8> {
    let mut value: serde_json::Value = serde_json::from_slice(&body(false)).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("description".into(), description);
    serde_json_canonicalizer::to_vec(&value).unwrap()
}

fn signed_request(key: &SigningKey, body: Vec<u8>) -> Request<Body> {
    let preimage = signature_preimage("POST", PATH, &body);
    Request::builder()
        .method(Method::POST)
        .uri(PATH)
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes()),
        )
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn signed_prepare_route_has_closed_request_and_response() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let deps = service(MarketplacePreparationPreflight::ExactReplay);
    let router = marketplace_preparation_router(deps.service).layer(Extension(signed_auth(&key)));
    let response = router
        .clone()
        .oneshot(signed_request(&key, body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response_body).unwrap(),
        serde_json::json!({
            "invoice_id": "00000000-0000-0000-0000-000000000001",
            "prepare_expires_at": "1970-01-01T00:15:00Z",
            "state": "prepared",
            "amount": "50000",
            "asset": "BTC",
            "accepted_assets": ["BTC"],
            "payment_amounts": {"BTC": "50000"},
            "rates": {},
        })
    );

    let response = router
        .oneshot(signed_request(&key, body(true)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn signed_prepare_route_distinguishes_omitted_description_from_null_and_wrong_type() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let deps = service(MarketplacePreparationPreflight::New);
    let router = marketplace_preparation_router(deps.service).layer(Extension(signed_auth(&key)));

    let valid = router
        .clone()
        .oneshot(signed_request(
            &key,
            body_with_description(serde_json::json!("Café order 🛒")),
        ))
        .await
        .unwrap();
    assert_eq!(valid.status(), StatusCode::OK);

    let omitted = router
        .clone()
        .oneshot(signed_request(&key, body(false)))
        .await
        .unwrap();
    assert_eq!(omitted.status(), StatusCode::OK);

    for invalid in [serde_json::Value::Null, serde_json::json!(42)] {
        let response = router
            .clone()
            .oneshot(signed_request(&key, body_with_description(invalid)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn prepare_route_preserves_binding_conflict_but_not_storage_invariant_failure() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let conflict = service(MarketplacePreparationPreflight::Conflict);
    let conflict_router =
        marketplace_preparation_router(conflict.service).layer(Extension(signed_auth(&key)));
    let response = conflict_router
        .oneshot(signed_request(&key, body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response_body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response_body).unwrap(),
        serde_json::json!({"error":{"code":"operation_conflict","message":"operation binding conflicts with persisted payment state"}})
    );

    let invariant = service_with_prepare_error(PersistenceError::CorruptOrMissing);
    let invariant_router =
        marketplace_preparation_router(invariant.service).layer(Extension(signed_auth(&key)));
    let response = invariant_router
        .oneshot(signed_request(&key, body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_ne!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn prepare_route_distinguishes_seller_setup_from_invalid_request() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let service = Arc::new(PrepareMarketplaceService::new(
        Arc::new(FakeSession {
            calls: AtomicUsize::default(),
        }),
        Arc::new(FakeRegistries {
            calls: AtomicUsize::default(),
        }),
        Arc::new(MissingBitcoinCredentials),
        Arc::new(FakeStore::new(MarketplacePreparationPreflight::New)),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    ));
    let router = marketplace_preparation_router(service).layer(Extension(signed_auth(&key)));

    let response = router
        .oneshot(signed_request(&key, body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response_body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response_body).unwrap(),
        serde_json::json!({"error":{"code":"seller_setup_pending","message":"seller Bitcoin receiving setup is needed"}})
    );
}

#[tokio::test]
async fn setup_pending_emits_exact_redacted_failure_stage() {
    let request_id = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let key = SigningKey::from_bytes(&[11; 32]);
    let service = PrepareMarketplaceService::new(
        Arc::new(FakeSession {
            calls: AtomicUsize::default(),
        }),
        Arc::new(MissingThenPresentRegistries {
            discoveries: AtomicUsize::default(),
            present_after: usize::MAX,
        }),
        Arc::new(FakeCredentials {
            calls: AtomicUsize::default(),
        }),
        Arc::new(FakeStore::new(MarketplacePreparationPreflight::New)),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    )
    .with_registry_retry_delay(Arc::new(NoRegistryRetryDelay));
    let router = operational_router(
        marketplace_preparation_router(Arc::new(service)).layer(Extension(signed_auth(&key))),
        Arc::new(Runtime::new(Arc::new(ReadyDependency), 1)),
    );
    let mut request = signed_request(&key, body(false));
    request
        .headers_mut()
        .insert("x-request-id", request_id.parse().unwrap());
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    let response = router
        .oneshot(request)
        .with_subscriber(subscriber)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["x-request-id"], request_id);
    let events: Vec<_> = capture
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event_field(event, "event") == Some("paykit_request_failure"))
        .cloned()
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(
        event_field(&events[0], "operation"),
        Some("marketplace_prepare")
    );
    assert_eq!(
        event_field(&events[0], "stage"),
        Some("reader_app_registry_check")
    );
    assert_eq!(
        event_field(&events[0], "category"),
        Some("reader_setup_pending")
    );
    assert_eq!(event_field(&events[0], "request_id"), Some(request_id));
    assert!(events[0].iter().all(|(name, _)| {
        matches!(
            name.as_str(),
            "message" | "event" | "operation" | "stage" | "category" | "request_id"
        )
    }));
}

const USDT_ADDRESS: &str = "0x1111111111111111111111111111111111111111";

struct RailCredentials {
    bitcoin: bool,
    usdt: bool,
    calls: AtomicUsize,
}

#[async_trait]
impl CreatorReceivingProvider for RailCredentials {
    async fn receiving(
        &self,
        _creator: &CreatorPubky,
    ) -> Result<ReceivingDetails, PersistenceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReceivingDetails {
            bitcoin: self
                .bitcoin
                .then(|| paykit_server::domain::receiving::BitcoinAccount {
                    xpub: account_xpub().into(),
                    account_index: 0,
                }),
            usdt: self
                .usdt
                .then(|| USDT_ADDRESS.to_owned().try_into().unwrap()),
        })
    }
}

struct FixedRates {
    price: Option<&'static str>,
    calls: AtomicUsize,
}

#[async_trait]
impl ExchangeRates for FixedRates {
    async fn usd_per_btc(&self) -> Result<String, CreateInvoiceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.price
            .map(str::to_owned)
            .ok_or(CreateInvoiceError::Unavailable)
    }
}

#[derive(Default)]
struct CapturingStore {
    bindings: Mutex<Vec<Vec<u8>>>,
    terms: Mutex<Option<paykit_server::application::semantic_intent::PaymentTermsV1>>,
    result: Mutex<Option<MarketplacePreparationResult>>,
    prepare_calls: AtomicUsize,
}

#[async_trait]
impl MarketplacePreparationPersistence for CapturingStore {
    async fn preflight(
        &self,
        _creator: &CreatorPubky,
        _operation_id: &str,
        request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError> {
        self.bindings.lock().unwrap().push(request_binding.to_vec());
        Ok(MarketplacePreparationPreflight::New)
    }

    async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        let payloads = input.payloads.for_child_index(0)?;
        let quote = MarketplaceQuote::from_intent(&payloads.payment_request_intent, input.terms)?;
        *self.terms.lock().unwrap() =
            Some(payloads.payment_request_intent.terms().unwrap().clone());
        let result = MarketplacePreparationResult::new(
            uuid::Uuid::from_u128(1),
            input.terms.clone(),
            quote,
            time::OffsetDateTime::UNIX_EPOCH,
            time::OffsetDateTime::UNIX_EPOCH + time::Duration::minutes(15),
            false,
        );
        *self.result.lock().unwrap() = Some(result.clone());
        Ok(result)
    }
}

struct Rig {
    service: PrepareMarketplaceService,
    session: Arc<FakeSession>,
    credentials: Arc<RailCredentials>,
    rates: Arc<FixedRates>,
    store: Arc<CapturingStore>,
}

fn rig(bitcoin: bool, usdt: bool, usdt_enabled: bool, price: Option<&'static str>) -> Rig {
    let session = Arc::new(FakeSession {
        calls: AtomicUsize::default(),
    });
    let credentials = Arc::new(RailCredentials {
        bitcoin,
        usdt,
        calls: AtomicUsize::default(),
    });
    let rates = Arc::new(FixedRates {
        price,
        calls: AtomicUsize::default(),
    });
    let store = Arc::new(CapturingStore::default());
    let service = PrepareMarketplaceService::new(
        session.clone(),
        Arc::new(FakeRegistries {
            calls: AtomicUsize::default(),
        }),
        credentials.clone(),
        store.clone(),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    )
    .with_usdt(usdt_enabled)
    .with_conversion_payment_window(Duration::from_secs(60 * 60))
    .with_exchange_rates(rates.clone());
    Rig {
        service,
        session,
        credentials,
        rates,
        store,
    }
}

fn multi_request(
    amount: u64,
    asset: CriterionAsset,
    accepted: &[AcceptedAsset],
    window: u64,
) -> PrepareMarketplaceRequest {
    PrepareMarketplaceRequest {
        amount,
        asset,
        accepted_assets: accepted.to_vec(),
        ..request(window)
    }
}

const BTC: AcceptedAsset = AcceptedAsset::Btc;
const USDT: AcceptedAsset = AcceptedAsset::Usdt;
const FEED: &str = "81000";

#[tokio::test]
async fn every_denomination_and_accepted_asset_combination_is_prepared_exactly() {
    use CriterionAsset::{Btc as InBtc, Usd, Usdt as InUsdt};
    let btc_rate = "0.000012345679012346";
    // (denomination, amount, accepted, intent amount, payable per asset, rates, feed read)
    #[allow(clippy::type_complexity)]
    let cases: Vec<(
        CriterionAsset,
        u64,
        Vec<AcceptedAsset>,
        &str,
        Vec<(AcceptedAsset, u64)>,
        Vec<(AcceptedAsset, &str)>,
        bool,
    )> = vec![
        (
            Usd,
            500,
            vec![BTC],
            "5.00",
            vec![(BTC, 6_173)],
            vec![(BTC, btc_rate)],
            true,
        ),
        (
            Usd,
            500,
            vec![USDT],
            "5.00",
            vec![(USDT, 5_000_000)],
            vec![(USDT, "1")],
            false,
        ),
        (
            Usd,
            500,
            vec![BTC, USDT],
            "5.00",
            vec![(BTC, 6_173), (USDT, 5_000_000)],
            vec![(BTC, btc_rate), (USDT, "1")],
            true,
        ),
        (
            InBtc,
            50_000,
            vec![BTC],
            "0.00050000",
            vec![(BTC, 50_000)],
            vec![],
            false,
        ),
        (
            InBtc,
            50_000,
            vec![USDT],
            "0.00050000",
            vec![(USDT, 40_500_000)],
            vec![(USDT, "81000")],
            true,
        ),
        (
            InBtc,
            50_000,
            vec![BTC, USDT],
            "0.00050000",
            vec![(BTC, 50_000), (USDT, 40_500_000)],
            vec![(USDT, "81000")],
            true,
        ),
        (
            InUsdt,
            5_000_000,
            vec![BTC],
            "5.000000",
            vec![(BTC, 6_173)],
            vec![(BTC, btc_rate)],
            true,
        ),
        (
            InUsdt,
            5_000_000,
            vec![USDT],
            "5.000000",
            vec![(USDT, 5_000_000)],
            vec![],
            false,
        ),
        (
            InUsdt,
            5_000_000,
            vec![BTC, USDT],
            "5.000000",
            vec![(BTC, 6_173), (USDT, 5_000_000)],
            vec![(BTC, btc_rate)],
            true,
        ),
    ];
    for (asset, amount, accepted, intent_amount, payable, rates, feed_read) in cases {
        let rig = rig(true, true, true, Some(FEED));
        let result = rig
            .service
            .prepare(multi_request(amount, asset, &accepted, 3_600))
            .await
            .unwrap_or_else(|error| panic!("{asset:?} {accepted:?}: {error:?}"));
        let label = format!("{asset:?} {accepted:?}");
        assert_eq!(result.quote().payment_amounts(), payable, "{label}");
        assert_eq!(
            result.quote().rates(),
            rates
                .iter()
                .map(|(asset, value)| (*asset, (*value).to_owned()))
                .collect::<Vec<_>>(),
            "{label}"
        );
        assert_eq!(
            result.terms().accepted_assets(),
            accepted.as_slice(),
            "{label}"
        );
        assert_eq!(
            rig.rates.calls.load(Ordering::SeqCst),
            usize::from(feed_read),
            "{label}"
        );
        let terms = rig.store.terms.lock().unwrap().clone().unwrap();
        assert_eq!(terms.asset, asset.as_str().to_ascii_lowercase(), "{label}");
        assert_eq!(terms.amount, intent_amount, "{label}");
        let mut endpoints: Vec<_> = terms.payment_endpoints.keys().cloned().collect();
        endpoints.sort();
        let mut expected: Vec<_> = accepted
            .iter()
            .map(|asset| match asset {
                AcceptedAsset::Btc => "btc-bitcoin-p2wpkh",
                AcceptedAsset::Usdt => "usdt-arbitrum-address",
            })
            .map(str::to_owned)
            .collect();
        expected.sort();
        assert_eq!(
            endpoints, expected,
            "no rail the request did not ask for: {label}"
        );
        assert_eq!(
            terms.accepted_endpoint_identifiers.len(),
            accepted.len(),
            "{label}"
        );
    }
}

#[tokio::test]
async fn payable_amounts_round_up_to_each_endpoint_unit() {
    use CriterionAsset::{Btc as InBtc, Usd};
    for (asset, amount, accepted, expected) in [
        (Usd, 1, vec![BTC, USDT], vec![(BTC, 13), (USDT, 10_000)]),
        (InBtc, 1, vec![BTC, USDT], vec![(BTC, 1), (USDT, 810)]),
        (InBtc, 3, vec![BTC, USDT], vec![(BTC, 3), (USDT, 2_430)]),
        (Usd, 8_100_000, vec![BTC], vec![(BTC, 100_000_001)]),
    ] {
        let rig = rig(true, true, true, Some(FEED));
        let result = rig
            .service
            .prepare(multi_request(amount, asset, &accepted, 3_600))
            .await
            .unwrap();
        assert_eq!(
            result.quote().payment_amounts(),
            expected,
            "{asset:?} {amount}"
        );
    }
}

#[tokio::test]
async fn amount_that_overflows_a_payment_asset_is_an_invalid_request() {
    let rig = rig(true, true, true, Some(FEED));
    assert_eq!(
        rig.service
            .prepare(multi_request(u64::MAX, CriterionAsset::Btc, &[USDT], 3_600))
            .await,
        Err(PrepareMarketplaceError::InvalidRequest)
    );
    assert_eq!(rig.store.prepare_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn seller_missing_a_requested_rail_is_a_typed_setup_refusal_that_names_it() {
    use CriterionAsset::Usd;
    for (bitcoin, usdt, accepted, expected) in [
        (false, true, vec![BTC], MissingSellerRails::Bitcoin),
        (true, false, vec![USDT], MissingSellerRails::Usdt),
        (
            false,
            false,
            vec![BTC, USDT],
            MissingSellerRails::BitcoinAndUsdt,
        ),
        (true, false, vec![BTC, USDT], MissingSellerRails::Usdt),
        (false, true, vec![BTC, USDT], MissingSellerRails::Bitcoin),
    ] {
        let rig = rig(bitcoin, usdt, true, Some(FEED));
        assert_eq!(
            rig.service
                .prepare(multi_request(500, Usd, &accepted, 3_600))
                .await,
            Err(PrepareMarketplaceError::SellerSetupPending(expected)),
            "{accepted:?} bitcoin={bitcoin} usdt={usdt}"
        );
        assert_eq!(rig.store.prepare_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            rig.rates.calls.load(Ordering::SeqCst),
            0,
            "no feed read before the seller can receive"
        );
    }
}

#[tokio::test]
async fn seller_rail_that_was_not_requested_is_never_added() {
    let rig = rig(true, true, true, Some(FEED));
    rig.service
        .prepare(multi_request(50_000, CriterionAsset::Btc, &[BTC], 3_600))
        .await
        .unwrap();
    let terms = rig.store.terms.lock().unwrap().clone().unwrap();
    assert_eq!(
        terms.payment_endpoints.keys().collect::<Vec<_>>(),
        ["btc-bitcoin-p2wpkh"]
    );
    assert!(terms.rates.is_empty());
}

#[tokio::test]
async fn usdt_without_server_configuration_is_asset_unsupported_before_any_read() {
    use CriterionAsset::{Btc as InBtc, Usd, Usdt as InUsdt};
    for (asset, accepted) in [
        (Usd, vec![USDT]),
        (Usd, vec![BTC, USDT]),
        (InBtc, vec![USDT]),
        (InUsdt, vec![BTC]),
    ] {
        let rig = rig(true, true, false, Some(FEED));
        assert_eq!(
            rig.service
                .prepare(multi_request(500, asset, &accepted, 3_600))
                .await,
            Err(PrepareMarketplaceError::AssetUnsupported),
            "{asset:?} {accepted:?}"
        );
        assert_eq!(rig.session.calls.load(Ordering::SeqCst), 0);
        assert_eq!(rig.credentials.calls.load(Ordering::SeqCst), 0);
        assert_eq!(rig.rates.calls.load(Ordering::SeqCst), 0);
    }
    let rig = rig(true, false, false, Some(FEED));
    rig.service
        .prepare(multi_request(500, CriterionAsset::Usd, &[BTC], 3_600))
        .await
        .unwrap();
}

#[tokio::test]
async fn window_over_the_applicable_cap_is_refused_never_clamped() {
    use CriterionAsset::{Btc as InBtc, Usd};
    for (asset, accepted, window, allowed) in [
        (Usd, vec![BTC, USDT], 3_600, true),
        (Usd, vec![BTC, USDT], 3_601, false),
        (Usd, vec![BTC], 3_601, false),
        (InBtc, vec![USDT], 3_601, false),
        (InBtc, vec![BTC, USDT], 3_601, false),
        (Usd, vec![USDT], 86_400, true),
        (Usd, vec![USDT], 86_401, false),
        (InBtc, vec![BTC], 86_400, true),
        (InBtc, vec![BTC], 86_401, false),
        (Usd, vec![BTC, USDT], 1_800, true),
    ] {
        let rig = rig(true, true, true, Some(FEED));
        let amount = if asset == InBtc { 50_000 } else { 500 };
        let result = rig
            .service
            .prepare(multi_request(amount, asset, &accepted, window))
            .await;
        let label = format!("{asset:?} {accepted:?} {window}");
        if allowed {
            result.unwrap_or_else(|error| panic!("{label}: {error:?}"));
        } else {
            assert_eq!(
                result,
                Err(PrepareMarketplaceError::PaymentWindowExceedsCap),
                "{label}"
            );
            assert_eq!(rig.rates.calls.load(Ordering::SeqCst), 0, "{label}");
            assert_eq!(rig.session.calls.load(Ordering::SeqCst), 0, "{label}");
        }
    }
}

#[tokio::test]
async fn unavailable_rate_fails_only_the_combinations_that_cross_bitcoin() {
    use CriterionAsset::{Btc as InBtc, Usd, Usdt as InUsdt};
    for (asset, accepted) in [
        (Usd, vec![BTC]),
        (Usd, vec![BTC, USDT]),
        (InBtc, vec![USDT]),
        (InUsdt, vec![BTC, USDT]),
    ] {
        let rig = rig(true, true, true, None);
        assert_eq!(
            rig.service
                .prepare(multi_request(500, asset, &accepted, 3_600))
                .await,
            Err(PrepareMarketplaceError::Unavailable),
            "{asset:?} {accepted:?}"
        );
        assert_eq!(rig.rates.calls.load(Ordering::SeqCst), 1);
        assert_eq!(rig.store.prepare_calls.load(Ordering::SeqCst), 0);
    }
    for (asset, accepted) in [(Usd, vec![USDT]), (InBtc, vec![BTC]), (InUsdt, vec![USDT])] {
        let rig = rig(true, true, true, None);
        rig.service
            .prepare(multi_request(500, asset, &accepted, 3_600))
            .await
            .unwrap();
        assert_eq!(rig.rates.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn rate_provider_is_not_configured_means_crossing_requests_are_unavailable() {
    let session = Arc::new(FakeSession {
        calls: AtomicUsize::default(),
    });
    let service = PrepareMarketplaceService::new(
        session,
        Arc::new(FakeRegistries {
            calls: AtomicUsize::default(),
        }),
        Arc::new(RailCredentials {
            bitcoin: true,
            usdt: true,
            calls: AtomicUsize::default(),
        }),
        Arc::new(CapturingStore::default()),
        paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        BitcoinNetwork::Mainnet,
        Duration::from_secs(24 * 60 * 60),
        Duration::from_secs(15 * 60),
    )
    .with_usdt(true);
    assert_eq!(
        service
            .prepare(multi_request(500, CriterionAsset::Usd, &[BTC], 3_600))
            .await,
        Err(PrepareMarketplaceError::Unavailable)
    );
}

#[tokio::test]
async fn binding_covers_amount_asset_and_the_accepted_set_but_not_its_order() {
    use CriterionAsset::{Btc as InBtc, Usd};
    let mut bindings = Vec::new();
    for (asset, amount, accepted) in [
        (Usd, 500, vec![BTC, USDT]),
        (Usd, 500, vec![USDT, BTC]),
        (Usd, 500, vec![BTC]),
        (Usd, 500, vec![USDT]),
        (Usd, 501, vec![BTC, USDT]),
        (InBtc, 500, vec![BTC, USDT]),
    ] {
        let rig = rig(true, true, true, Some(FEED));
        let _ = rig
            .service
            .prepare(multi_request(amount, asset, &accepted, 3_600))
            .await;
        bindings.push(rig.store.bindings.lock().unwrap()[0].clone());
    }
    assert_eq!(bindings[0], bindings[1], "set order is not significant");
    for changed in &bindings[2..] {
        assert_ne!(&bindings[0], changed);
    }
    let unique: std::collections::BTreeSet<_> = bindings[2..].iter().collect();
    assert_eq!(unique.len(), 4);
}

#[tokio::test]
async fn empty_duplicate_or_zero_terms_are_invalid_before_any_read() {
    for (amount, accepted) in [
        (500, vec![]),
        (500, vec![BTC, BTC]),
        (500, vec![USDT, USDT, BTC]),
        (0, vec![BTC]),
    ] {
        let rig = rig(true, true, true, Some(FEED));
        assert_eq!(
            rig.service
                .prepare(multi_request(amount, CriterionAsset::Usd, &accepted, 3_600))
                .await,
            Err(PrepareMarketplaceError::InvalidRequest)
        );
        assert!(rig.store.bindings.lock().unwrap().is_empty());
    }
}

fn dual_body(change: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
    let mut value = serde_json::json!({
        "accepted_assets": ["BTC", "USDT"],
        "amount": "500",
        "asset": "USD",
        "creator": CREATOR,
        "operation_id": "marketplace-payment:order-1:attempt-1",
        "payment_window_seconds": 1_800,
        "reader": reader(),
        "reference": REFERENCE,
    });
    change(&mut value);
    serde_json_canonicalizer::to_vec(&value).unwrap()
}

#[tokio::test]
async fn prepare_route_echoes_terms_payable_amounts_and_fixed_rates() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let rig = rig(true, true, true, Some(FEED));
    let router =
        marketplace_preparation_router(Arc::new(rig.service)).layer(Extension(signed_auth(&key)));
    let response = router
        .oneshot(signed_request(&key, dual_body(|_| {})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "invoice_id": "00000000-0000-0000-0000-000000000001",
            "state": "prepared",
            "amount": "500",
            "asset": "USD",
            "accepted_assets": ["BTC", "USDT"],
            "payment_amounts": {"BTC": "6173", "USDT": "5000000"},
            "rates": {"BTC": "0.000012345679012346", "USDT": "1"},
            "prepare_expires_at": "1970-01-01T00:15:00Z",
        })
    );
}

#[tokio::test]
async fn prepare_route_body_is_closed_to_the_old_and_malformed_shapes() {
    let key = SigningKey::from_bytes(&[11; 32]);
    for (label, change) in [
        (
            "amount_sats is the retired field",
            Box::new(|value: &mut serde_json::Value| {
                value["amount_sats"] = serde_json::json!(500);
            }) as Box<dyn FnOnce(&mut serde_json::Value)>,
        ),
        (
            "amount must be a string",
            Box::new(|value| value["amount"] = serde_json::json!(500)),
        ),
        (
            "amount zero",
            Box::new(|value| value["amount"] = serde_json::json!("0")),
        ),
        (
            "amount fractional",
            Box::new(|value| value["amount"] = serde_json::json!("5.00")),
        ),
        (
            "amount overflow",
            Box::new(|value| value["amount"] = serde_json::json!("18446744073709551616")),
        ),
        (
            "asset lowercase",
            Box::new(|value| value["asset"] = serde_json::json!("usd")),
        ),
        (
            "asset unknown",
            Box::new(|value| value["asset"] = serde_json::json!("EUR")),
        ),
        (
            "accepted empty",
            Box::new(|value| value["accepted_assets"] = serde_json::json!([])),
        ),
        (
            "accepted duplicate",
            Box::new(|value| value["accepted_assets"] = serde_json::json!(["BTC", "BTC"])),
        ),
        (
            "accepted denomination is not a payment asset",
            Box::new(|value| value["accepted_assets"] = serde_json::json!(["USD"])),
        ),
        (
            "accepted lowercase",
            Box::new(|value| value["accepted_assets"] = serde_json::json!(["btc"])),
        ),
        (
            "accepted missing",
            Box::new(|value| {
                value.as_object_mut().unwrap().remove("accepted_assets");
            }),
        ),
        (
            "asset missing",
            Box::new(|value| {
                value.as_object_mut().unwrap().remove("asset");
            }),
        ),
    ] {
        let rig = rig(true, true, true, Some(FEED));
        let router = marketplace_preparation_router(Arc::new(rig.service))
            .layer(Extension(signed_auth(&key)));
        let response = router
            .oneshot(signed_request(&key, dual_body(change)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{label}");
    }
}

#[tokio::test]
async fn prepare_route_maps_typed_refusals_to_stable_bodies() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let call = |bitcoin: bool, usdt: bool, usdt_enabled: bool, window: u64| {
        let key = key.clone();
        async move {
            let rig = rig(bitcoin, usdt, usdt_enabled, Some(FEED));
            let router = marketplace_preparation_router(Arc::new(rig.service))
                .layer(Extension(signed_auth(&key)));
            let response = router
                .oneshot(signed_request(
                    &key,
                    dual_body(|value| value["payment_window_seconds"] = window.into()),
                ))
                .await
                .unwrap();
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            )
        }
    };
    for (outcome, status, code, message) in [
        (
            call(true, false, true, 1_800).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "seller_setup_pending",
            "seller USDT receiving setup is needed",
        ),
        (
            call(false, true, true, 1_800).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "seller_setup_pending",
            "seller Bitcoin receiving setup is needed",
        ),
        (
            call(false, false, true, 1_800).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "seller_setup_pending",
            "seller Bitcoin and USDT receiving setup is needed",
        ),
        (
            call(true, true, false, 1_800).await,
            StatusCode::CONFLICT,
            "asset_unsupported",
            "requested payment asset is not supported by this deployment",
        ),
        (
            call(true, true, true, 3_601).await,
            StatusCode::BAD_REQUEST,
            "payment_window_exceeds_cap",
            "payment window exceeds the cap for this request",
        ),
    ]
    .map(|((status, body), expected_status, code, message)| {
        ((status, body), expected_status, code, message)
    }) {
        assert_eq!(outcome.0, status, "{code}");
        assert_eq!(
            outcome.1,
            serde_json::json!({"error":{"code":code,"message":message}})
        );
    }
}
