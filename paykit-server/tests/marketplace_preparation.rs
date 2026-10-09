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
            AppRegistryDiscovery, CreatorReceivingProvider, ReaderAuthorization, ReceivingDetails,
            RegistryDiscoveryError, SessionValidationError, SessionValidator,
        },
        marketplace_preparation::{
            MarketplacePreparationPersistence, PrepareMarketplaceError, PrepareMarketplaceRequest,
            PrepareMarketplaceService,
        },
    },
    config::{BitcoinNetwork, Config, ConfigEnvironment},
    domain::locks::{CreatorPubky, parse_creator, parse_reader},
    http::{
        auth::{SignedServiceAuth, signature_preimage},
        marketplace_preparation::marketplace_preparation_router,
    },
    persistence::{
        MarketplacePreparationInput, MarketplacePreparationPreflight, MarketplacePreparationResult,
        PersistenceError,
    },
};
use tower::ServiceExt;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const REFERENCE: &str = "7cceb26d-9042-4ea6-bfcb-01bbd778d76e";
const PATH: &str = "/marketplace/payment-requests/prepare";

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
        amount_sats: 50_000,
        operation_id: "marketplace-payment:order-1:attempt-1".into(),
        payment_window_seconds: window,
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
    prepare_calls: AtomicUsize,
    captured_reference: Mutex<Option<String>>,
}

impl FakeStore {
    fn new(preflight: MarketplacePreparationPreflight) -> Self {
        Self {
            preflight,
            prepare_calls: AtomicUsize::default(),
            captured_reference: Mutex::new(None),
        }
    }
}

#[async_trait]
impl MarketplacePreparationPersistence for FakeStore {
    async fn preflight(
        &self,
        _creator: &CreatorPubky,
        _operation_id: &str,
        _request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError> {
        Ok(self.preflight)
    }

    async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
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
        }
        Ok(MarketplacePreparationResult::new(
            uuid::Uuid::from_u128(1),
            input.total_sats,
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
            input.total_sats,
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
}

#[tokio::test]
async fn invalid_or_over_cap_window_fails_before_preflight() {
    for window in [0, 86_401] {
        let deps = service(MarketplacePreparationPreflight::New);
        assert_eq!(
            deps.service.prepare(request(window)).await,
            Err(PrepareMarketplaceError::InvalidRequest)
        );
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
        "amount_sats": 50_000,
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
            "total_sats": 50_000,
        })
    );

    let response = router
        .oneshot(signed_request(&key, body(true)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
