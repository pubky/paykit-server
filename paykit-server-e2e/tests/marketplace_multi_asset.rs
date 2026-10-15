//! Marketplace requests that carry every accepted asset, over the signed routes and
//! the real services on PostgreSQL. Only the BTC/USD feed and the chain are doubles.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Extension, Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bitcoin::{OutPoint, Txid};
use ed25519_dalek::{Signer, SigningKey};
use paykit_sdk::PaykitIdentitySecretKey;
use paykit_server::{
    application::{
        create_invoice::{
            AppRegistryDiscovery, CreateInvoiceError, CreatorReceivingProvider,
            ReaderAuthorization, RegistryDiscoveryError, SessionValidationError, SessionValidator,
        },
        invoice_pricing::ExchangeRates,
        marketplace_lifecycle::MarketplaceLifecycleService,
        marketplace_preparation::PrepareMarketplaceService,
        marketplace_status::MarketplaceStatusService,
    },
    bitcoin::{ObservationTarget, ObservedOutput},
    config::{BitcoinNetwork, Config, ConfigEnvironment},
    crypto::{Crypto, EncryptedEnvelope, EnvelopeContext},
    domain::locks::{CreatorPubky, ReaderPubky, parse_creator, parse_reader},
    http::{
        auth::{SignedServiceAuth, signature_preimage},
        marketplace_lifecycle::marketplace_lifecycle_router,
        marketplace_preparation::marketplace_preparation_router,
        marketplace_status::marketplace_status_router,
    },
    persistence::{
        CreatorCredentials, CreatorStore, MarketplacePreparationStore, OutboxStore, run_migrations,
    },
    workers::observer::{ElectrumPort, ObserverError, observe_marketplace_once_at},
};
use paykit_server_e2e::postgres::TestDatabase;
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tower::ServiceExt;

mod common;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const USDT_ADDRESS: &str = "0x1111111111111111111111111111111111111111";
const REFERENCE: &str = "7cceb26d-9042-4ea6-bfcb-01bbd778d76e";
const FIXTURES: &str = "marketplace-payment-request-prepare-activate";
const PREPARE: &str = "/marketplace/payment-requests/prepare";
const ACTIVATE: &str = "/marketplace/payment-requests/activate";
const STATUS: &str = "/marketplace/payment-requests/status";
const VOLATILE: [(&str, &str); 4] = [
    ("invoice_id", "4c6e66c7-8a8b-4d54-b97b-340cedb3e1d0"),
    ("prepare_expires_at", "2026-10-23T09:15:00Z"),
    ("activated_at", "2026-10-23T09:05:00Z"),
    ("payment_deadline", "2026-10-23T09:35:00Z"),
];

fn creator() -> CreatorPubky {
    parse_creator(CREATOR).unwrap()
}

fn reader() -> ReaderPubky {
    for replacement in "ybndrfg8ejkmcpqxot1uwisza345h769".chars() {
        let mut candidate = CREATOR.to_owned();
        candidate.replace_range(5..6, &replacement.to_string());
        if let Ok(reader) = parse_reader(&candidate) {
            return reader;
        }
    }
    panic!("valid reader fixture")
}

fn crypto() -> Arc<Crypto> {
    Arc::new(Crypto::from_master_key(&[7; 32]).unwrap())
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

struct ReadySession;

#[async_trait]
impl SessionValidator for ReadySession {
    async fn validate(&self, _creator: &CreatorPubky) -> Result<(), SessionValidationError> {
        Ok(())
    }
}

struct ReadyRegistry;

#[async_trait]
impl AppRegistryDiscovery for ReadyRegistry {
    async fn discover(
        &self,
        _reader: &ReaderPubky,
    ) -> Result<Option<paykit_lib::PaykitAppRegistry>, RegistryDiscoveryError> {
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
        Ok(Some(registry))
    }

    async fn authorization(
        &self,
        _reader: &ReaderPubky,
    ) -> Result<ReaderAuthorization, RegistryDiscoveryError> {
        Ok(ReaderAuthorization::Verified)
    }
}

/// BTC/USD feed double. `failing` makes every read fail, like a stale or down feed.
struct Feed {
    price: &'static str,
    failing: AtomicBool,
    calls: AtomicUsize,
}

#[async_trait]
impl ExchangeRates for Feed {
    async fn usd_per_btc(&self) -> Result<String, CreateInvoiceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            return Err(CreateInvoiceError::Unavailable);
        }
        Ok(self.price.to_owned())
    }
}

struct Rig {
    database: TestDatabase,
    store: Arc<MarketplacePreparationStore>,
    router: Router,
    key: SigningKey,
    feed: Arc<Feed>,
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

#[derive(Clone, Copy)]
struct Seller {
    bitcoin: bool,
    usdt: bool,
}

const BOTH_RAILS: Seller = Seller {
    bitcoin: true,
    usdt: true,
};

async fn rig(seller: Seller, usdt_enabled: bool) -> Rig {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let creators = CreatorStore::new(database.pool(), crypto());
    creators
        .create(&CreatorCredentials::new(
            creator(),
            "session-secret".into(),
            PaykitIdentitySecretKey::new([9; 32], 1).unwrap(),
            seller
                .bitcoin
                .then(|| paykit_server::domain::receiving::BitcoinAccount {
                    xpub: account_xpub().into(),
                    account_index: 0,
                }),
            seller
                .usdt
                .then(|| USDT_ADDRESS.to_owned().try_into().unwrap()),
        ))
        .await
        .unwrap();
    let store = Arc::new(MarketplacePreparationStore::new(database.pool(), crypto()));
    let feed = Arc::new(Feed {
        price: "81000",
        failing: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let credentials: Arc<dyn CreatorReceivingProvider> = Arc::new(creators);
    let preparation = Arc::new(
        PrepareMarketplaceService::new(
            Arc::new(ReadySession),
            Arc::new(ReadyRegistry),
            credentials,
            store.clone(),
            paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
            BitcoinNetwork::Mainnet,
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(15 * 60),
        )
        .with_usdt(usdt_enabled)
        .with_conversion_payment_window(Duration::from_secs(60 * 60))
        .with_exchange_rates(feed.clone()),
    );
    let lifecycle = Arc::new(MarketplaceLifecycleService::new(
        store.clone(),
        Duration::from_secs(30 * 60),
    ));
    let status = Arc::new(MarketplaceStatusService::new(store.clone()));
    let key = SigningKey::from_bytes(&[11; 32]);
    let router = marketplace_preparation_router(preparation)
        .merge(marketplace_lifecycle_router(lifecycle))
        .merge(marketplace_status_router(status))
        .layer(Extension(signed_auth(&key)));
    Rig {
        database,
        store,
        router,
        key,
        feed,
    }
}

impl Rig {
    async fn post(&self, path: &str, value: serde_json::Value) -> (StatusCode, Vec<u8>) {
        let body = serde_json_canonicalizer::to_vec(&value).unwrap();
        let preimage = signature_preimage("POST", path, &body);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header(
                        "X-Paykit-Signature",
                        URL_SAFE_NO_PAD.encode(self.key.sign(&preimage).to_bytes()),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        (
            status,
            axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
    }

    async fn prepare(
        &self,
        operation: &str,
        amount: &str,
        asset: &str,
        accepted: &[&str],
        window: u64,
    ) -> (StatusCode, Vec<u8>) {
        self.post(
            PREPARE,
            serde_json::json!({
                "creator": CREATOR,
                "reader": reader().to_string(),
                "reference": REFERENCE,
                "operation_id": operation,
                "amount": amount,
                "asset": asset,
                "accepted_assets": accepted,
                "payment_window_seconds": window,
            }),
        )
        .await
    }

    async fn activate(&self, invoice_id: &str, amount: &str, asset: &str) -> (StatusCode, Vec<u8>) {
        self.post(
            ACTIVATE,
            serde_json::json!({
                "creator": CREATOR,
                "invoice_id": invoice_id,
                "amount": amount,
                "asset": asset,
            }),
        )
        .await
    }

    async fn status(&self, invoice_id: &str) -> serde_json::Value {
        let (status, body) = self
            .post(
                STATUS,
                serde_json::json!({"creator": CREATOR, "invoice_id": invoice_id}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        serde_json::from_slice(&body).unwrap()
    }

    async fn cleanup(self) {
        self.database.cleanup().await;
    }
}

fn json(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body).unwrap()
}

fn invoice_id(body: &[u8]) -> String {
    json(body)["invoice_id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn dual_asset_prepare_and_activate_round_trip_with_replay_and_binding() {
    let rig = rig(BOTH_RAILS, true).await;
    let (status, prepared) = rig
        .prepare("order-1:attempt-1", "500", "USD", &["USDT", "BTC"], 1_800)
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&prepared)
    );
    common::assert_fixture(FIXTURES, "prepare-usd-btc-usdt", &prepared, &VOLATILE);
    let expected_terms = serde_json::json!({
        "amount": "500",
        "asset": "USD",
        "accepted_assets": ["BTC", "USDT"],
        "payment_amounts": {"BTC": "6173", "USDT": "5000000"},
        "rates": {"BTC": "0.000012345679012346", "USDT": "1"},
    });
    for key in expected_terms.as_object().unwrap().keys() {
        assert_eq!(json(&prepared)[key], expected_terms[key], "{key}");
    }
    let invoice = invoice_id(&prepared);
    assert_eq!(rig.feed.calls.load(Ordering::SeqCst), 1);

    // An exact retry replays the stored quote without reading the feed again, even
    // while it is down, and the set order does not matter.
    rig.feed.failing.store(true, Ordering::SeqCst);
    let (status, replay) = rig
        .prepare("order-1:attempt-1", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&replay), json(&prepared));
    assert_eq!(rig.feed.calls.load(Ordering::SeqCst), 1);

    // Any change under the same operation id is a binding conflict.
    for (label, amount, asset, accepted, window) in [
        ("amount", "501", "USD", vec!["BTC", "USDT"], 1_800),
        ("asset", "500", "USDT", vec!["BTC", "USDT"], 1_800),
        ("accepted set", "500", "USD", vec!["BTC"], 1_800),
        ("window", "500", "USD", vec!["BTC", "USDT"], 900),
    ] {
        let (status, body) = rig
            .prepare("order-1:attempt-1", amount, asset, &accepted, window)
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{label}");
        assert_eq!(
            json(&body)["error"]["code"],
            "operation_conflict",
            "{label}"
        );
    }
    let (status, body) = rig
        .prepare("order-1:attempt-1", "501", "USD", &["BTC", "USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    common::assert_fixture(FIXTURES, "operation-conflict", &body, &VOLATILE);

    let (status, activated) = rig.activate(&invoice, "500", "USD").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&activated)
    );
    common::assert_fixture(FIXTURES, "activate-usd-btc-usdt", &activated, &VOLATILE);
    assert_eq!(
        json(&activated)["payment_amounts"],
        expected_terms["payment_amounts"]
    );
    assert_eq!(
        json(&activated)["accepted_assets"],
        expected_terms["accepted_assets"]
    );
    let (status, replay) = rig.activate(&invoice, "500", "USD").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&replay), json(&activated));

    for (label, amount, asset) in [
        ("amount", "501", "USD"),
        ("asset", "500", "USDT"),
        ("sats total", "6173", "BTC"),
    ] {
        let (status, body) = rig.activate(&invoice, amount, asset).await;
        assert_eq!(status, StatusCode::CONFLICT, "{label}");
        assert_eq!(json(&body)["error"]["code"], "total_mismatch", "{label}");
    }
    let (_, body) = rig.activate(&invoice, "501", "USD").await;
    common::assert_fixture(FIXTURES, "total-mismatch", &body, &VOLATILE);
    rig.cleanup().await;
}

#[tokio::test]
async fn every_denomination_and_accepted_set_prepares_over_the_signed_route() {
    let rig = rig(BOTH_RAILS, true).await;
    for (asset, amount, name) in [
        ("USD", "500", "usd"),
        ("BTC", "50000", "btc"),
        ("USDT", "5000000", "usdt"),
    ] {
        for (accepted, suffix) in [
            (vec!["BTC"], "btc"),
            (vec!["USDT"], "usdt"),
            (vec!["BTC", "USDT"], "btc-usdt"),
        ] {
            let (status, body) = rig
                .prepare(&format!("{name}-{suffix}"), amount, asset, &accepted, 1_800)
                .await;
            assert_eq!(status, StatusCode::OK, "{asset} {accepted:?}");
            let prepared = json(&body);
            assert_eq!(prepared["asset"], asset);
            assert_eq!(prepared["amount"], amount);
            assert_eq!(prepared["accepted_assets"], serde_json::json!(accepted));
            let payable = prepared["payment_amounts"].as_object().unwrap();
            assert_eq!(payable.len(), accepted.len());
            for rail in &accepted {
                assert!(payable[*rail].as_str().unwrap().parse::<u64>().unwrap() > 0);
            }
            common::assert_fixture(
                FIXTURES,
                &format!("prepare-{name}-{suffix}"),
                &body,
                &VOLATILE,
            );
        }
    }
    rig.cleanup().await;
}

#[tokio::test]
async fn proposal_carries_exactly_the_accepted_rails_and_the_fixed_rates() {
    let rig = rig(BOTH_RAILS, true).await;
    for (operation, asset, amount, accepted) in [
        ("dual", "USD", "500", vec!["BTC", "USDT"]),
        ("btc-only", "BTC", "50000", vec!["BTC"]),
        ("usdt-only", "USD", "500", vec!["USDT"]),
    ] {
        let (status, prepared) = rig
            .prepare(operation, amount, asset, &accepted, 1_800)
            .await;
        assert_eq!(status, StatusCode::OK, "{operation}");
        let (status, _) = rig.activate(&invoice_id(&prepared), amount, asset).await;
        assert_eq!(status, StatusCode::OK, "{operation}");
    }
    let outbox = OutboxStore::new(rig.database.pool(), crypto());
    let claims = outbox
        .claim(uuid::Uuid::new_v4(), 3, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(claims.len(), 3);
    let mut seen = Vec::new();
    for claim in &claims {
        let intent = outbox.delivery_intent(claim).unwrap();
        let terms = intent.terms().unwrap();
        let mut endpoints: Vec<_> = terms.payment_endpoints.keys().cloned().collect();
        endpoints.sort();
        let mut rates: Vec<_> = terms.rates.iter().map(|rate| rate.asset.clone()).collect();
        rates.sort();
        seen.push((terms.asset.clone(), endpoints, rates));
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            (
                "btc".to_owned(),
                vec!["btc-bitcoin-p2wpkh".to_owned()],
                Vec::<String>::new()
            ),
            (
                "usd".to_owned(),
                vec![
                    "btc-bitcoin-p2wpkh".to_owned(),
                    "usdt-arbitrum-address".to_owned()
                ],
                vec!["btc".to_owned(), "usdt".to_owned()]
            ),
            (
                "usd".to_owned(),
                vec!["usdt-arbitrum-address".to_owned()],
                vec!["usdt".to_owned()]
            ),
        ]
    );
    rig.cleanup().await;
}

#[tokio::test]
async fn rate_is_fixed_at_prepare_and_activation_never_refetches_it() {
    let rig = rig(BOTH_RAILS, true).await;
    let (status, prepared) = rig
        .prepare("fixed-rate", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rig.feed.calls.load(Ordering::SeqCst), 1);

    rig.feed.failing.store(true, Ordering::SeqCst);
    let (status, activated) = rig.activate(&invoice_id(&prepared), "500", "USD").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&activated)
    );
    assert_eq!(
        json(&activated)["payment_amounts"],
        json(&prepared)["payment_amounts"]
    );
    assert_eq!(
        rig.feed.calls.load(Ordering::SeqCst),
        1,
        "activation must not read the feed"
    );

    let proposal = OutboxStore::new(rig.database.pool(), crypto());
    let claims = proposal
        .claim(uuid::Uuid::new_v4(), 1, Duration::from_secs(30))
        .await
        .unwrap();
    let intent = proposal.delivery_intent(&claims[0]).unwrap();
    assert_eq!(
        intent
            .terms()
            .unwrap()
            .rates
            .iter()
            .map(|rate| (rate.asset.as_str(), rate.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("btc", "0.000012345679012346"), ("usdt", "1")]
    );
    rig.cleanup().await;
}

#[tokio::test]
async fn unavailable_or_stale_rate_at_prepare_persists_nothing_and_keeps_the_address_index() {
    let rig = rig(BOTH_RAILS, true).await;
    rig.feed.failing.store(true, Ordering::SeqCst);
    let (status, body) = rig
        .prepare("stale", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json(&body)["error"]["code"], "dependency_unavailable");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM marketplace_payment_preparations")
        .fetch_one(rig.database.pool())
        .await
        .unwrap();
    let next_index: i64 = sqlx::query_scalar("SELECT next_child_index FROM creators")
        .fetch_one(rig.database.pool())
        .await
        .unwrap();
    assert_eq!((rows, next_index), (0, 0));

    // Combinations that do not cross Bitcoin never need the feed.
    let (status, _) = rig.prepare("parity", "500", "USD", &["USDT"], 1_800).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rig.feed.calls.load(Ordering::SeqCst), 1);
    rig.cleanup().await;
}

#[tokio::test]
async fn typed_refusals_persist_nothing_and_have_stable_bodies() {
    let usdt_less = rig(
        Seller {
            bitcoin: true,
            usdt: false,
        },
        true,
    )
    .await;
    let (status, body) = usdt_less
        .prepare("no-usdt", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    common::assert_fixture(FIXTURES, "seller-setup-pending-usdt", &body, &VOLATILE);
    let (status, body) = usdt_less
        .prepare("window", "500", "USD", &["BTC", "USDT"], 3_601)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    common::assert_fixture(FIXTURES, "payment-window-exceeds-cap", &body, &VOLATILE);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM marketplace_payment_preparations")
        .fetch_one(usdt_less.database.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    usdt_less.cleanup().await;

    let unsupported = rig(BOTH_RAILS, false).await;
    let (status, body) = unsupported
        .prepare("unsupported", "500", "USD", &["USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    common::assert_fixture(FIXTURES, "asset-unsupported", &body, &VOLATILE);
    unsupported.cleanup().await;

    let bitcoin_less = rig(
        Seller {
            bitcoin: false,
            usdt: true,
        },
        true,
    )
    .await;
    let (status, body) = bitcoin_less
        .prepare("no-bitcoin", "500", "USD", &["BTC"], 1_800)
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(&body)["error"]["message"],
        "seller Bitcoin receiving setup is needed"
    );
    common::assert_fixture(FIXTURES, "seller-setup-pending-bitcoin", &body, &VOLATILE);
    bitcoin_less.cleanup().await;

    let neither = rig(
        Seller {
            bitcoin: false,
            usdt: false,
        },
        true,
    )
    .await;
    let (status, body) = neither
        .prepare("no-rails", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    common::assert_fixture(FIXTURES, "seller-setup-pending-both", &body, &VOLATILE);
    neither.cleanup().await;
}

#[tokio::test]
async fn usdt_only_request_activates_without_a_bitcoin_address_and_is_never_watched() {
    let rig = rig(
        Seller {
            bitcoin: false,
            usdt: true,
        },
        true,
    )
    .await;
    let mut invoices = Vec::new();
    for operation in ["usdt-1", "usdt-2"] {
        let (status, prepared) = rig.prepare(operation, "500", "USD", &["USDT"], 1_800).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&prepared)
        );
        if operation == "usdt-1" {
            common::assert_fixture(FIXTURES, "prepare-usd-usdt", &prepared, &VOLATILE);
        }
        let invoice = invoice_id(&prepared);
        let (status, activated) = rig.activate(&invoice, "500", "USD").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&activated)
        );
        invoices.push(invoice);
    }
    let settlements: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(bitcoin_address_lookup_hash) FROM marketplace_settlements",
    )
    .fetch_one(rig.database.pool())
    .await
    .unwrap();
    assert_eq!(settlements, (2, 0));
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM bitcoin_address_owners")
        .fetch_one(rig.database.pool())
        .await
        .unwrap();
    assert_eq!(owners, 0);
    assert!(rig.store.observation_targets().await.unwrap().is_empty());
    rig.store.scan_observation_integrity().await.unwrap();
    for invoice in invoices {
        let status = rig.status(&invoice).await;
        assert_eq!(status["state"], "active");
        assert_eq!(status["payment_state"], "undetected");
        assert!(status["bitcoin"].is_null());
    }
    rig.cleanup().await;
}

#[tokio::test]
async fn one_thousand_eight_hundred_second_window_gives_fifteen_minute_accept_by_and_thirty_minute_deadline()
 {
    let rig = rig(BOTH_RAILS, true).await;
    let (_, prepared) = rig
        .prepare("window", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    let (status, activated) = rig.activate(&invoice_id(&prepared), "500", "USD").await;
    assert_eq!(status, StatusCode::OK);
    let activated_at =
        OffsetDateTime::parse(json(&activated)["activated_at"].as_str().unwrap(), &Rfc3339)
            .unwrap();
    let deadline = OffsetDateTime::parse(
        json(&activated)["payment_deadline"].as_str().unwrap(),
        &Rfc3339,
    )
    .unwrap();
    assert_eq!(deadline - activated_at, time::Duration::minutes(30));
    let outbox = OutboxStore::new(rig.database.pool(), crypto());
    let claims = outbox
        .claim(uuid::Uuid::new_v4(), 1, Duration::from_secs(30))
        .await
        .unwrap();
    let terms = outbox
        .delivery_intent(&claims[0])
        .unwrap()
        .terms()
        .unwrap()
        .clone();
    let accept_by =
        OffsetDateTime::parse(terms.proposal_expires_at.as_deref().unwrap(), &Rfc3339).unwrap();
    assert_eq!(accept_by - activated_at, time::Duration::minutes(15));
    assert_eq!(
        OffsetDateTime::parse(terms.payment_deadline.as_deref().unwrap(), &Rfc3339).unwrap(),
        deadline
    );
    rig.cleanup().await;
}

struct Batch(Vec<ObservedOutput>);

#[async_trait]
impl ElectrumPort for Batch {
    async fn observations(
        &self,
        _targets: &[ObservationTarget],
    ) -> Result<Vec<ObservedOutput>, ObserverError> {
        Ok(self.0.clone())
    }
}

fn outpoint(byte: u8) -> OutPoint {
    OutPoint::new(
        Txid::from_str(&format!("{byte:02x}{}", "00".repeat(31))).unwrap(),
        u32::from(byte),
    )
}

#[tokio::test]
async fn bitcoin_payment_on_a_dual_asset_request_matches_the_converted_amount() {
    let rig = rig(BOTH_RAILS, true).await;
    let (_, prepared) = rig
        .prepare("btc-on-dual", "500", "USD", &["BTC", "USDT"], 1_800)
        .await;
    let invoice = invoice_id(&prepared);
    rig.activate(&invoice, "500", "USD").await;
    let observe = |sats: u64, confirmations: u32, byte: u8| {
        let store = rig.store.clone();
        async move {
            let at = OffsetDateTime::now_utc();
            let targets = store.observation_targets_at(at).await.unwrap();
            assert_eq!(targets.len(), 1);
            let output = ObservedOutput {
                network: BitcoinNetwork::Mainnet,
                address: targets[0].address().to_owned(),
                outpoint: outpoint(byte),
                sats,
                confirmations,
                present: true,
            };
            observe_marketplace_once_at(
                &Batch(vec![output]),
                &store,
                &BitcoinNetwork::Mainnet,
                &targets,
                at,
            )
            .await
            .unwrap();
        }
    };

    observe(6_172, 1, 1).await;
    let underpaid = rig.status(&invoice).await;
    assert_eq!(underpaid["bitcoin"]["observed_sats"], 6_172);
    assert_eq!(underpaid["bitcoin"]["amount_matched"], false);
    assert_eq!(underpaid["bitcoin"]["paid_on_time"], false);

    observe(6_173, 2, 2).await;
    let paid = rig.status(&invoice).await;
    assert_eq!(paid["bitcoin"]["observed_sats"], 6_173);
    assert_eq!(paid["bitcoin"]["confirmations"], 2);
    assert_eq!(paid["bitcoin"]["amount_matched"], true);
    assert_eq!(paid["bitcoin"]["paid_on_time"], true);
    assert_eq!(paid["payment_state"], "confirmed");
    rig.cleanup().await;
}

#[derive(Serialize, Deserialize)]
struct StoredLegacyEnvelope {
    version: u8,
    operation_id: String,
    request_binding: Vec<u8>,
    reader: String,
    reference: String,
    total_sats: u64,
    payment_window_seconds: u64,
    prepare_ttl_seconds: u64,
    prepared_at: OffsetDateTime,
    prepare_expires_at: OffsetDateTime,
    child_index: i64,
    payment_request_intent: paykit_server::application::semantic_intent::DeliveryIntentV1,
}

#[derive(Deserialize)]
struct StoredCurrentEnvelope {
    _version: u8,
    operation_id: String,
    request_binding: Vec<u8>,
    reader: String,
    reference: String,
    _terms: paykit_server::application::marketplace_terms::MarketplaceTerms,
    payment_window_seconds: u64,
    prepare_ttl_seconds: u64,
    prepared_at: OffsetDateTime,
    prepare_expires_at: OffsetDateTime,
    child_index: i64,
    payment_request_intent: paykit_server::application::semantic_intent::DeliveryIntentV1,
}

#[tokio::test]
async fn rc16_preparation_rows_still_activate_and_report_status_as_bitcoin() {
    let rig = rig(BOTH_RAILS, true).await;
    let (_, prepared) = rig.prepare("legacy", "50000", "BTC", &["BTC"], 3_600).await;
    let invoice = invoice_id(&prepared);
    let invoice_uuid = uuid::Uuid::parse_str(&invoice).unwrap();

    // Rewrite the stored envelope as rc11-rc16 wrote it: version 1, sats total.
    let creator_hash = crypto().lookup_hash(creator().to_string().as_bytes());
    let context = EnvelopeContext::marketplace_preparation(creator_hash, invoice_uuid);
    let encrypted: Vec<u8> = sqlx::query_scalar(
        "SELECT preparation_envelope FROM marketplace_payment_preparations WHERE id = $1",
    )
    .bind(invoice_uuid)
    .fetch_one(rig.database.pool())
    .await
    .unwrap();
    let current: StoredCurrentEnvelope = postcard::from_bytes(
        &crypto()
            .decrypt(&context, &EncryptedEnvelope::from_bytes(encrypted))
            .unwrap(),
    )
    .unwrap_or_else(|_| panic!("current envelope layout"));
    let legacy = StoredLegacyEnvelope {
        version: 1,
        operation_id: current.operation_id,
        request_binding: current.request_binding,
        reader: current.reader,
        reference: current.reference,
        total_sats: 50_000,
        payment_window_seconds: current.payment_window_seconds,
        prepare_ttl_seconds: current.prepare_ttl_seconds,
        prepared_at: current.prepared_at,
        prepare_expires_at: current.prepare_expires_at,
        child_index: current.child_index,
        payment_request_intent: current.payment_request_intent,
    };
    let rewritten = crypto()
        .encrypt(&context, &postcard::to_allocvec(&legacy).unwrap())
        .unwrap();
    let rc16_binding = serde_json_canonicalizer::to_vec(&serde_json::json!({
        "amount_sats": 50_000,
        "creator": CREATOR,
        "operation_id": "legacy",
        "payment_window_seconds": 3_600,
        "reader": reader().to_string(),
        "reference": REFERENCE,
    }))
    .unwrap();
    sqlx::query(
        "UPDATE marketplace_payment_preparations
         SET preparation_envelope = $2, request_lookup_hash = $3 WHERE id = $1",
    )
    .bind(invoice_uuid)
    .bind(rewritten.as_bytes())
    .bind(crypto().lookup_hash(&rc16_binding).as_bytes().as_slice())
    .execute(rig.database.pool())
    .await
    .unwrap();

    assert_eq!(rig.status(&invoice).await["state"], "prepared");
    let (status, body) = rig.activate(&invoice, "500", "USD").await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let (status, activated) = rig.activate(&invoice, "50000", "BTC").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&activated)
    );
    let activated = json(&activated);
    assert_eq!(activated["amount"], "50000");
    assert_eq!(activated["asset"], "BTC");
    assert_eq!(activated["accepted_assets"], serde_json::json!(["BTC"]));
    assert_eq!(
        activated["payment_amounts"],
        serde_json::json!({"BTC": "50000"})
    );
    let (status, replay) = rig.activate(&invoice, "50000", "BTC").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&replay), activated);
    assert_eq!(rig.status(&invoice).await["state"], "active");
    assert_eq!(rig.store.observation_targets().await.unwrap().len(), 1);
    rig.store.scan_observation_integrity().await.unwrap();

    // The retired v1 prepare body is rejected, and the new body under the same
    // operation id is a binding conflict, not a silent replay of the rc16 row.
    let (status, body) = rig
        .post(
            PREPARE,
            serde_json::json!({
                "creator": CREATOR,
                "reader": reader().to_string(),
                "reference": REFERENCE,
                "operation_id": "legacy",
                "amount_sats": 50_000,
                "payment_window_seconds": 3_600,
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let (status, body) = rig.prepare("legacy", "50000", "BTC", &["BTC"], 3_600).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(json(&body)["error"]["code"], "operation_conflict");
    rig.cleanup().await;
}
