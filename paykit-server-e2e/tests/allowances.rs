//! An Allowance a reader proposes on the Paykit Server's own link covers the
//! Locks invoice the server sends next.
//!
//! The reader's Bitkit and the server share one Encrypted Link:
//! `(creator, bitkit/server) <-> (reader, bitkit/wallet)`. Only the server
//! holds the creator's `bitkit/server` Noise key, so only the server can
//! accept an Allowance on that link. This test runs the production server
//! (PostgreSQL, the outbox worker, an ephemeral Pubky testnet) against a
//! reader on the same Allowances SDK, and checks the payer's admission.

use std::{collections::BTreeMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bitcoin::{
    Network,
    bip32::{ChildNumber, Xpriv, Xpub},
    secp256k1::Secp256k1,
};
use ed25519_dalek::{Signer, SigningKey};
use locks_core::{
    ids::CreatorPubky as RawCreatorPubky,
    lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, LockLogic, LockServerConfig,
        VerifierType,
    },
};
use paykit_lib::{
    AllowanceAmountRange, AllowanceId, AllowancePeriod, AllowancePeriodLimit, AllowancePeriodUnit,
    AllowanceTerms, PaykitReceiverCapabilities, PaykitReceiverPath, PaymentAmount,
    PaymentEndpointIdentifier, PaymentRequestId,
};
use paykit_sdk::{
    AllowanceAccountingBlock, AllowanceAccountingReconciliation, AllowanceFilter,
    AllowanceHistoryStatus, AllowanceLifecycleState, AllowanceLocalRole, AllowanceSelectionInput,
    InMemoryStorage, LinkedPeerState, OutboundPrivateMessageStatus, PaykitSdk, PaykitSdkConfig,
    PaymentAttemptDecision, PaymentExecutionChecks, PaymentExecutionMode, PaymentOccurrence,
    PaymentRequestRecord, PaymentRequestScope, PubkyLocalSecretKey, PubkyPublicKey,
    PubkySessionBootstrap, ReceiverNoiseSecretKey, storage::StorageState,
};
use paykit_server::{
    Server,
    bitcoin::{ObservationTarget, ObservedOutput},
    config::{Config, ConfigEnvironment},
    crypto::Crypto,
    domain::locks::{CreatorPubky, ReaderPubky, parse_creator, parse_reader},
    persistence::{CreatorCredentials, CreatorStore, PostgresStorageAdapter, SdkStateStore},
    startup::initialize_database,
    workers::observer::{ElectrumPort, ObserverError},
};
use paykit_server_e2e::postgres::TestDatabase;
use pubky_testnet::{EphemeralTestnet, pubky::Keypair};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[path = "fixtures/sdk.rs"]
mod sdk_fixtures;

use sdk_fixtures::{TestPaymentAdapter, TestSessionProvider};

const MASTER_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const BUNDLE_BEFORE_GRANT: &str = "000G40R40M30E209185GR38E1W";
const BUNDLE_AFTER_GRANT: &str = "000G40R40M30E209185GR38E2W";
/// The $2 locked post, at about $100k per BTC.
const LOCK_SATS: u64 = 2_000;
const SERVER_PATH: &str = "bitkit/server";
const READER_PATH: &str = "bitkit/wallet";
const REGTEST_ONCHAIN: &str = "btc-regtest-p2wpkh";
static PUBKY_TESTNET_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type ReaderSdk = PaykitSdk<InMemoryStorage, TestSessionProvider, TestPaymentAdapter>;

/// No payment is observed in this test.
struct EmptyElectrum;

#[async_trait]
impl ElectrumPort for EmptyElectrum {
    async fn observations(
        &self,
        _targets: &[ObservationTarget],
    ) -> Result<Vec<ObservedOutput>, ObserverError> {
        Ok(Vec::new())
    }
}

/// The server settings that matter here: the `bitkit/server` receiver path
/// and regtest endpoints. The outbox polls fast so handoffs run within the
/// test.
fn config(database_url: &str, signing_key: &SigningKey) -> Config {
    let trusted_key = pubky::PublicKey::from(
        pubky::pkarr::PublicKey::try_from(signing_key.verifying_key().as_bytes()).unwrap(),
    )
    .to_string();
    Config::from_toml_and_environment(
        &format!(
            r#"
[http]
listen_addr = "127.0.0.1:0"
[locks]
trusted_public_key = "{trusted_key}"
[setup]
allowed_origins = ["https://app.example"]
[paykit]
client_id = "app.paykit.server"
receiver_path = "{SERVER_PATH}"
receiver_path_priority = ["bitkit"]
network = "testnet"
[bitcoin]
network = "regtest"
[electrum]
endpoint = "tcp://127.0.0.1:1"
poll_interval = "100ms"
request_timeout = "1s"
connect_retries = 0
[outbox]
poll_interval = "100ms"
batch_size = 16
lease_duration = "5s"
retry_initial = "1s"
retry_max = "2s"
[shutdown]
drain_timeout = "2s"
"#
        ),
        ConfigEnvironment {
            database_url: Some(database_url.to_owned()),
            master_key: Some(MASTER_KEY.to_owned()),
        },
    )
    .unwrap()
}

fn account_xpub(seed: u8) -> String {
    let secp = Secp256k1::new();
    let account = Xpriv::new_master(Network::Regtest, &[seed; 32])
        .unwrap()
        .derive_priv(
            &secp,
            &[
                ChildNumber::from_hardened_idx(84).unwrap(),
                ChildNumber::from_hardened_idx(1).unwrap(),
                ChildNumber::from_hardened_idx(0).unwrap(),
            ],
        )
        .unwrap();
    Xpub::from_priv(&secp, &account).to_string()
}

fn content_lock(creator: &CreatorPubky) -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: RawCreatorPubky::from_str(&creator.to_string()).unwrap(),
        primary_resource: None,
        secondary_resources: BTreeMap::new(),
        criteria: vec![Criterion {
            criterion_id: "payment".into(),
            verifier_type: VerifierType::PaykitPayment,
            params: serde_json::json!({
                "recipient_pubky": creator.to_string(),
                "amount": LOCK_SATS.to_string(),
                "asset": "BTC"
            }),
        }],
        lock_logic: LockLogic::All {
            criteria: vec!["payment".into()],
        },
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

struct Stack {
    address: SocketAddr,
    pool: PgPool,
    crypto: Arc<Crypto>,
    signing_key: SigningKey,
    creator: CreatorPubky,
    creator_key: PubkyPublicKey,
    lock_resource: String,
    reader: ReaderPubky,
    reader_sdk: ReaderSdk,
    _testnet: EphemeralTestnet,
    database: TestDatabase,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    running: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Stack {
    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        self.running.await.unwrap().unwrap();
        self.pool.close().await;
        self.database.cleanup().await;
    }
}

/// Boots the production server with one creator (server-held `bitkit/server`
/// credentials, published marker and content lock) and one reader (a
/// `bitkit/wallet` wallet SDK with its marker published).
async fn boot(seed: u8) -> Stack {
    let database = TestDatabase::create().await;
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let server_config = config(database.database_url(), &signing_key);
    let pool = initialize_database(&server_config).await.unwrap();
    let testnet = EphemeralTestnet::builder()
        .postgres(
            pubky_testnet::pubky_homeserver::ConnectionString::new(
                &std::env::var("TEST_DATABASE_URL").unwrap(),
            )
            .unwrap(),
        )
        .build()
        .await
        .unwrap();
    let pubky = testnet.sdk().unwrap();
    let bootstrap = PubkySessionBootstrap::with_pubky(pubky.clone(), "app.paykit.server").unwrap();
    let homeserver = PubkyPublicKey::from_public_key(&testnet.homeserver_app().public_key());
    let crypto = Arc::new(Crypto::from_master_key(&[1; 32]).unwrap());
    let capabilities = |path: &str| {
        PaykitSdkConfig::new(PaykitReceiverPath::new(path).unwrap()).required_session_capabilities()
    };

    let reader_account = bootstrap
        .sign_up(
            &PubkyLocalSecretKey::new(Keypair::random().secret_key()),
            ReceiverNoiseSecretKey::random(),
            &homeserver,
            None,
            &capabilities(READER_PATH),
        )
        .await
        .unwrap();
    let reader = parse_reader(&format!("pubky{}", reader_account.public_key)).unwrap();
    let reader_sdk = PaykitSdk::new(
        InMemoryStorage::default(),
        TestSessionProvider::new(reader_account.access),
        TestPaymentAdapter,
        PaykitSdkConfig::new(PaykitReceiverPath::new(READER_PATH).unwrap()),
    )
    .unwrap();
    reader_sdk.initialize().await.unwrap();
    reader_sdk
        .publish_paykit_receiver_marker(PaykitReceiverCapabilities {
            private_payments: true,
            payment_requests: true,
            receipts: false,
            outgoing_payments: true,
        })
        .await
        .unwrap();

    let creator_secret = PubkyLocalSecretKey::new(Keypair::random().secret_key());
    let creator_account = bootstrap
        .sign_up(
            &creator_secret,
            ReceiverNoiseSecretKey::random(),
            &homeserver,
            None,
            &capabilities(SERVER_PATH),
        )
        .await
        .unwrap();
    let creator_key = creator_account.public_key.clone();
    let creator = parse_creator(&format!("pubky{creator_key}")).unwrap();
    let lock = content_lock(&creator);
    let lock_path = lock.content_lock_path().unwrap().to_string();
    bootstrap
        .sign_in(
            &creator_secret,
            ReceiverNoiseSecretKey::random(),
            "/pub/locks.app/:rw",
        )
        .await
        .unwrap()
        .access
        .session
        .storage()
        .put_json(lock_path.clone(), &lock)
        .await
        .unwrap();
    let row = CreatorStore::new(&pool, crypto.clone())
        .create(
            &CreatorCredentials::new(
                creator.clone(),
                creator_account
                    .export_session_secret()
                    .await
                    .unwrap()
                    .into_inner(),
                creator_account.access.receiver_noise_secret_key.clone(),
                account_xpub(seed),
                0,
            ),
            &StorageState::default(),
        )
        .await
        .unwrap();
    // Setup completion publishes the creator's marker with the stored Noise
    // key; do the same through the creator's own SDK state.
    let creator_sdk = PaykitSdk::new(
        PostgresStorageAdapter::new(&pool, crypto.clone(), row.id()),
        TestSessionProvider::new(creator_account.access),
        TestPaymentAdapter,
        PaykitSdkConfig::new(PaykitReceiverPath::new(SERVER_PATH).unwrap()),
    )
    .unwrap();
    creator_sdk.initialize().await.unwrap();
    creator_sdk
        .publish_paykit_receiver_marker(PaykitReceiverCapabilities {
            private_payments: true,
            payment_requests: true,
            receipts: false,
            outgoing_payments: false,
        })
        .await
        .unwrap();

    let server =
        Server::build_with_transports(server_config, pool.clone(), pubky, Arc::new(EmptyElectrum))
            .await
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(server.run_until(listener, async move {
        let _ = shutdown_rx.await;
    }));
    wait_until_ready(address).await;

    Stack {
        address,
        pool,
        crypto,
        signing_key,
        lock_resource: format!("{creator}{lock_path}"),
        creator,
        creator_key,
        reader,
        reader_sdk,
        _testnet: testnet,
        database,
        shutdown_tx,
        running,
    }
}

async fn send_http(address: SocketAddr, request: Request<Body>) -> (StatusCode, String) {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 32 * 1024).await.unwrap();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
        parts.method,
        parts.uri,
        address,
        body.len()
    );
    for (name, value) in &parts.headers {
        head.push_str(&format!("{}: {}\r\n", name, value.to_str().unwrap()));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response).into_owned();
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, response)
}

async fn wait_until_ready(address: SocketAddr) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            let request = Request::builder()
                .method(Method::GET)
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap();
            if send_http(address, request).await.0 == StatusCode::OK {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// What the Lock Server sends when Pubky App opens the unlock:
/// `{bundle_id, lock_resource, reader}`, signed.
async fn post_locks_invoice(stack: &Stack, bundle: &str) {
    let body = format!(
        r#"{{"bundle_id":"{bundle}","lock_resource":"{}","reader":"{}"}}"#,
        stack.lock_resource, stack.reader
    );
    let request = Request::builder()
        .method(Method::POST)
        .uri("/invoices")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(stack.signing_key.sign(body.as_bytes()).to_bytes()),
        )
        .body(Body::from(body))
        .unwrap();
    let (status, response) = send_http(stack.address, request).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "Locks invoice: {response}");
}

/// The reader's Bitkit: keeps its side of the handshake moving, reads the
/// link and returns the request for `bundle` once it has arrived.
async fn wait_for_request(stack: &Stack, bundle: &str) -> PaymentRequestRecord {
    let server_path = PaykitReceiverPath::new(SERVER_PATH).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let state = stack
            .reader_sdk
            .ensure_link_with_peer(stack.creator_key.clone(), server_path.clone(), 1)
            .await
            .map(|report| report.state);
        if matches!(state, Ok(LinkedPeerState::Linked)) {
            stack
                .reader_sdk
                .receive_private_messages(stack.creator_key.clone(), server_path.clone())
                .await
                .unwrap();
            if let Some(request) = stack
                .reader_sdk
                .payment_requests_with(&stack.creator_key, &server_path)
                .await
                .unwrap()
                .into_iter()
                .find(|request| {
                    request.terms.as_ref().is_some_and(|terms| {
                        terms.metadata.get("bundle_id") == Some(&serde_json::json!(bundle))
                    })
                })
            {
                return request;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let outbox: Vec<(String, i32, Option<String>)> =
                sqlx::query_as("SELECT status, attempt_count, error_class FROM outbox ORDER BY id")
                    .fetch_all(&stack.pool)
                    .await
                    .unwrap();
            panic!("request for {bundle} never arrived: link={state:?} outbox={outbox:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The reader's grant: a per-payment maximum and $50 a month, on-chain
/// regtest allowed.
fn allowance_terms(per_payment_maximum: &str) -> AllowanceTerms {
    AllowanceTerms::builder("btc")
        .per_payment_amount(AllowanceAmountRange::new("0.00000001", per_payment_maximum).unwrap())
        .period_limits(vec![
            AllowancePeriodLimit::new(
                Some("0.00050000".into()),
                None,
                AllowancePeriod::anchored(1, AllowancePeriodUnit::Month, "2026-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
        ])
        .allowed_payment_endpoint_identifiers(vec![
            PaymentEndpointIdentifier::new("btc-lightning-bolt11").unwrap(),
            PaymentEndpointIdentifier::new(REGTEST_ONCHAIN).unwrap(),
        ])
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn allowance_proposed_on_the_server_link_covers_the_next_locks_invoice() {
    let _testnet_guard = PUBKY_TESTNET_LOCK.lock().await;
    let stack = boot(71).await;
    let server_path = PaykitReceiverPath::new(SERVER_PATH).unwrap();
    let reader = &stack.reader_sdk;

    // An earlier unlock creates the server link. No Allowance exists yet, and
    // the server's intake on an empty link changes nothing.
    post_locks_invoice(&stack, BUNDLE_BEFORE_GRANT).await;
    let before = wait_for_request(&stack, BUNDLE_BEFORE_GRANT).await;
    assert_eq!(before.counterparty_receiver_path, server_path);
    assert!(
        reader
            .list_allowances(AllowanceFilter::default())
            .await
            .unwrap()
            .is_empty()
    );

    // The reader grants on the creator's server link: one grant covers the
    // lock price, the other's per-payment maximum is one sat below it.
    let covering = reader
        .propose_allowance(
            stack.creator_key.clone(),
            server_path.clone(),
            AllowanceLocalRole::Allower,
            allowance_terms("0.00005000"),
        )
        .await
        .unwrap();
    let tight = reader
        .propose_allowance(
            stack.creator_key.clone(),
            server_path.clone(),
            AllowanceLocalRole::Allower,
            allowance_terms("0.00001999"),
        )
        .await
        .unwrap();
    let sent = reader
        .process_outbound_private_messages(stack.creator_key.clone(), server_path.clone())
        .await
        .unwrap();
    assert_eq!(sent.sent.len(), 2, "both proposals reach the server link");

    // The next unlock: the server accepts both during the handoff, before it
    // proposes the request.
    post_locks_invoice(&stack, BUNDLE_AFTER_GRANT).await;
    let request = wait_for_request(&stack, BUNDLE_AFTER_GRANT).await;
    let allowances = reader
        .list_allowances(AllowanceFilter {
            counterparty: Some(stack.creator_key.clone()),
            counterparty_receiver_path: Some(server_path.clone()),
            local_role: Some(AllowanceLocalRole::Allower),
            states: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(allowances.len(), 2);
    for allowance in &allowances {
        assert_eq!(
            allowance.state,
            AllowanceLifecycleState::Accepted,
            "the server did not accept {}",
            allowance.allowance_id
        );
        assert_eq!(allowance.history_status, AllowanceHistoryStatus::Consistent);
        assert!(allowance.acceptance_event_id.is_some());
    }

    // The request is the lock price on the regtest on-chain endpoint.
    let terms = request.terms.clone().unwrap();
    assert_eq!(terms.amount.asset, "btc");
    assert_eq!(terms.amount.value, "0.00002000");
    assert_eq!(
        terms.accepted_payment_endpoint_identifiers,
        [REGTEST_ONCHAIN]
    );

    // The reader's wallet admission, as Bitkit runs it before paying.
    let now = chrono::Utc::now();
    reader
        .reconcile_allowance_accounting(AllowanceAccountingReconciliation {
            expected_revision: None,
            history: Default::default(),
            outcomes: Vec::new(),
            trusted_time: now,
        })
        .await
        .unwrap();
    let scope = PaymentRequestScope {
        counterparty: stack.creator_key.clone(),
        counterparty_receiver_path: server_path.clone(),
        payment_request_id: PaymentRequestId::new(request.payment_request_id.clone()).unwrap(),
    };
    let candidates = reader
        .evaluate_allowance_candidates(scope.clone(), now)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 2);
    let covering_candidate = candidates
        .iter()
        .find(|candidate| candidate.allowance_id == covering.allowance_id)
        .unwrap();
    assert_eq!(covering_candidate.blocked, None);
    assert_eq!(
        covering_candidate.eligible_payment_endpoint_identifiers,
        [REGTEST_ONCHAIN]
    );
    let tight_candidate = candidates
        .iter()
        .find(|candidate| candidate.allowance_id == tight.allowance_id)
        .unwrap();
    assert_eq!(
        tight_candidate.blocked,
        Some(AllowanceAccountingBlock::SharedRule {
            code: "amount_outside_range".into()
        })
    );

    let checks = PaymentExecutionChecks {
        trusted_time: now,
        payment_endpoint_identifier: PaymentEndpointIdentifier::new(REGTEST_ONCHAIN).unwrap(),
        actual_amount: PaymentAmount::new(terms.amount.value.clone(), "btc").unwrap(),
        endpoint_current: true,
        local_enabled: true,
        recurrence_eligible: true,
    };
    let association = reader
        .accept_payment_request_automatically(
            scope.clone(),
            AllowanceSelectionInput {
                allowance_id: AllowanceId::new(covering.allowance_id.clone()).unwrap(),
                expected_revision: None,
                trusted_time: now,
            },
            checks.clone(),
        )
        .await
        .unwrap();
    let decision = reader
        .reserve_automatic_payment(
            PaymentOccurrence {
                request: scope,
                billing_period: None,
            },
            association.revisions.last().unwrap().revision,
            checks,
        )
        .await
        .unwrap();
    let PaymentAttemptDecision::Ready { attempt } = decision else {
        panic!("the Allowance must admit the Locks payment");
    };
    assert_eq!(attempt.mode, PaymentExecutionMode::Automatic);
    assert_eq!(
        attempt.allowance_id.as_deref(),
        Some(covering.allowance_id.as_str())
    );
    assert_eq!(attempt.amount.value, terms.amount.value);

    // On the server, both acceptances were sent on the reader link ahead of
    // the second request, and the server keeps no Allowance accounting.
    let state = SdkStateStore::new(&stack.pool, stack.crypto.clone())
        .load(&stack.creator)
        .await
        .unwrap();
    assert!(state.allowance_accounting.is_none());
    let outbound = |kind: &str| {
        state
            .outbound_private_messages
            .iter()
            .filter(|record| record.kind == kind)
            .map(|record| (record.outbound_message_id, record.status.clone()))
            .collect::<Vec<_>>()
    };
    let acceptances = outbound("paykit.allowance_acceptance");
    assert_eq!(acceptances.len(), 2);
    assert!(
        acceptances
            .iter()
            .all(|(_, status)| *status == OutboundPrivateMessageStatus::Sent)
    );
    let requests = outbound("paykit.payment_request");
    assert_eq!(requests.len(), 2);
    assert!(acceptances.iter().all(|(id, _)| *id < requests[1].0));

    stack.shutdown().await;
}
