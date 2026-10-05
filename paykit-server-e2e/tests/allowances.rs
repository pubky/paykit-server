//! An Allowance a reader proposes on the Paykit Server's own link covers the
//! Locks invoice the server sends next.
//!
//! The reader's Bitkit and the server share one identity-wide Encrypted Link:
//! `creator <-> reader`. The server runs the creator's `paykit-server` App on
//! that identity's shared state, so it can accept an Allowance on the link.
//! This test runs the production server (PostgreSQL, the outbox worker, an
//! ephemeral Pubky testnet) against a reader on the same SDK, and checks the
//! payer's admission.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

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
    AllowanceTerms, PaymentAmount, PaymentEndpointIdentifier, PaymentRequestId,
};
use paykit_sdk::{
    AllowanceAccountingBlock, AllowanceAccountingReconciliation, AllowanceFilter,
    AllowanceHistoryStatus, AllowanceLifecycleState, AllowanceLocalRole, AllowanceSelectionInput,
    LinkedPeerState, OutboundPrivateMessageStatus, PAYKIT_SESSION_CAPABILITIES,
    PaymentAttemptDecision, PaymentExecutionChecks, PaymentExecutionMode, PaymentOccurrence,
    PaymentRequestRecord, PaymentRequestScope, PubkyLocalSecretKey, PubkyPublicKey,
    PubkySessionBootstrap,
};
use paykit_server::{
    Server,
    bitcoin::{ObservationTarget, ObservedOutput},
    config::{Config, ConfigEnvironment},
    crypto::Crypto,
    domain::locks::{CreatorPubky, ReaderPubky, parse_creator, parse_reader},
    http::auth::signature_preimage,
    persistence::{CreatorCredentials, CreatorStore},
    startup::initialize_database,
    workers::observer::{ElectrumPort, ObserverError},
};
use paykit_server_e2e::postgres::TestDatabase;
use pubky_testnet::{EphemeralTestnet, pubky::Keypair};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[path = "fixtures/sdk.rs"]
mod sdk_fixtures;

use sdk_fixtures::{HostedSdk, hosted_sdk};

const MASTER_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const BUNDLE_BEFORE_GRANT: &str = "000G40R40M30E209185GR38E1W";
const BUNDLE_AFTER_GRANT: &str = "000G40R40M30E209185GR38E2W";
/// The $2 locked post, at about $100k per BTC.
const LOCK_SATS: u64 = 2_000;
const REGTEST_ONCHAIN: &str = "btc-regtest-p2wpkh";
static PUBKY_TESTNET_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type ReaderSdk = HostedSdk;

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

/// The server settings that matter here: the `paykit-server` App and regtest
/// endpoints. The outbox polls fast so handoffs run within the test. The lease
/// is the production default: a handoff here is a link handshake with the
/// wallet, an intake and a send over an in-process Pubky testnet, and under
/// load it outlasts a short lease, which makes the outbox claim the row again.
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
app_id = "paykit-server"
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
lease_duration = "30s"
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
    signing_key: SigningKey,
    creator_key: PubkyPublicKey,
    /// The creator identity's shared state, read through the same hosted
    /// storage the server writes.
    creator_sdk: HostedSdk,
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

/// Boots the production server with one creator (server-held delegated
/// credentials, published `paykit-server` App and content lock) and one reader
/// (a `bitkit` wallet SDK on its own identity, App published).
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

    let reader_account = bootstrap
        .sign_up(
            &PubkyLocalSecretKey::new(Keypair::random().secret_key()),
            &homeserver,
            None,
            PAYKIT_SESSION_CAPABILITIES,
        )
        .await
        .unwrap();
    let reader = parse_reader(&format!("pubky{}", reader_account.public_key)).unwrap();
    let reader_sdk = hosted_sdk(reader_account.access, "bitkit", 0).await;

    let creator_keypair = Keypair::random();
    let creator_account = bootstrap
        .sign_up(
            &PubkyLocalSecretKey::new(creator_keypair.secret_key()),
            &homeserver,
            None,
            PAYKIT_SESSION_CAPABILITIES,
        )
        .await
        .unwrap();
    let creator_key = creator_account.public_key.clone();
    let creator = parse_creator(&format!("pubky{creator_key}")).unwrap();
    let lock = content_lock(&creator);
    let lock_path = lock.content_lock_path().unwrap().to_string();
    bootstrap
        .sign_in(
            &PubkyLocalSecretKey::new(creator_keypair.secret_key()),
            "/pub/app.locks/:rw",
        )
        .await
        .unwrap()
        .access
        .session
        .storage()
        .put_json(lock_path.clone(), &lock)
        .await
        .unwrap();
    let store = CreatorStore::new(&pool, crypto.clone());
    store
        .create(&CreatorCredentials::new(
            creator.clone(),
            creator_account
                .export_session_secret()
                .await
                .unwrap()
                .into_inner(),
            PubkyLocalSecretKey::new(creator_keypair.secret_key())
                .derive_paykit_identity_secret_key(1)
                .unwrap(),
            account_xpub(seed),
            0,
        ))
        .await
        .unwrap();
    // Setup completion publishes the server's App on the creator's shared
    // state; do the same through a second grant on the creator's identity.
    let observer = PubkySessionBootstrap::with_pubky(
        creator_account.access.outbox_client.clone(),
        "app.paykit.test-observer",
    )
    .unwrap()
    .sign_in(
        &PubkyLocalSecretKey::new(creator_keypair.secret_key()),
        PAYKIT_SESSION_CAPABILITIES,
    )
    .await
    .unwrap();
    let creator_sdk = hosted_sdk(observer.access, "paykit-server", 0).await;
    store.mark_setup_complete(&creator).await.unwrap();

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
        signing_key,
        lock_resource: format!("{creator}{lock_path}"),
        creator_key,
        creator_sdk,
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
/// `{bundle_id, lock_resource, reader}`, signed. The Lock Server retries a
/// `503`: the server answers it when validating the creator's session fails
/// in a way it cannot tell from an outage, which an ambiguous Pubky request
/// failure under load does, and an exact replay of the invoice changes nothing.
async fn post_locks_invoice(stack: &Stack, bundle: &str) {
    let body = format!(
        r#"{{"bundle_id":"{bundle}","lock_resource":"{}","reader":"{}"}}"#,
        stack.lock_resource, stack.reader
    );
    let preimage = signature_preimage("POST", "/invoices", body.as_bytes());
    let mut attempt = 1;
    loop {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/invoices")
            .header(
                "X-Paykit-Signature",
                URL_SAFE_NO_PAD.encode(stack.signing_key.sign(&preimage).to_bytes()),
            )
            .body(Body::from(body.clone()))
            .unwrap();
        let (status, response) = send_http(stack.address, request).await;
        if status == StatusCode::SERVICE_UNAVAILABLE && attempt < 5 {
            attempt += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        assert_eq!(
            status,
            StatusCode::OK,
            "Locks invoice, attempt {attempt}: {response}"
        );
        return;
    }
}

/// The reader's Bitkit: keeps its side of the handshake moving, reads the
/// link and returns the request for `bundle` once it has arrived.
async fn wait_for_request(stack: &Stack, bundle: &str) -> PaymentRequestRecord {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let state = stack
            .reader_sdk
            .ensure_link_with_peer(stack.creator_key.clone(), 1)
            .await
            .map(|report| report.state);
        if matches!(state, Ok(LinkedPeerState::Linked)) {
            stack
                .reader_sdk
                .receive_private_messages(stack.creator_key.clone())
                .await
                .unwrap();
            if let Some(request) = stack
                .reader_sdk
                .payment_requests_with(&stack.creator_key)
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
    let reader = &stack.reader_sdk;

    // An earlier unlock creates the server link. No Allowance exists yet, and
    // the server's intake on an empty link changes nothing.
    post_locks_invoice(&stack, BUNDLE_BEFORE_GRANT).await;
    let before = wait_for_request(&stack, BUNDLE_BEFORE_GRANT).await;
    assert_eq!(before.counterparty, stack.creator_key);
    assert!(
        reader
            .list_allowances(AllowanceFilter::default())
            .await
            .unwrap()
            .is_empty()
    );

    // The reader grants on the creator's link: one grant covers the
    // lock price, the other's per-payment maximum is one sat below it.
    let covering = reader
        .propose_allowance(
            stack.creator_key.clone(),
            AllowanceLocalRole::Allower,
            allowance_terms("0.00005000"),
        )
        .await
        .unwrap();
    let tight = reader
        .propose_allowance(
            stack.creator_key.clone(),
            AllowanceLocalRole::Allower,
            allowance_terms("0.00001999"),
        )
        .await
        .unwrap();
    let sent = reader
        .process_outbound_private_messages(stack.creator_key.clone())
        .await
        .unwrap();
    // The identity-wide queue may also carry earlier link traffic.
    assert!(sent.failed.is_empty(), "{sent:?}");
    for proposal in [&covering, &tight] {
        assert!(
            proposal
                .proposal_outbound_message_id
                .is_some_and(|id| sent.sent.contains(&id)),
            "a proposal did not reach the server link: {sent:?}"
        );
    }

    // The next unlock: the server accepts both during the handoff, before it
    // proposes the request.
    post_locks_invoice(&stack, BUNDLE_AFTER_GRANT).await;
    let request = wait_for_request(&stack, BUNDLE_AFTER_GRANT).await;
    let allowances = reader
        .list_allowances(AllowanceFilter {
            counterparty: Some(stack.creator_key.clone()),
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
    // The wallet App claims the request for execution before it accepts it.
    reader
        .claim_payment_request_for_execution(scope.counterparty.clone(), &scope.payment_request_id)
        .await
        .unwrap();
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
    let state = stack.creator_sdk.export_backup_state().await.unwrap();
    assert!(state.allowance_accounting.is_none());
    let outbound = |kind: &str| {
        state
            .outbound_private_messages
            .iter()
            .filter(|record| record.kind == kind)
            .collect::<Vec<_>>()
    };
    let acceptances = outbound("paykit.allowance_acceptance");
    assert_eq!(acceptances.len(), 2);
    assert!(
        acceptances
            .iter()
            .all(|record| record.status == OutboundPrivateMessageStatus::Sent)
    );

    // The outbox hands a row off at least once: a handoff that outlives its
    // lease is claimed again and queues the same Payment Request once more.
    // What the feature promises is one logical request per Locks invoice, so
    // group the queued requests by invoice and compare their references.
    let mut references_by_bundle: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut first_request_by_bundle: BTreeMap<String, u64> = BTreeMap::new();
    for record in outbound("paykit.payment_request") {
        let json: serde_json::Value = serde_json::from_str(&record.raw_json).unwrap();
        let bundle = json["request"]["metadata"]["bundle_id"].as_str().unwrap();
        let reference = json["request"]["payment_reference"].as_str().unwrap();
        references_by_bundle
            .entry(bundle.to_owned())
            .or_default()
            .insert(reference.to_owned());
        first_request_by_bundle
            .entry(bundle.to_owned())
            .and_modify(|id| *id = (*id).min(record.outbound_message_id))
            .or_insert(record.outbound_message_id);
    }
    assert_eq!(
        references_by_bundle
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [BUNDLE_BEFORE_GRANT, BUNDLE_AFTER_GRANT],
        "one invoice per unlock"
    );
    for (bundle, references) in &references_by_bundle {
        assert_eq!(
            references.len(),
            1,
            "{bundle} was proposed under {references:?}"
        );
    }
    assert!(
        acceptances
            .iter()
            .all(|record| record.outbound_message_id < first_request_by_bundle[BUNDLE_AFTER_GRANT])
    );

    stack.shutdown().await;
}
