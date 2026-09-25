use std::sync::Arc;

use paykit_lib::{
    PaykitReceiverCapabilities, PaykitReceiverMarker, PaykitReceiverPath, PaymentAmount,
    PaymentEndpointIdentifier, PaymentEndpointPayload, PaymentReference, PaymentRequestTerms,
    PublicKey,
};
use paykit_sdk::{
    LinkedPeerState, PubkyPublicKey, ReceiverNoiseSecretKey,
    storage::{LinkedPeerRecord, StorageState},
};
use paykit_server::{
    application::{
        connection_status::{
            ConnectionStatusError, ConnectionStatusService, PaykitConnectionState,
        },
        semantic_intent::{DeliveryIntentV1, DeliveryOperationV1},
    },
    crypto::{Crypto, EncryptedEnvelope, EnvelopeContext},
    domain::locks::{CreatorPubky, ReaderPubky, parse_bundle_id, parse_creator, parse_reader},
    persistence::{
        AtomicInvoiceInput, CreatorCredentials, CreatorStore, InvoiceStore,
        NewReaderPayloadFactory, NewReaderPayloads, PersistenceError, SdkStateStore,
        run_migrations,
    },
};
use paykit_server_e2e::postgres::TestDatabase;
use sqlx::Row;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const CONNECTION_BUNDLE: &str = "000G40R40M30E209185GR38E1W";

struct TestPayloads;
impl NewReaderPayloadFactory for TestPayloads {
    fn for_child_index(&self, _child_index: i64) -> Result<NewReaderPayloads, PersistenceError> {
        Ok(NewReaderPayloads {
            endpoint_intent: endpoint_intent(format!("test-address-{_child_index}")),
            bitcoin_address: format!("test-address-{_child_index}"),
        })
    }
}

struct PaymentAsEndpointPayloads;
impl NewReaderPayloadFactory for PaymentAsEndpointPayloads {
    fn for_child_index(&self, child_index: i64) -> Result<NewReaderPayloads, PersistenceError> {
        Ok(NewReaderPayloads {
            endpoint_intent: payment_intent(),
            bitcoin_address: format!("bad-address-{child_index}"),
        })
    }
}

struct CreatorPayloads {
    address_prefix: &'static str,
}

impl NewReaderPayloadFactory for CreatorPayloads {
    fn for_child_index(&self, child_index: i64) -> Result<NewReaderPayloads, PersistenceError> {
        let address = format!("{}-{child_index}", self.address_prefix);
        Ok(NewReaderPayloads {
            endpoint_intent: endpoint_intent(address.clone()),
            bitcoin_address: address,
        })
    }
}

fn marker() -> PaykitReceiverMarker {
    PaykitReceiverMarker::new(
        PaykitReceiverPath::new("bitkit/wallet").unwrap(),
        PaykitReceiverCapabilities {
            private_payments: true,
            payment_requests: true,
            receipts: false,
            outgoing_payments: true,
        },
        PublicKey::try_from_z32("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
    )
}

fn endpoint_intent(address: String) -> DeliveryIntentV1 {
    DeliveryIntentV1::endpoint(
        reader().to_string(),
        &marker(),
        PaykitReceiverPath::new("paykit/server").unwrap(),
        vec![(
            PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            PaymentEndpointPayload::new(address),
        )],
    )
    .unwrap()
}

fn payment_intent() -> DeliveryIntentV1 {
    DeliveryIntentV1::payment_request(
        reader().to_string(),
        &marker(),
        PaykitReceiverPath::new("paykit/server").unwrap(),
        &PaymentRequestTerms {
            amount: PaymentAmount::new("0.00000100", "btc").unwrap(),
            payment_reference: PaymentReference::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            proposal_expires_at: None,
            recurrence: None,
            accepted_payment_endpoint_identifiers: vec![
                PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
}

static TEST_PAYLOADS: TestPayloads = TestPayloads;

fn crypto() -> Arc<Crypto> {
    Arc::new(Crypto::from_master_key(&[7; 32]).unwrap())
}

fn creator() -> CreatorPubky {
    parse_creator(CREATOR).unwrap()
}

fn second_creator() -> CreatorPubky {
    for replacement in "ybndrfg8ejkmcpqxot1uwisza345h769".chars() {
        let mut candidate = CREATOR.to_owned();
        candidate.replace_range(6..7, &replacement.to_string());
        if let Ok(candidate) = parse_creator(&candidate)
            && candidate != creator()
        {
            return candidate;
        }
    }
    panic!("second valid creator fixture")
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

fn second_reader() -> ReaderPubky {
    parse_reader(&second_creator().to_string()).unwrap()
}

async fn invoice_store(database: &TestDatabase) -> InvoiceStore {
    run_migrations(database.pool()).await.unwrap();
    let crypto = crypto();
    CreatorStore::new(database.pool(), crypto.clone())
        .create(
            &CreatorCredentials::new(
                creator(),
                "session-secret".into(),
                ReceiverNoiseSecretKey::new([9; 32]),
                "xpub-secret".into(),
                0,
            ),
            &StorageState::default(),
        )
        .await
        .unwrap();
    InvoiceStore::new(database.pool(), crypto)
}

fn linked_peer_record(
    reader: &ReaderPubky,
    path: PaykitReceiverPath,
    state: LinkedPeerState,
) -> LinkedPeerRecord {
    LinkedPeerRecord {
        counterparty: PubkyPublicKey::from_raw_or_app_key(reader.to_string()).unwrap(),
        counterparty_receiver_path: path,
        state,
        last_sync_at: None,
        last_private_receive_at: None,
        failure_count: 0,
        local_recovery_attempt_id: None,
        local_recovery_marker_created_at: None,
        local_recovery_marker_last_error: None,
        remote_recovery_attempt_id: None,
        remote_recovery_marker_observed_at: None,
    }
}

async fn connection_rows(database: &TestDatabase) -> (String, String, Vec<String>) {
    let invoice = sqlx::query_scalar("SELECT row_to_json(i)::text FROM invoices i")
        .fetch_one(database.pool())
        .await
        .unwrap();
    let sdk_state = sqlx::query_scalar("SELECT row_to_json(s)::text FROM sdk_states s")
        .fetch_one(database.pool())
        .await
        .unwrap();
    let outbox = sqlx::query_scalar("SELECT row_to_json(o)::text FROM outbox o ORDER BY id")
        .fetch_all(database.pool())
        .await
        .unwrap();
    (invoice, sdk_state, outbox)
}

fn input<'a>(
    creator: &'a CreatorPubky,
    reader: &'a ReaderPubky,
    bundle: &'a [u8],
    request: &'a [u8],
) -> AtomicInvoiceInput<'a> {
    AtomicInvoiceInput {
        creator,
        reader,
        bundle_binding: bundle,
        payment_request_binding: request,
        new_reader_payloads: &TEST_PAYLOADS,
        payment_request_intent: payment_intent(),
        required_sats: 100,
    }
}

#[tokio::test]
async fn terminal_payment_request_states_override_already_detected_invoices() {
    let database = TestDatabase::create().await;
    let invoices = invoice_store(&database).await;
    let creator = creator();
    let reader = reader();
    let cancelled_invoice = invoices
        .create_atomic(input(
            &creator,
            &reader,
            b"detected-cancelled-bundle",
            b"detected-cancelled-request",
        ))
        .await
        .unwrap();
    let expired_invoice = invoices
        .create_atomic(input(
            &creator,
            &reader,
            b"detected-expired-bundle",
            b"detected-expired-request",
        ))
        .await
        .unwrap();

    for invoice_id in [cancelled_invoice.invoice_id(), expired_invoice.invoice_id()] {
        sqlx::query(
            "UPDATE invoices SET payment_status = 'detected', amount_matched = FALSE \
             WHERE id = $1",
        )
        .bind(invoice_id)
        .execute(database.pool())
        .await
        .unwrap();
    }
    for (invoice_id, event_id, request_id) in [
        (
            cancelled_invoice.invoice_id(),
            "event-cancelled",
            "request-cancelled",
        ),
        (
            expired_invoice.invoice_id(),
            "event-expired",
            "request-expired",
        ),
    ] {
        sqlx::query(
            "UPDATE outbox SET sdk_event_id = $2, sdk_payment_request_id = $3 \
             WHERE invoice_id = $1 AND depends_on_id IS NOT NULL",
        )
        .bind(invoice_id)
        .bind(event_id)
        .bind(request_id)
        .execute(database.pool())
        .await
        .unwrap();
    }

    let creator_ids = invoices
        .pending_payment_request_creator_ids()
        .await
        .unwrap();
    assert_eq!(creator_ids.len(), 1);
    assert_eq!(
        invoices
            .apply_terminal_payment_request_states(
                creator_ids[0],
                &["request-cancelled".to_owned()],
                &["request-expired".to_owned()],
            )
            .await
            .unwrap(),
        2
    );
    let cancelled_status: String =
        sqlx::query_scalar("SELECT payment_status FROM invoices WHERE id = $1")
            .bind(cancelled_invoice.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(cancelled_status, "cancelled");
    let expired_status: String =
        sqlx::query_scalar("SELECT payment_status FROM invoices WHERE id = $1")
            .bind(expired_invoice.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(expired_status, "expired");
}

#[tokio::test]
async fn connection_status_uses_exact_persisted_binding_and_does_not_mutate_rows() {
    let database = TestDatabase::create().await;
    let invoices = invoice_store(&database).await;
    let creator = creator();
    let reader = reader();
    let bundle_id = parse_bundle_id(CONNECTION_BUNDLE).unwrap();
    let invoice = invoices
        .create_atomic(input(
            &creator,
            &reader,
            CONNECTION_BUNDLE.as_bytes(),
            b"connection-request",
        ))
        .await
        .unwrap();
    let sdk_states = SdkStateStore::new(database.pool(), crypto());
    let service =
        ConnectionStatusService::new(Arc::new(invoices.clone()), Arc::new(sdk_states.clone()));
    let reader_key = PubkyPublicKey::from_raw_or_app_key(reader.to_string()).unwrap();
    let exact_path = PaykitReceiverPath::new("bitkit/wallet").unwrap();
    let wrong_path = PaykitReceiverPath::new("other/wallet").unwrap();

    sdk_states
        .update(&creator, |state| {
            state.linked_peers.insert(
                (reader_key.clone(), wrong_path.clone()),
                linked_peer_record(&reader, wrong_path.clone(), LinkedPeerState::Linked),
            );
        })
        .await
        .unwrap();
    assert_eq!(
        service.status(&creator, &bundle_id).await,
        Ok(PaykitConnectionState::None),
        "a different receiver path must not satisfy the persisted invoice binding"
    );
    assert_eq!(
        service
            .status(
                &creator,
                &parse_bundle_id("000G40R40M30E209185GR38E2W").unwrap(),
            )
            .await,
        Err(ConnectionStatusError::NotFound)
    );
    assert_eq!(
        service.status(&second_creator(), &bundle_id).await,
        Err(ConnectionStatusError::NotFound)
    );

    for (linked_state, public_state) in [
        (LinkedPeerState::NotLinked, PaykitConnectionState::None),
        (LinkedPeerState::Linking, PaykitConnectionState::Handshake),
        (LinkedPeerState::Linked, PaykitConnectionState::Connected),
        (
            LinkedPeerState::RecoveryRequired,
            PaykitConnectionState::RecoveryRequired,
        ),
        (LinkedPeerState::Blocked, PaykitConnectionState::Blocked),
    ] {
        sdk_states
            .update(&creator, |state| {
                state.linked_peers.insert(
                    (reader_key.clone(), exact_path.clone()),
                    linked_peer_record(&reader, exact_path.clone(), linked_state),
                );
            })
            .await
            .unwrap();
        let before = connection_rows(&database).await;

        assert_eq!(service.status(&creator, &bundle_id).await, Ok(public_state));
        assert_eq!(connection_rows(&database).await, before);
    }

    sqlx::query("UPDATE sdk_states SET state_envelope = $1")
        .bind(vec![0_u8; 8])
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(
        service.status(&creator, &bundle_id).await,
        Err(ConnectionStatusError::Unavailable)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM outbox WHERE invoice_id = $1")
            .bind(invoice.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap(),
        2
    );

    database.cleanup().await;
}

#[tokio::test]
async fn invoice_allocation_encrypts_payloads_orders_outbox_and_replays() {
    let database = TestDatabase::create().await;
    let store = invoice_store(&database).await;
    let creator = creator();
    let reader = reader();

    let first = store
        .create_atomic(input(&creator, &reader, b"bundle-one", b"request-one"))
        .await
        .unwrap();
    assert!(!first.replayed());
    assert_eq!(first.reader_child_index(), 0);
    let endpoint_id = first
        .endpoint_publication_outbox_id()
        .expect("new reader must enqueue endpoint publication");

    let protected_payment_row = sqlx::query(
        "SELECT payment_record_envelope, bitcoin_address_lookup_hash FROM invoices WHERE id = $1",
    )
    .bind(first.invoice_id())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let payment_record_envelope =
        protected_payment_row.get::<Vec<u8>, _>("payment_record_envelope");
    let address_lookup_hash =
        protected_payment_row.get::<Vec<u8>, _>("bitcoin_address_lookup_hash");
    assert_eq!(
        address_lookup_hash,
        crypto()
            .bitcoin_address_lookup_hash(b"test-address-0")
            .as_bytes()
            .as_slice()
    );
    assert!(
        !payment_record_envelope
            .windows(b"test-address-0".len())
            .any(|window| window == b"test-address-0")
    );

    let payment_row =
        sqlx::query("SELECT invoice_id, depends_on_id, intent_envelope FROM outbox WHERE id = $1")
            .bind(first.payment_request_outbox_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(
        payment_row.get::<Option<_>, _>("invoice_id"),
        Some(first.invoice_id())
    );
    assert_eq!(
        payment_row.get::<Option<_>, _>("depends_on_id"),
        Some(endpoint_id)
    );

    let endpoint_row = sqlx::query("SELECT invoice_id, intent_envelope FROM outbox WHERE id = $1")
        .bind(endpoint_id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(
        endpoint_row.get::<Option<uuid::Uuid>, _>("invoice_id"),
        Some(first.invoice_id())
    );

    for raw in [
        payment_row.get::<Vec<u8>, _>("intent_envelope"),
        endpoint_row.get::<Vec<u8>, _>("intent_envelope"),
        sqlx::query_scalar::<_, Vec<u8>>("SELECT invoice_envelope FROM invoices WHERE id = $1")
            .bind(first.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap(),
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT assignment_envelope FROM reader_assignments WHERE id = $1",
        )
        .bind(first.reader_assignment_id())
        .fetch_one(database.pool())
        .await
        .unwrap(),
    ] {
        for plaintext in [
            b"reader-assignment-private-sentinel".as_slice(),
            b"invoice-private-sentinel".as_slice(),
            b"endpoint-publication-private-sentinel".as_slice(),
            b"payment-request-private-sentinel".as_slice(),
        ] {
            assert!(
                !raw.windows(plaintext.len())
                    .any(|window| window == plaintext)
            );
        }
    }

    let original_payment_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT intent_envelope FROM outbox WHERE id = $1")
            .bind(first.payment_request_outbox_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    let creator_hash = crypto().lookup_hash(creator.to_string().as_bytes());
    let original_plaintext = crypto()
        .decrypt(
            &EnvelopeContext::outbox_semantic_intent(
                creator_hash,
                first.payment_request_outbox_id(),
            ),
            &EncryptedEnvelope::from_bytes(original_payment_envelope.clone()),
        )
        .unwrap();
    let original_intent = DeliveryIntentV1::decode(&original_plaintext).unwrap();
    let original_reference = match original_intent.operation() {
        DeliveryOperationV1::PaymentRequestProposal { terms } => terms.payment_reference.clone(),
        DeliveryOperationV1::EndpointPublication { .. } => {
            panic!("payment row had endpoint intent")
        }
    };
    let original_path = original_intent.selected_reader_path().unwrap();
    let original_fingerprint = original_intent.marker_fingerprint();

    let replay = store
        .create_atomic(input(&creator, &reader, b"bundle-one", b"request-one"))
        .await
        .unwrap();
    assert!(replay.replayed());
    assert_eq!(replay.invoice_id(), first.invoice_id());
    assert_eq!(
        replay.payment_request_outbox_id(),
        first.payment_request_outbox_id()
    );
    assert_eq!(replay.reader_assignment_id(), first.reader_assignment_id());
    assert_eq!(replay.reader_child_index(), 0);
    assert_eq!(replay.endpoint_publication_outbox_id(), Some(endpoint_id));
    let replayed_payment_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT intent_envelope FROM outbox WHERE id = $1")
            .bind(replay.payment_request_outbox_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(replayed_payment_envelope, original_payment_envelope);
    let replayed_plaintext = crypto()
        .decrypt(
            &EnvelopeContext::outbox_semantic_intent(
                creator_hash,
                replay.payment_request_outbox_id(),
            ),
            &EncryptedEnvelope::from_bytes(replayed_payment_envelope),
        )
        .unwrap();
    let replayed_intent = DeliveryIntentV1::decode(&replayed_plaintext).unwrap();
    assert_eq!(
        replayed_intent.selected_reader_path().unwrap(),
        original_path
    );
    assert_eq!(replayed_intent.marker_fingerprint(), original_fingerprint);
    assert!(matches!(
        replayed_intent.operation(),
        DeliveryOperationV1::PaymentRequestProposal { terms }
            if terms.payment_reference == original_reference
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM reader_assignments")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM invoices")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM outbox")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .create_atomic(input(&creator, &reader, b"bundle-one", b"changed-request"))
            .await,
        Err(PersistenceError::Conflict)
    );
    assert_eq!(
        store
            .create_atomic(input(
                &creator,
                &second_reader(),
                b"bundle-one",
                b"changed-reader-request",
            ))
            .await,
        Err(PersistenceError::Conflict)
    );

    let second = store
        .create_atomic(input(&creator, &reader, b"bundle-two", b"request-two"))
        .await
        .unwrap();
    assert!(!second.replayed());
    assert_ne!(second.reader_assignment_id(), first.reader_assignment_id());
    assert_eq!(second.reader_child_index(), 1);
    assert_ne!(
        second.endpoint_publication_outbox_id(),
        first.endpoint_publication_outbox_id()
    );
    assert_ne!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT bitcoin_address_lookup_hash FROM invoices WHERE id = $1",
        )
        .bind(first.invoice_id())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT bitcoin_address_lookup_hash FROM invoices WHERE id = $1",
        )
        .bind(second.invoice_id())
        .fetch_one(database.pool())
        .await
        .unwrap(),
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "SELECT depends_on_id FROM outbox WHERE id = $1",
        )
        .bind(second.payment_request_outbox_id())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        second.endpoint_publication_outbox_id()
    );

    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_exact_invoice_allocation_serializes_to_one_durable_result() {
    let database = TestDatabase::create().await;
    let store = invoice_store(&database).await;
    let other_store = store.clone();
    let creator = creator();
    let reader = reader();

    let (first, second) = tokio::join!(
        store.create_atomic(input(
            &creator,
            &reader,
            b"concurrent-bundle",
            b"concurrent-request"
        )),
        other_store.create_atomic(input(
            &creator,
            &reader,
            b"concurrent-bundle",
            b"concurrent-request"
        ))
    );
    let first = first.unwrap();
    let second = second.unwrap();

    assert_ne!(first.replayed(), second.replayed());
    assert_eq!(first.invoice_id(), second.invoice_id());
    assert_eq!(first.reader_assignment_id(), second.reader_assignment_id());
    assert_eq!(
        first.endpoint_publication_outbox_id(),
        second.endpoint_publication_outbox_id()
    );
    assert_eq!(
        first.payment_request_outbox_id(),
        second.payment_request_outbox_id()
    );
    for (table, expected) in [
        ("reader_assignments", 1_i64),
        ("invoices", 1_i64),
        ("outbox", 2_i64),
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, expected, "unexpected {table} cardinality");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_child_index FROM creators")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );

    database.cleanup().await;
}

#[tokio::test]
async fn atomic_store_rejects_wrong_intent_role_and_reader_before_any_insert() {
    static BAD_PAYLOADS: PaymentAsEndpointPayloads = PaymentAsEndpointPayloads;
    let database = TestDatabase::create().await;
    let store = invoice_store(&database).await;
    let creator = creator();
    let reader = reader();

    let mut wrong_payment_role = input(&creator, &reader, b"bad-role", b"bad-role-request");
    wrong_payment_role.payment_request_intent = endpoint_intent("wrong-role-address".into());
    assert_eq!(
        store.create_atomic(wrong_payment_role).await,
        Err(PersistenceError::CorruptOrMissing)
    );

    let mut wrong_endpoint_role = input(
        &creator,
        &reader,
        b"bad-endpoint-role",
        b"bad-endpoint-request",
    );
    wrong_endpoint_role.new_reader_payloads = &BAD_PAYLOADS;
    assert_eq!(
        store.create_atomic(wrong_endpoint_role).await,
        Err(PersistenceError::CorruptOrMissing)
    );

    let other_reader = second_reader();
    assert_eq!(
        store
            .create_atomic(input(
                &creator,
                &other_reader,
                b"bad-reader",
                b"bad-reader-request",
            ))
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM invoices),
            (SELECT COUNT(*) FROM reader_assignments),
            (SELECT COUNT(*) FROM outbox)",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(counts, (0, 0, 0));

    database.cleanup().await;
}

#[tokio::test]
async fn allocation_invoice_and_both_outbox_inserts_rollback_on_dependent_outbox_failure() {
    let database = TestDatabase::create().await;
    let store = invoice_store(&database).await;
    let creator = creator();
    let reader = reader();
    sqlx::query(
        "CREATE FUNCTION fail_payment_request_outbox_insert() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF NEW.invoice_id IS NOT NULL THEN RAISE EXCEPTION 'forced payment request outbox failure'; END IF; RETURN NEW; END; $$",
    )
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_payment_request_outbox_insert BEFORE INSERT ON outbox \
         FOR EACH ROW EXECUTE FUNCTION fail_payment_request_outbox_insert()",
    )
    .execute(database.pool())
    .await
    .unwrap();

    assert!(
        store
            .create_atomic(input(
                &creator,
                &reader,
                b"rollback-bundle",
                b"rollback-request",
            ))
            .await
            .is_err()
    );
    for table in ["reader_assignments", "invoices", "outbox"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{table} escaped the failed transaction");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_child_index FROM creators")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0,
        "allocator increment escaped the failed transaction"
    );

    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_creators_own_distinct_intents_at_the_same_child_index() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let crypto = crypto();
    let creators = CreatorStore::new(database.pool(), crypto.clone());
    let first_creator = creator();
    let second_creator = second_creator();
    creators
        .create(
            &CreatorCredentials::new(
                first_creator.clone(),
                "session-one".into(),
                ReceiverNoiseSecretKey::new([9; 32]),
                "xpub-one".into(),
                0,
            ),
            &StorageState::default(),
        )
        .await
        .unwrap();
    creators
        .create(
            &CreatorCredentials::new(
                second_creator.clone(),
                "session-two".into(),
                ReceiverNoiseSecretKey::new([8; 32]),
                "xpub-two".into(),
                0,
            ),
            &StorageState::default(),
        )
        .await
        .unwrap();

    let first_store = InvoiceStore::new(database.pool(), crypto.clone());
    let second_store = first_store.clone();
    let reader = reader();
    let first_payloads = CreatorPayloads {
        address_prefix: "creator-one-address",
    };
    let second_payloads = CreatorPayloads {
        address_prefix: "creator-two-address",
    };
    let (first, second) = tokio::join!(
        first_store.create_atomic(AtomicInvoiceInput {
            creator: &first_creator,
            reader: &reader,
            bundle_binding: b"creator-one-bundle",
            payment_request_binding: b"creator-one-request",
            new_reader_payloads: &first_payloads,
            payment_request_intent: payment_intent(),
            required_sats: 100,
        }),
        second_store.create_atomic(AtomicInvoiceInput {
            creator: &second_creator,
            reader: &reader,
            bundle_binding: b"creator-two-bundle",
            payment_request_binding: b"creator-two-request",
            new_reader_payloads: &second_payloads,
            payment_request_intent: payment_intent(),
            required_sats: 100,
        })
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.reader_child_index(), 0);
    assert_eq!(second.reader_child_index(), 0);

    let first_hashes: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT bitcoin_address_lookup_hash, derivation_index_lookup_hash
         FROM invoices WHERE id = $1",
    )
    .bind(first.invoice_id())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let second_hashes: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT bitcoin_address_lookup_hash, derivation_index_lookup_hash
         FROM invoices WHERE id = $1",
    )
    .bind(second.invoice_id())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_ne!(first_hashes.0, second_hashes.0);
    assert_ne!(first_hashes.1, second_hashes.1);

    let first_endpoint = first.endpoint_publication_outbox_id().unwrap();
    let first_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT intent_envelope FROM outbox WHERE id = $1")
            .bind(first_endpoint)
            .fetch_one(database.pool())
            .await
            .unwrap();
    let first_hash = crypto.lookup_hash(first_creator.to_string().as_bytes());
    let second_hash = crypto.lookup_hash(second_creator.to_string().as_bytes());
    let first_payment_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT payment_record_envelope FROM invoices WHERE id = $1")
            .bind(first.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert!(
        crypto
            .decrypt(
                &EnvelopeContext::invoice_payment_record(second_hash, first.invoice_id()),
                &EncryptedEnvelope::from_bytes(first_payment_envelope.clone()),
            )
            .is_err(),
        "another Creator context decrypted the first Creator payment record"
    );
    assert!(
        crypto
            .decrypt(
                &EnvelopeContext::invoice_payment_record(first_hash, first.invoice_id()),
                &EncryptedEnvelope::from_bytes(first_payment_envelope),
            )
            .is_ok()
    );
    assert!(
        crypto
            .decrypt(
                &EnvelopeContext::outbox_semantic_intent(second_hash, first_endpoint),
                &EncryptedEnvelope::from_bytes(first_envelope.clone()),
            )
            .is_err(),
        "another Creator context decrypted the first Creator intent"
    );
    let plaintext = crypto
        .decrypt(
            &EnvelopeContext::outbox_semantic_intent(first_hash, first_endpoint),
            &EncryptedEnvelope::from_bytes(first_envelope),
        )
        .unwrap();
    let intent: DeliveryIntentV1 = postcard::from_bytes(&plaintext).unwrap();
    assert!(matches!(
        intent.operation(),
        DeliveryOperationV1::EndpointPublication { receiving_details }
            if receiving_details[0].payload == "creator-one-address-0"
    ));

    database.cleanup().await;
}
