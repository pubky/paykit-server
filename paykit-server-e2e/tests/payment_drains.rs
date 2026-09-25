use std::sync::Arc;

use paykit_lib::{
    PaykitReceiverCapabilities, PaykitReceiverMarker, PaykitReceiverPath, PaymentAmount,
    PaymentEndpointIdentifier, PaymentReference, PaymentRequestTerms, PublicKey,
};
use paykit_server::{
    application::semantic_intent::{DeliveryIntentV1, DeliveryOperationV1},
    crypto::{Crypto, EncryptedEnvelope, EnvelopeContext},
    domain::{
        locks::{PubkyLockResource, parse_addressed_lock_resource},
        payment_request_lifecycle::PaymentRequestLifecycleState,
    },
    persistence::{OutboxStore, PaymentDrainStore, PersistenceError, run_migrations},
};
use paykit_server_e2e::postgres::TestDatabase;
use sqlx::Row;
use time::OffsetDateTime;
use uuid::Uuid;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const LOCK_RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";

fn lock_resource() -> PubkyLockResource {
    parse_addressed_lock_resource(LOCK_RESOURCE).unwrap()
}

fn marker() -> PaykitReceiverMarker {
    PaykitReceiverMarker::new(
        PaykitReceiverPath::new("bitkit/wallet").unwrap(),
        PaykitReceiverCapabilities {
            private_payments: true,
            payment_requests: true,
            receipts: false,
            outgoing_payments: false,
        },
        PublicKey::try_from_z32("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
    )
}

fn proposal_intent() -> DeliveryIntentV1 {
    DeliveryIntentV1::payment_request(
        READER.into(),
        &marker(),
        PaykitReceiverPath::new("paykit/server").unwrap(),
        &PaymentRequestTerms {
            amount: PaymentAmount::new("0.00001000", "BTC").unwrap(),
            payment_reference: PaymentReference::new(Uuid::new_v4().hyphenated().to_string())
                .unwrap(),
            proposal_expires_at: Some("2027-01-15T08:00:00Z".into()),
            recurrence: None,
            accepted_payment_endpoint_identifiers: vec![
                PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            ],
            metadata: serde_json::Map::new(),
        },
    )
    .unwrap()
}

async fn insert_invoice(
    database: &TestDatabase,
    crypto: &Crypto,
    creator_id: Uuid,
    ordinal: u8,
    lock_resource_generation: i64,
    state: PaymentRequestLifecycleState,
) -> (Uuid, String) {
    let invoice_id = Uuid::new_v4();
    let outbox_id = Uuid::new_v4();
    let request_id = Uuid::new_v4().to_string();
    let event_id = Uuid::new_v4().to_string();
    let creator_hash = crypto.lookup_hash(CREATOR.as_bytes());
    let intent = proposal_intent();
    let plaintext = postcard::to_allocvec(&intent).unwrap();
    let envelope = crypto
        .encrypt(
            &EnvelopeContext::outbox_semantic_intent(creator_hash, outbox_id),
            &plaintext,
        )
        .unwrap();
    let hash = |label: &str| {
        crypto
            .lookup_hash(format!("{label}-{ordinal}").as_bytes())
            .as_bytes()
            .to_vec()
    };
    let created_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();

    sqlx::query(
        "INSERT INTO invoices (
             id, creator_id, reader_lookup_hash, bundle_lookup_hash,
             lock_resource_lookup_hash, lock_resource_generation,
             payment_request_lookup_hash,
             invoice_envelope, payment_record_envelope, bitcoin_address_lookup_hash,
             derivation_index_lookup_hash, payment_status, confirmation_count,
             amount_matched, invoice_created_at, payment_deadline, payment_in_hours
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                   'undetected', 0, FALSE, $12, $13, 24)",
    )
    .bind(invoice_id)
    .bind(creator_id)
    .bind(hash("reader"))
    .bind(hash("bundle"))
    .bind(
        crypto
            .lookup_hash(LOCK_RESOURCE.as_bytes())
            .as_bytes()
            .as_slice(),
    )
    .bind(lock_resource_generation)
    .bind(hash("request"))
    .bind(b"encrypted-invoice".as_slice())
    .bind(b"encrypted-payment".as_slice())
    .bind(hash("address"))
    .bind(hash("index"))
    .bind(created_at)
    .bind(created_at + time::Duration::hours(24))
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox (
             id, creator_id, invoice_id, intent_envelope, intent_kind, status,
             sdk_outbound_message_id, sdk_event_id, sdk_payment_request_id,
             proposal_lookup_hash
         ) VALUES ($1, $2, $3, $4, 'payment_request_proposal', 'delivered', $5, $6, $7, $8)",
    )
    .bind(outbox_id)
    .bind(creator_id)
    .bind(invoice_id)
    .bind(envelope.as_bytes())
    .bind(u64::from(ordinal + 1).to_string())
    .bind(&event_id)
    .bind(&request_id)
    .bind(
        crypto
            .payment_request_proposal_lookup_hash(request_id.as_bytes())
            .as_bytes()
            .as_slice(),
    )
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO payment_request_lifecycles (
             invoice_id, sdk_payment_request_id, request_state, state_event_id,
             last_stream_item_id, last_outbound_message_id, last_event_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(invoice_id)
    .bind(&request_id)
    .bind(state.as_str())
    .bind(event_id)
    .bind(i64::from(ordinal + 1))
    .bind(i64::from(ordinal + 1))
    .bind(created_at)
    .execute(database.pool())
    .await
    .unwrap();

    (invoice_id, request_id)
}

async fn insert_attempt(
    database: &TestDatabase,
    invoice_id: Uuid,
    ordinal: u8,
    state: PaymentRequestLifecycleState,
) -> String {
    let request_id = Uuid::new_v4().to_string();
    let event_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO payment_request_lifecycles (
             invoice_id, sdk_payment_request_id, request_state, state_event_id,
             last_stream_item_id, last_outbound_message_id, last_event_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(invoice_id)
    .bind(&request_id)
    .bind(state.as_str())
    .bind(event_id)
    .bind(i64::from(ordinal + 1))
    .bind(i64::from(ordinal + 1))
    .bind(OffsetDateTime::from_unix_timestamp(1_800_000_000 + i64::from(ordinal)).unwrap())
    .execute(database.pool())
    .await
    .unwrap();
    request_id
}

#[tokio::test]
async fn drain_atomically_freezes_classification_and_cancellation_intent_for_exact_replay() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let crypto = Arc::new(Crypto::from_master_key(&[51; 32]).unwrap());
    let creator_id: Uuid = sqlx::query_scalar(
        "INSERT INTO creators (creator_lookup_hash, credential_envelope)
         VALUES ($1, $2) RETURNING id",
    )
    .bind(crypto.lookup_hash(CREATOR.as_bytes()).as_bytes().as_slice())
    .bind(b"encrypted-creator".as_slice())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let (accepted_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        0,
        0,
        PaymentRequestLifecycleState::Accepted,
    )
    .await;
    let (rejected_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        1,
        0,
        PaymentRequestLifecycleState::Rejected,
    )
    .await;
    let (proposed_invoice, proposed_request_id) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        2,
        0,
        PaymentRequestLifecycleState::Proposed,
    )
    .await;
    let (canceled_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        3,
        0,
        PaymentRequestLifecycleState::Canceled,
    )
    .await;
    let (expired_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        4,
        0,
        PaymentRequestLifecycleState::ProposalExpired,
    )
    .await;

    let store = PaymentDrainStore::new(database.pool(), crypto.clone());
    let (proposal_outbox_id, proposal_envelope): (Uuid, Vec<u8>) = sqlx::query_as(
        "SELECT id, intent_envelope FROM outbox
         WHERE invoice_id = $1 AND sdk_payment_request_id = $2",
    )
    .bind(proposed_invoice)
    .bind(&proposed_request_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE outbox SET intent_envelope = $1 WHERE id = $2")
        .bind(b"corrupt-cancellation-source".as_slice())
        .bind(proposal_outbox_id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(
        store.create(&lock_resource()).await,
        Err(PersistenceError::CorruptOrMissing)
    );
    let rollback_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
           (SELECT COUNT(*) FROM payment_drains),
           (SELECT COUNT(*) FROM payment_drain_items),
           (SELECT COUNT(*) FROM outbox)",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(rollback_counts, (0, 0, 5));
    sqlx::query("UPDATE outbox SET intent_envelope = $1 WHERE id = $2")
        .bind(proposal_envelope)
        .bind(proposal_outbox_id)
        .execute(database.pool())
        .await
        .unwrap();

    sqlx::query(
        "UPDATE outbox
         SET status = 'retryable', next_attempt_at = transaction_timestamp()
         WHERE id = $1",
    )
    .bind(proposal_outbox_id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO lock_payment_generations
             (creator_id, lock_resource_lookup_hash, current_generation)
         VALUES ($1, $2, 0)",
    )
    .bind(creator_id)
    .bind(
        crypto
            .lookup_hash(lock_resource().to_string().as_bytes())
            .as_bytes()
            .as_slice(),
    )
    .execute(database.pool())
    .await
    .unwrap();
    let outbox = OutboxStore::new(database.pool(), crypto.clone());
    let claimed_proposals = outbox
        .claim(Uuid::new_v4(), 10, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let claimed_proposal = claimed_proposals
        .iter()
        .find(|claim| claim.id() == proposal_outbox_id)
        .expect("proposal retry is claimed before drain");

    let lock = lock_resource();
    let (left, right) = tokio::join!(store.create(&lock), store.create(&lock));
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.drain_id(), right.drain_id());
    assert_ne!(left.replayed(), right.replayed());
    let first = if left.replayed() { right } else { left };
    assert!(!first.replayed());
    assert_eq!(first.accepted_count(), 1);
    assert_eq!(first.terminal_count(), 3);
    assert_eq!(first.cancellation_enqueued_count(), 1);
    assert!(!first.completed());
    assert!(
        !outbox
            .claim_handoff_eligible(claimed_proposal)
            .await
            .unwrap()
    );
    let first_token = crypto.payment_drain_cleanup_token(first.drain_id());

    assert_eq!(
        store
            .cleanup_completed(&lock_resource(), first_token.as_bytes())
            .await,
        Err(PersistenceError::Conflict)
    );
    let retained_active_drain: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM payment_drains WHERE id = $1")
            .bind(first.drain_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(retained_active_drain, 1);

    let items: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT invoice_id, classification
         FROM payment_drain_items WHERE drain_id = $1 ORDER BY classification",
    )
    .bind(first.drain_id())
    .fetch_all(database.pool())
    .await
    .unwrap();
    assert!(items.contains(&(accepted_invoice, "accepted".into())));
    assert!(items.contains(&(rejected_invoice, "rejected".into())));
    assert!(items.contains(&(canceled_invoice, "canceled".into())));
    assert!(items.contains(&(expired_invoice, "proposal_expired".into())));
    assert!(items.contains(&(proposed_invoice, "cancellation_enqueued".into())));
    sqlx::query(
        "UPDATE payment_drain_items SET classification = 'canceled'
         WHERE drain_id = $1 AND invoice_id = $2",
    )
    .bind(first.drain_id())
    .bind(rejected_invoice)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(
        store.exact_replay(&lock_resource()).await,
        Err(PersistenceError::CorruptOrMissing)
    );
    sqlx::query(
        "UPDATE payment_drain_items SET classification = 'rejected'
         WHERE drain_id = $1 AND invoice_id = $2",
    )
    .bind(first.drain_id())
    .bind(rejected_invoice)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE invoices SET lock_resource_generation = lock_resource_generation + 1
         WHERE id = $1",
    )
    .bind(rejected_invoice)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(
        store.exact_replay(&lock_resource()).await,
        Err(PersistenceError::CorruptOrMissing)
    );
    sqlx::query(
        "UPDATE invoices SET lock_resource_generation = lock_resource_generation - 1
         WHERE id = $1",
    )
    .bind(rejected_invoice)
    .execute(database.pool())
    .await
    .unwrap();
    let cancellation_outbox_id: Uuid = sqlx::query_scalar(
        "SELECT cancellation_outbox_id
         FROM payment_drain_cancellations
         WHERE drain_id = $1 AND invoice_id = $2 AND sdk_payment_request_id = $3",
    )
    .bind(first.drain_id())
    .bind(proposed_invoice)
    .bind(&proposed_request_id)
    .fetch_one(database.pool())
    .await
    .expect("proposed request has a durable cancellation intent");
    let cancellation_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT intent_envelope FROM outbox WHERE id = $1")
            .bind(cancellation_outbox_id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE outbox SET intent_envelope = $1 WHERE id = $2")
        .bind(b"corrupt-frozen-cancellation".as_slice())
        .bind(cancellation_outbox_id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(
        store.exact_replay(&lock_resource()).await,
        Err(PersistenceError::CorruptOrMissing)
    );
    sqlx::query("UPDATE outbox SET intent_envelope = $1 WHERE id = $2")
        .bind(cancellation_envelope)
        .bind(cancellation_outbox_id)
        .execute(database.pool())
        .await
        .unwrap();

    sqlx::query(
        "DELETE FROM payment_drain_cancellations
         WHERE drain_id = $1 AND sdk_payment_request_id = $2",
    )
    .bind(first.drain_id())
    .bind(&proposed_request_id)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(
        store.exact_replay(&lock_resource()).await,
        Err(PersistenceError::CorruptOrMissing)
    );
    sqlx::query(
        "INSERT INTO payment_drain_cancellations
             (drain_id, invoice_id, sdk_payment_request_id, cancellation_outbox_id)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(first.drain_id())
    .bind(proposed_invoice)
    .bind(&proposed_request_id)
    .bind(cancellation_outbox_id)
    .execute(database.pool())
    .await
    .unwrap();

    let substituted = sqlx::query(
        "UPDATE payment_drain_cancellations
         SET cancellation_outbox_id = $1
         WHERE drain_id = $2 AND sdk_payment_request_id = $3",
    )
    .bind(proposal_outbox_id)
    .bind(first.drain_id())
    .bind(&proposed_request_id)
    .execute(database.pool())
    .await;
    if substituted.is_ok() {
        sqlx::query(
            "UPDATE payment_drain_cancellations
             SET cancellation_outbox_id = $1
             WHERE drain_id = $2 AND sdk_payment_request_id = $3",
        )
        .bind(cancellation_outbox_id)
        .bind(first.drain_id())
        .bind(&proposed_request_id)
        .execute(database.pool())
        .await
        .unwrap();
    }
    assert!(
        substituted.is_err(),
        "database must reject an outbox row that is not the exact cancellation intent"
    );

    let cancellation_row = sqlx::query(
        "SELECT intent_envelope, invoice_id, depends_on_id, status
         FROM outbox WHERE id = $1",
    )
    .bind(cancellation_outbox_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(
        cancellation_row.get::<Option<Uuid>, _>("invoice_id"),
        Some(proposed_invoice)
    );
    assert_eq!(
        cancellation_row.get::<Option<Uuid>, _>("depends_on_id"),
        None
    );
    assert_eq!(cancellation_row.get::<String, _>("status"), "queued");
    let plaintext = crypto
        .decrypt(
            &EnvelopeContext::outbox_semantic_intent(
                crypto.lookup_hash(CREATOR.as_bytes()),
                cancellation_outbox_id,
            ),
            &EncryptedEnvelope::from_bytes(cancellation_row.get::<Vec<u8>, _>("intent_envelope")),
        )
        .unwrap();
    let cancellation = DeliveryIntentV1::decode(&plaintext).unwrap();
    match cancellation.operation() {
        DeliveryOperationV1::PaymentRequestCancellation { payment_request_id } => {
            assert_eq!(payment_request_id, &proposed_request_id);
        }
        operation => panic!("unexpected cancellation operation: {operation:?}"),
    }

    sqlx::query(
        "UPDATE payment_request_lifecycles
         SET request_state = 'accepted', state_event_id = $1,
             last_stream_item_id = last_stream_item_id + 1,
             last_event_at = last_event_at + INTERVAL '1 second'
         WHERE invoice_id = $2",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(proposed_invoice)
    .execute(database.pool())
    .await
    .unwrap();

    let replay = PaymentDrainStore::new(database.pool(), crypto.clone())
        .create(&lock_resource())
        .await
        .unwrap();
    assert!(replay.replayed());
    assert_eq!(replay.drain_id(), first.drain_id());
    assert_eq!(replay.created_at(), first.created_at());
    assert_eq!(replay.accepted_count(), 1);
    assert_eq!(replay.terminal_count(), 3);
    assert_eq!(replay.cancellation_enqueued_count(), 1);
    let replayed_classifications: Vec<String> = sqlx::query_scalar(
        "SELECT classification FROM payment_drain_items
         WHERE drain_id = $1 ORDER BY classification",
    )
    .bind(first.drain_id())
    .fetch_all(database.pool())
    .await
    .unwrap();
    assert_eq!(
        replayed_classifications,
        vec![
            "accepted",
            "canceled",
            "cancellation_enqueued",
            "proposal_expired",
            "rejected",
        ]
    );
    let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(outbox_count, 6);

    sqlx::query(
        "UPDATE payment_drains
         SET accepted_count = 0, terminal_count = 5, status = 'completed',
             completed_at = clock_timestamp()
         WHERE id = $1",
    )
    .bind(first.drain_id())
    .execute(database.pool())
    .await
    .unwrap();
    let cleanup_token = crypto.payment_drain_cleanup_token(first.drain_id());
    assert_eq!(
        store
            .cleanup_completed(&lock_resource(), cleanup_token.as_bytes())
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );

    let debug = format!("{first:?}");
    for forbidden in [
        accepted_invoice.to_string(),
        rejected_invoice.to_string(),
        proposed_invoice.to_string(),
        canceled_invoice.to_string(),
        expired_invoice.to_string(),
        proposed_request_id,
        first.drain_id().to_string(),
    ] {
        assert!(!debug.contains(&forbidden));
    }

    database.cleanup().await;
}

#[tokio::test]
async fn replay_monotonically_completes_accepted_item_after_timely_amount_match() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let crypto = Arc::new(Crypto::from_master_key(&[55; 32]).unwrap());
    let creator_id: Uuid = sqlx::query_scalar(
        "INSERT INTO creators (creator_lookup_hash, credential_envelope)
         VALUES ($1, $2) RETURNING id",
    )
    .bind(crypto.lookup_hash(CREATOR.as_bytes()).as_bytes().as_slice())
    .bind(b"encrypted-creator".as_slice())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let (accepted_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        0,
        0,
        PaymentRequestLifecycleState::Accepted,
    )
    .await;
    let store = PaymentDrainStore::new(database.pool(), crypto);
    let active = store.create(&lock_resource()).await.unwrap();
    assert!(!active.completed());
    assert_eq!(active.accepted_count(), 1);
    assert_eq!(active.terminal_count(), 0);

    sqlx::query(
        "UPDATE invoices
         SET payment_status = 'detected', amount_matched = TRUE,
             first_amount_matched_observed_at = invoice_created_at,
             first_amount_matched_outpoint_lookup_hash = decode(repeat('01', 32), 'hex')
         WHERE id = $1",
    )
    .bind(accepted_invoice)
    .execute(database.pool())
    .await
    .unwrap();

    let completed = store.exact_replay(&lock_resource()).await.unwrap().unwrap();
    assert!(completed.completed());
    assert_eq!(completed.accepted_count(), 0);
    assert_eq!(completed.terminal_count(), 1);
    assert_eq!(completed.cancellation_enqueued_count(), 0);
    assert_eq!(completed.drain_id(), active.drain_id());

    let replay = store.exact_replay(&lock_resource()).await.unwrap().unwrap();
    assert_eq!(replay.accepted_count(), 0);
    assert_eq!(replay.terminal_count(), 1);
    assert!(replay.completed());

    database.cleanup().await;
}

#[tokio::test]
async fn durable_cancellation_enqueue_is_sufficient_for_completion() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let crypto = Arc::new(Crypto::from_master_key(&[52; 32]).unwrap());
    let creator_id: Uuid = sqlx::query_scalar(
        "INSERT INTO creators (creator_lookup_hash, credential_envelope)
         VALUES ($1, $2) RETURNING id",
    )
    .bind(crypto.lookup_hash(CREATOR.as_bytes()).as_bytes().as_slice())
    .bind(b"encrypted-creator".as_slice())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let (historical_proposed, historical_request_id) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        0,
        0,
        PaymentRequestLifecycleState::Proposed,
    )
    .await;
    let (historical_rejected, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        1,
        0,
        PaymentRequestLifecycleState::Rejected,
    )
    .await;
    sqlx::query(
        "UPDATE invoices
         SET payment_status = 'detected', amount_matched = TRUE
         WHERE id = $1",
    )
    .bind(historical_rejected)
    .execute(database.pool())
    .await
    .unwrap();

    let store = PaymentDrainStore::new(database.pool(), crypto.clone());
    let drain = store.create(&lock_resource()).await.unwrap();
    assert!(drain.completed());
    assert_eq!(drain.accepted_count(), 0);
    assert_eq!(drain.terminal_count(), 1);
    assert_eq!(drain.cancellation_enqueued_count(), 1);
    let durable: (String, bool) =
        sqlx::query_as("SELECT status, completed_at IS NOT NULL FROM payment_drains WHERE id = $1")
            .bind(drain.drain_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(durable, ("completed".into(), true));

    let replay = store.create(&lock_resource()).await.unwrap();
    assert!(replay.replayed());
    assert!(replay.completed());
    assert_eq!(replay.drain_id(), drain.drain_id());
    let replay_only = store
        .exact_replay(&lock_resource())
        .await
        .unwrap()
        .expect("durable replay exists");
    assert!(replay_only.replayed());
    assert!(replay_only.completed());
    assert_eq!(replay_only.drain_id(), drain.drain_id());
    let drain_token = crypto.payment_drain_cleanup_token(drain.drain_id());

    let cancellation_outbox_id: Uuid = sqlx::query_scalar(
        "SELECT cancellation_outbox_id FROM payment_drain_cancellations
         WHERE drain_id = $1 AND sdk_payment_request_id = $2",
    )
    .bind(drain.drain_id())
    .bind(&historical_request_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM payment_drain_cancellations
         WHERE drain_id = $1 AND sdk_payment_request_id = $2",
    )
    .bind(drain.drain_id())
    .bind(&historical_request_id)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(
        store
            .cleanup_completed(&lock_resource(), drain_token.as_bytes())
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );
    sqlx::query(
        "INSERT INTO payment_drain_cancellations
             (drain_id, invoice_id, sdk_payment_request_id, cancellation_outbox_id)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(drain.drain_id())
    .bind(historical_proposed)
    .bind(&historical_request_id)
    .bind(cancellation_outbox_id)
    .execute(database.pool())
    .await
    .unwrap();

    store
        .cleanup_completed(&lock_resource(), drain_token.as_bytes())
        .await
        .unwrap();
    let remaining: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
           (SELECT COUNT(*) FROM payment_drain_items),
           (SELECT COUNT(*) FROM outbox),
           (SELECT COUNT(*) FROM invoices),
           (SELECT COUNT(*) FROM payment_request_lifecycles)",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(remaining, (0, 3, 2, 2));
    let retained_observation: (String, bool) =
        sqlx::query_as("SELECT payment_status, amount_matched FROM invoices WHERE id = $1")
            .bind(historical_rejected)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(retained_observation, ("detected".into(), true));

    let boundary: (i64, Option<Uuid>) = sqlx::query_as(
        "SELECT current_generation, active_drain_id
         FROM lock_payment_generations",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(boundary, (1, None));

    store
        .cleanup_completed(&lock_resource(), drain_token.as_bytes())
        .await
        .unwrap();
    let idempotent_boundary: (i64, Option<Uuid>) = sqlx::query_as(
        "SELECT current_generation, active_drain_id
         FROM lock_payment_generations",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(idempotent_boundary, boundary);

    sqlx::query(
        "UPDATE payment_request_lifecycles
         SET request_state = 'accepted', state_event_id = $1,
             last_stream_item_id = last_stream_item_id + 1,
             last_event_at = last_event_at + INTERVAL '1 second'
         WHERE invoice_id = $2",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(historical_proposed)
    .execute(database.pool())
    .await
    .unwrap();

    let (fresh_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        2,
        1,
        PaymentRequestLifecycleState::Proposed,
    )
    .await;
    let fresh_drain = store.create(&lock_resource()).await.unwrap();
    assert!(fresh_drain.completed());
    assert_eq!(fresh_drain.accepted_count(), 0);
    assert_eq!(fresh_drain.terminal_count(), 0);
    assert_eq!(fresh_drain.cancellation_enqueued_count(), 1);
    let fresh_items: Vec<Uuid> =
        sqlx::query_scalar("SELECT invoice_id FROM payment_drain_items WHERE drain_id = $1")
            .bind(fresh_drain.drain_id())
            .fetch_all(database.pool())
            .await
            .unwrap();
    assert_eq!(fresh_items, vec![fresh_invoice]);
    assert!(!fresh_items.contains(&historical_proposed));
    assert!(!fresh_items.contains(&historical_rejected));
    let fresh_token = crypto.payment_drain_cleanup_token(fresh_drain.drain_id());

    assert_eq!(
        store
            .cleanup_completed(&lock_resource(), drain_token.as_bytes())
            .await,
        Err(PersistenceError::Conflict)
    );
    let fresh_boundary: (i64, Option<Uuid>) =
        sqlx::query_as("SELECT current_generation, active_drain_id FROM lock_payment_generations")
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(fresh_boundary, (1, Some(fresh_drain.drain_id())));

    let original_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT lock_resource_envelope FROM payment_drains WHERE id = $1")
            .bind(fresh_drain.drain_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    let swapped_plaintext = format!("{}-authenticated-mismatch", lock_resource());
    let swapped_envelope = crypto
        .encrypt(
            &EnvelopeContext::payment_drain(
                crypto.lookup_hash(CREATOR.as_bytes()),
                fresh_drain.drain_id(),
            ),
            swapped_plaintext.as_bytes(),
        )
        .unwrap();
    sqlx::query("UPDATE payment_drains SET lock_resource_envelope = $1 WHERE id = $2")
        .bind(swapped_envelope.as_bytes())
        .bind(fresh_drain.drain_id())
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .cleanup_completed(&lock_resource(), fresh_token.as_bytes())
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );
    let after_mismatch: (i64, Option<Uuid>, i64) = sqlx::query_as(
        "SELECT current_generation, active_drain_id,
                (SELECT COUNT(*) FROM payment_drains WHERE id = $1)
         FROM lock_payment_generations",
    )
    .bind(fresh_drain.drain_id())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(after_mismatch, (1, Some(fresh_drain.drain_id()), 1));
    sqlx::query("UPDATE payment_drains SET lock_resource_envelope = $1 WHERE id = $2")
        .bind(original_envelope)
        .bind(fresh_drain.drain_id())
        .execute(database.pool())
        .await
        .unwrap();

    sqlx::query("DELETE FROM lock_payment_generations WHERE creator_id = $1")
        .bind(creator_id)
        .execute(database.pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .cleanup_completed(&lock_resource(), fresh_token.as_bytes())
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );
    let retained_corrupt_drain: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM payment_drains WHERE id = $1")
            .bind(fresh_drain.drain_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(retained_corrupt_drain, 1);

    database.cleanup().await;
}

#[tokio::test]
async fn multi_attempt_drain_counts_invoices_and_cancels_each_live_proposal_once() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let crypto = Arc::new(Crypto::from_master_key(&[61; 32]).unwrap());
    let creator_id: Uuid = sqlx::query_scalar(
        "INSERT INTO creators (creator_lookup_hash, credential_envelope)
         VALUES ($1, $2) RETURNING id",
    )
    .bind(crypto.lookup_hash(CREATOR.as_bytes()).as_bytes().as_slice())
    .bind(b"encrypted-creator".as_slice())
    .fetch_one(database.pool())
    .await
    .unwrap();

    let (cancel_invoice, first_proposed) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        0,
        0,
        PaymentRequestLifecycleState::Proposed,
    )
    .await;
    let second_proposed = insert_attempt(
        &database,
        cancel_invoice,
        1,
        PaymentRequestLifecycleState::Proposed,
    )
    .await;
    insert_attempt(
        &database,
        cancel_invoice,
        2,
        PaymentRequestLifecycleState::Rejected,
    )
    .await;

    let (accepted_invoice, _) = insert_invoice(
        &database,
        &crypto,
        creator_id,
        3,
        0,
        PaymentRequestLifecycleState::Proposed,
    )
    .await;
    insert_attempt(
        &database,
        accepted_invoice,
        4,
        PaymentRequestLifecycleState::ProofSubmitted,
    )
    .await;

    let store = PaymentDrainStore::new(database.pool(), crypto);
    let first = store.create(&lock_resource()).await.unwrap();
    assert_eq!(first.accepted_count(), 1);
    assert_eq!(first.terminal_count(), 0);
    assert_eq!(first.cancellation_enqueued_count(), 3);
    assert!(!first.completed());

    let cancellations: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT invoice_id, sdk_payment_request_id
         FROM payment_drain_cancellations ORDER BY sdk_payment_request_id",
    )
    .fetch_all(database.pool())
    .await
    .unwrap();
    assert_eq!(cancellations.len(), 3);
    assert!(cancellations.contains(&(cancel_invoice, first_proposed)));
    assert!(cancellations.contains(&(cancel_invoice, second_proposed)));
    assert!(
        cancellations
            .iter()
            .any(|(invoice_id, _)| *invoice_id == accepted_invoice)
    );

    let replay = store.create(&lock_resource()).await.unwrap();
    assert!(replay.replayed());
    assert_eq!(replay.drain_id(), first.drain_id());
    assert_eq!(replay.cancellation_enqueued_count(), 3);
    let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
        .fetch_one(database.pool())
        .await
        .unwrap();
    // Two original proposal intents plus three cancellation attempts. The first
    // invoice's ambiguous SDK attempts intentionally share one outbox intent.
    assert_eq!(outbox_count, 5);

    database.cleanup().await;
}

#[tokio::test]
async fn unsupported_canonical_states_fail_drain_classification_closed() {
    for (state, expected) in [
        (
            PaymentRequestLifecycleState::RecoveryRequired,
            PersistenceError::Unavailable,
        ),
        (
            PaymentRequestLifecycleState::InvalidConflict,
            PersistenceError::Conflict,
        ),
    ] {
        let database = TestDatabase::create().await;
        run_migrations(database.pool()).await.unwrap();
        let crypto = Arc::new(Crypto::from_master_key(&[state.as_str().len() as u8; 32]).unwrap());
        let creator_id: Uuid = sqlx::query_scalar(
            "INSERT INTO creators (creator_lookup_hash, credential_envelope)
             VALUES ($1, $2) RETURNING id",
        )
        .bind(crypto.lookup_hash(CREATOR.as_bytes()).as_bytes().as_slice())
        .bind(b"encrypted-creator".as_slice())
        .fetch_one(database.pool())
        .await
        .unwrap();
        insert_invoice(&database, &crypto, creator_id, 0, 0, state).await;

        let store = PaymentDrainStore::new(database.pool(), crypto);
        assert_eq!(store.create(&lock_resource()).await, Err(expected));
        let durable_rows: (i64, i64) = sqlx::query_as(
            "SELECT
               (SELECT COUNT(*) FROM payment_drains),
               (SELECT COUNT(*) FROM payment_drain_items)",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(durable_rows, (0, 0));
        database.cleanup().await;
    }
}
