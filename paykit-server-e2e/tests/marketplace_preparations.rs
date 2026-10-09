use std::sync::Arc;

use paykit_sdk::PaykitIdentitySecretKey;
use paykit_server::{
    application::semantic_intent::DeliveryIntentV1,
    crypto::{Crypto, EncryptedEnvelope, EnvelopeContext},
    domain::locks::{CreatorPubky, ReaderPubky, parse_creator, parse_reader},
    persistence::{
        CreatorCredentials, CreatorStore, MarketplaceActivationInput, MarketplacePreparationInput,
        MarketplacePreparationPayloadFactory, MarketplacePreparationPayloads,
        MarketplacePreparationStore, MarketplaceResolutionOutcome, OutboxStore, PersistenceError,
        run_migrations,
    },
};
use paykit_server_e2e::postgres::TestDatabase;
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

mod common;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

type MarketplaceLifecycleFacts = (
    uuid::Uuid,
    String,
    Option<OffsetDateTime>,
    Option<OffsetDateTime>,
    Option<OffsetDateTime>,
);

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

struct Payloads;

#[derive(Serialize, Deserialize)]
struct StoredPreparationEnvelopeV1 {
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
    payment_request_intent: DeliveryIntentV1,
}

impl MarketplacePreparationPayloadFactory for Payloads {
    fn for_child_index(
        &self,
        child_index: i64,
    ) -> Result<MarketplacePreparationPayloads, PersistenceError> {
        Ok(MarketplacePreparationPayloads {
            payment_request_intent: common::payment_intent_with_reference(
                &reader(),
                format!("marketplace-address-{child_index}"),
                "7cceb26d-9042-4ea6-bfcb-01bbd778d76e".into(),
            ),
        })
    }
}

async fn store(database: &TestDatabase) -> MarketplacePreparationStore {
    run_migrations(database.pool()).await.unwrap();
    CreatorStore::new(database.pool(), crypto())
        .create(&CreatorCredentials::new(
            creator(),
            "session-secret".into(),
            PaykitIdentitySecretKey::new([9; 32], 1).unwrap(),
            Some(paykit_server::domain::receiving::BitcoinAccount {
                xpub: "xpub-secret".to_owned().into(),
                account_index: 0,
            }),
            None,
        ))
        .await
        .unwrap();
    MarketplacePreparationStore::new(database.pool(), crypto())
}

fn input<'a>(
    creator: &'a CreatorPubky,
    reader: &'a ReaderPubky,
    operation_id: &'a str,
    request_binding: &'a [u8],
) -> MarketplacePreparationInput<'a> {
    input_with_ttl(creator, reader, operation_id, request_binding, 15 * 60)
}

fn input_with_ttl<'a>(
    creator: &'a CreatorPubky,
    reader: &'a ReaderPubky,
    operation_id: &'a str,
    request_binding: &'a [u8],
    prepare_ttl_seconds: u64,
) -> MarketplacePreparationInput<'a> {
    input_with_ttl_and_window(
        creator,
        reader,
        operation_id,
        request_binding,
        prepare_ttl_seconds,
        24 * 60 * 60,
    )
}

fn input_with_ttl_and_window<'a>(
    creator: &'a CreatorPubky,
    reader: &'a ReaderPubky,
    operation_id: &'a str,
    request_binding: &'a [u8],
    prepare_ttl_seconds: u64,
    payment_window_seconds: u64,
) -> MarketplacePreparationInput<'a> {
    MarketplacePreparationInput {
        creator,
        reader,
        reference: "7cceb26d-9042-4ea6-bfcb-01bbd778d76e",
        operation_id,
        request_binding,
        total_sats: 100,
        payment_window_seconds,
        prepare_ttl_seconds,
        payloads: &Payloads,
    }
}

#[tokio::test]
async fn preparation_is_durable_unpublished_and_replay_first() {
    let database = TestDatabase::create().await;
    let store = store(&database).await;
    let creator = creator();
    let reader = reader();

    let first = store
        .prepare(input_with_ttl(
            &creator,
            &reader,
            "marketplace-payment:order-1:attempt-1",
            b"request-one",
            1,
        ))
        .await
        .unwrap();
    assert!(!first.replayed());
    assert_eq!(first.total_sats(), 100);
    assert_eq!(
        first.prepare_expires_at() - first.prepared_at(),
        time::Duration::seconds(1)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM marketplace_payment_preparations")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM invoices")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    assert!(
        OutboxStore::new(database.pool(), crypto())
            .claim(uuid::Uuid::new_v4(), 10, std::time::Duration::from_secs(30),)
            .await
            .unwrap()
            .is_empty(),
        "prepared work must not be claimable"
    );

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let replay = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:order-1:attempt-1",
            b"request-one",
        ))
        .await
        .unwrap();
    assert!(replay.replayed());
    assert_eq!(replay.invoice_id(), first.invoice_id());
    assert_eq!(replay.total_sats(), first.total_sats());
    assert!(replay.prepare_expires_at() < time::OffsetDateTime::now_utc());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_child_index FROM creators")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1,
        "exact replay allocated another address"
    );

    assert_eq!(
        store
            .prepare(input(
                &creator,
                &reader,
                "marketplace-payment:order-1:attempt-1",
                b"changed-request",
            ))
            .await,
        Err(PersistenceError::Conflict)
    );
    let second = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:order-1:attempt-2",
            b"changed-request",
        ))
        .await
        .unwrap();
    assert_ne!(second.invoice_id(), first.invoice_id());
    assert_eq!(
        second.prepare_expires_at() - second.prepared_at(),
        time::Duration::minutes(15)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM marketplace_payment_preparations")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );

    sqlx::query(
        "UPDATE marketplace_payment_preparations
         SET prepare_expires_at = prepare_expires_at + INTERVAL '1 second'
         WHERE id = $1",
    )
    .bind(first.invoice_id())
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(
        store
            .prepare(input(
                &creator,
                &reader,
                "marketplace-payment:order-1:attempt-1",
                b"request-one",
            ))
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );

    database.cleanup().await;
}

#[tokio::test]
async fn non_unique_insert_failure_is_not_an_idempotency_conflict() {
    let database = TestDatabase::create().await;
    let store = store(&database).await;
    let creator = creator();
    let reader = reader();

    sqlx::query(
        "CREATE FUNCTION reject_marketplace_preparation() RETURNS trigger AS $$
         BEGIN
             RAISE EXCEPTION 'forced non-unique insertion failure' USING ERRCODE = '23514';
         END;
         $$ LANGUAGE plpgsql",
    )
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_marketplace_preparation
         BEFORE INSERT ON marketplace_payment_preparations
         FOR EACH ROW EXECUTE FUNCTION reject_marketplace_preparation()",
    )
    .execute(database.pool())
    .await
    .unwrap();

    assert_eq!(
        store
            .prepare(input(
                &creator,
                &reader,
                "marketplace-payment:order-2:attempt-1",
                b"request-two",
            ))
            .await,
        Err(PersistenceError::CorruptOrMissing),
        "a PostgreSQL check/invariant failure must fail closed, not become Conflict/HTTP 409"
    );

    database.cleanup().await;
}

#[tokio::test]
async fn activate_starts_db_window_once_and_void_competes_on_the_preparation_row() {
    let database = TestDatabase::create().await;
    let first_store = store(&database).await;
    let second_store = MarketplacePreparationStore::new(database.pool(), crypto());
    let creator = creator();
    let reader = reader();
    let prepared = first_store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:race:attempt-1",
            b"race-request",
        ))
        .await
        .unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let activate = tokio::spawn({
        let barrier = barrier.clone();
        let store = first_store.clone();
        let creator = creator.clone();
        let invoice_id = prepared.invoice_id();
        async move {
            barrier.wait().await;
            store
                .activate(MarketplaceActivationInput {
                    creator: &creator,
                    invoice_id,
                    total_sats: 100,
                    proposal_acceptance_seconds: 30 * 60,
                })
                .await
        }
    });
    let void = tokio::spawn({
        let barrier = barrier.clone();
        let store = second_store.clone();
        let creator = creator.clone();
        let invoice_id = prepared.invoice_id();
        async move {
            barrier.wait().await;
            store.void(&creator, invoice_id).await
        }
    });
    barrier.wait().await;
    let (activated, voided) = tokio::join!(activate, void);
    let activated = activated.unwrap();
    let voided = voided.unwrap();

    assert!(
        matches!(
            (&activated, &voided),
            (Ok(Some(_)), Err(PersistenceError::Conflict))
        ) || matches!(
            (&activated, &voided),
            (Err(PersistenceError::Conflict), Ok(Some(_)))
        ),
        "exactly one lifecycle transition must commit: activate={activated:?}, void={voided:?}"
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM marketplace_payment_preparations WHERE id = $1")
            .bind(prepared.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    let outbox_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE marketplace_preparation_id = $1")
            .bind(prepared.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(outbox_count, i64::from(state == "active"));

    if let Ok(Some(activated)) = activated {
        assert_eq!(
            activated.payment_deadline() - activated.activated_at(),
            time::Duration::days(1)
        );
        assert_eq!(activated.total_sats(), 100);
        assert!(!activated.replayed());
        let replay = first_store
            .activate(MarketplaceActivationInput {
                creator: &creator,
                invoice_id: prepared.invoice_id(),
                total_sats: 100,
                proposal_acceptance_seconds: 30 * 60,
            })
            .await
            .unwrap()
            .unwrap();
        assert!(replay.replayed());
        assert_eq!(replay.activated_at(), activated.activated_at());
        assert_eq!(replay.payment_deadline(), activated.payment_deadline());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM outbox WHERE marketplace_preparation_id = $1",
            )
            .bind(prepared.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap(),
            1,
            "activation replay inserted another proposal intent"
        );
        let outbox = OutboxStore::new(database.pool(), crypto());
        let claims = outbox
            .claim(uuid::Uuid::new_v4(), 10, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(claims.len(), 1, "active proposal intent must be claimable");
        let intent = outbox.delivery_intent(&claims[0]).unwrap();
        let terms = intent.terms().unwrap();
        assert!(terms.proposal_expires_at.is_some());
        assert_eq!(
            terms.payment_deadline.as_deref(),
            Some(
                activated
                    .payment_deadline()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
                    .as_str()
            )
        );
    } else {
        let replay = second_store
            .void(&creator, prepared.invoice_id())
            .await
            .unwrap()
            .unwrap();
        assert!(replay.replayed());
        assert_eq!(replay.invoice_id(), prepared.invoice_id());
    }

    database.cleanup().await;
}

#[tokio::test]
async fn activation_rejects_wrong_total_expiry_and_prepared_resolution_without_publication() {
    let database = TestDatabase::create().await;
    let store = store(&database).await;
    let creator = creator();
    let reader = reader();

    let wrong_total = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:wrong-total:attempt-1",
            b"wrong-total",
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .activate(MarketplaceActivationInput {
                creator: &creator,
                invoice_id: wrong_total.invoice_id(),
                total_sats: 101,
                proposal_acceptance_seconds: 30 * 60,
            })
            .await,
        Err(PersistenceError::Conflict)
    );

    let expired = store
        .prepare(input_with_ttl(
            &creator,
            &reader,
            "marketplace-payment:expired:attempt-1",
            b"expired",
            1,
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    assert_eq!(
        store
            .activate(MarketplaceActivationInput {
                creator: &creator,
                invoice_id: expired.invoice_id(),
                total_sats: 100,
                proposal_acceptance_seconds: 30 * 60,
            })
            .await,
        Err(PersistenceError::Conflict)
    );

    let resolved = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:resolved:attempt-1",
            b"resolved",
        ))
        .await
        .unwrap();
    store
        .resolve(
            &creator,
            resolved.invoice_id(),
            MarketplaceResolutionOutcome::Abandoned,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .activate(MarketplaceActivationInput {
                creator: &creator,
                invoice_id: resolved.invoice_id(),
                total_sats: 100,
                proposal_acceptance_seconds: 30 * 60,
            })
            .await,
        Err(PersistenceError::Conflict)
    );
    assert!(
        store
            .void(&creator, resolved.invoice_id())
            .await
            .unwrap()
            .is_some()
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        0
    );
    database.cleanup().await;
}

#[tokio::test]
async fn resolution_annotates_prepared_active_and_voided_without_changing_lifecycle_facts() {
    let database = TestDatabase::create().await;
    let store = store(&database).await;
    let creator = creator();
    let reader = reader();
    let prepared = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:resolve-prepared:attempt-1",
            b"resolve-prepared",
        ))
        .await
        .unwrap();
    let active = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:resolve-active:attempt-1",
            b"resolve-active",
        ))
        .await
        .unwrap();
    let activated = store
        .activate(MarketplaceActivationInput {
            creator: &creator,
            invoice_id: active.invoice_id(),
            total_sats: 100,
            proposal_acceptance_seconds: 30 * 60,
        })
        .await
        .unwrap()
        .unwrap();
    let voided = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:resolve-voided:attempt-1",
            b"resolve-voided",
        ))
        .await
        .unwrap();
    let void_result = store
        .void(&creator, voided.invoice_id())
        .await
        .unwrap()
        .unwrap();

    for (invoice_id, outcome) in [
        (
            prepared.invoice_id(),
            MarketplaceResolutionOutcome::Abandoned,
        ),
        (
            active.invoice_id(),
            MarketplaceResolutionOutcome::PaidManually,
        ),
        (voided.invoice_id(), MarketplaceResolutionOutcome::Refunded),
    ] {
        let result = store
            .resolve(&creator, invoice_id, outcome)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.invoice_id(), invoice_id);
        assert_eq!(result.outcome(), outcome);
        assert!(!result.replayed());
    }

    let rows: Vec<MarketplaceLifecycleFacts> = sqlx::query_as(
        "SELECT id, state, activated_at, payment_deadline, voided_at
             FROM marketplace_payment_preparations ORDER BY prepared_at",
    )
    .fetch_all(database.pool())
    .await
    .unwrap();
    assert_eq!(
        rows[0],
        (prepared.invoice_id(), "prepared".into(), None, None, None)
    );
    assert_eq!(
        rows[1],
        (
            active.invoice_id(),
            "active".into(),
            Some(activated.activated_at()),
            Some(activated.payment_deadline()),
            None,
        )
    );
    assert_eq!(
        rows[2],
        (
            voided.invoice_id(),
            "voided".into(),
            None,
            None,
            Some(void_result.voided_at()),
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox")
            .fetch_one(database.pool())
            .await
            .unwrap(),
        1,
        "resolution must not add publication or cancellation work"
    );
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_same_resolution_replays_one_db_timestamp() {
    let database = TestDatabase::create().await;
    let first_store = store(&database).await;
    let second_store = MarketplacePreparationStore::new(database.pool(), crypto());
    let creator = creator();
    let prepared = first_store
        .prepare(input(
            &creator,
            &reader(),
            "marketplace-payment:resolve-replay:attempt-1",
            b"resolve-replay",
        ))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first = tokio::spawn({
        let barrier = barrier.clone();
        let creator = creator.clone();
        async move {
            barrier.wait().await;
            first_store
                .resolve(
                    &creator,
                    prepared.invoice_id(),
                    MarketplaceResolutionOutcome::PaidManually,
                )
                .await
        }
    });
    let second = tokio::spawn({
        let barrier = barrier.clone();
        let creator = creator.clone();
        async move {
            barrier.wait().await;
            second_store
                .resolve(
                    &creator,
                    prepared.invoice_id(),
                    MarketplaceResolutionOutcome::PaidManually,
                )
                .await
        }
    });
    barrier.wait().await;
    let first = first.await.unwrap().unwrap().unwrap();
    let second = second.await.unwrap().unwrap().unwrap();

    assert_ne!(first.replayed(), second.replayed());
    assert_eq!(first.resolved_at(), second.resolved_at());
    assert_eq!(first.outcome(), MarketplaceResolutionOutcome::PaidManually);
    assert_eq!(second.outcome(), MarketplaceResolutionOutcome::PaidManually);
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_different_resolutions_commit_one_immutable_outcome() {
    let database = TestDatabase::create().await;
    let first_store = store(&database).await;
    let second_store = MarketplacePreparationStore::new(database.pool(), crypto());
    let creator = creator();
    let prepared = first_store
        .prepare(input(
            &creator,
            &reader(),
            "marketplace-payment:resolve-conflict:attempt-1",
            b"resolve-conflict",
        ))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first = tokio::spawn({
        let barrier = barrier.clone();
        let creator = creator.clone();
        async move {
            barrier.wait().await;
            first_store
                .resolve(
                    &creator,
                    prepared.invoice_id(),
                    MarketplaceResolutionOutcome::Refunded,
                )
                .await
        }
    });
    let second = tokio::spawn({
        let barrier = barrier.clone();
        let creator = creator.clone();
        async move {
            barrier.wait().await;
            second_store
                .resolve(
                    &creator,
                    prepared.invoice_id(),
                    MarketplaceResolutionOutcome::Abandoned,
                )
                .await
        }
    });
    barrier.wait().await;
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert!(
        matches!(
            (&first, &second),
            (Ok(Some(_)), Err(PersistenceError::Conflict))
        ) || matches!(
            (&first, &second),
            (Err(PersistenceError::Conflict), Ok(Some(_)))
        ),
        "exactly one outcome must commit: first={first:?}, second={second:?}"
    );
    let stored: String = sqlx::query_scalar(
        "SELECT business_outcome FROM marketplace_payment_preparations WHERE id = $1",
    )
    .bind(prepared.invoice_id())
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert!(matches!(stored.as_str(), "refunded" | "abandoned"));
    let rewrite = sqlx::query(
        "UPDATE marketplace_payment_preparations
         SET business_outcome = 'paid_manually', resolved_at = clock_timestamp()
         WHERE id = $1",
    )
    .bind(prepared.invoice_id())
    .execute(database.pool())
    .await
    .unwrap_err();
    assert_eq!(
        rewrite.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    database.cleanup().await;
}

#[tokio::test]
async fn prepared_resolution_and_activation_serialize_without_unresolved_publication() {
    let database = TestDatabase::create().await;
    let activation_store = store(&database).await;
    let resolution_store = MarketplacePreparationStore::new(database.pool(), crypto());
    let creator = creator();
    let prepared = activation_store
        .prepare(input(
            &creator,
            &reader(),
            "marketplace-payment:resolve-activate:attempt-1",
            b"resolve-activate",
        ))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let activate = tokio::spawn({
        let barrier = barrier.clone();
        let creator = creator.clone();
        async move {
            barrier.wait().await;
            activation_store
                .activate(MarketplaceActivationInput {
                    creator: &creator,
                    invoice_id: prepared.invoice_id(),
                    total_sats: 100,
                    proposal_acceptance_seconds: 30 * 60,
                })
                .await
        }
    });
    let resolve = tokio::spawn({
        let barrier = barrier.clone();
        let creator = creator.clone();
        async move {
            barrier.wait().await;
            resolution_store
                .resolve(
                    &creator,
                    prepared.invoice_id(),
                    MarketplaceResolutionOutcome::Abandoned,
                )
                .await
        }
    });
    barrier.wait().await;
    let activated = activate.await.unwrap();
    let resolved = resolve.await.unwrap().unwrap().unwrap();
    assert!(!resolved.replayed());
    let state: String =
        sqlx::query_scalar("SELECT state FROM marketplace_payment_preparations WHERE id = $1")
            .bind(prepared.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    let outbox_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE marketplace_preparation_id = $1")
            .bind(prepared.invoice_id())
            .fetch_one(database.pool())
            .await
            .unwrap();
    match activated {
        Ok(Some(_)) => {
            assert_eq!(state, "active");
            assert_eq!(outbox_count, 1);
        }
        Err(PersistenceError::Conflict) => {
            assert_eq!(state, "prepared");
            assert_eq!(outbox_count, 0);
        }
        other => panic!("unexpected activation result: {other:?}"),
    }
    database.cleanup().await;
}

#[tokio::test]
async fn shortest_payment_window_still_has_a_usable_proposal_interval() {
    let database = TestDatabase::create().await;
    let store = store(&database).await;
    let creator = creator();
    let reader = reader();
    let prepared = store
        .prepare(input_with_ttl_and_window(
            &creator,
            &reader,
            "marketplace-payment:short-window:attempt-1",
            b"short-window",
            60,
            1,
        ))
        .await
        .unwrap();

    let activated = store
        .activate(MarketplaceActivationInput {
            creator: &creator,
            invoice_id: prepared.invoice_id(),
            total_sats: 100,
            proposal_acceptance_seconds: 30 * 60,
        })
        .await
        .unwrap()
        .unwrap();
    let outbox = OutboxStore::new(database.pool(), crypto());
    let claims = outbox
        .claim(uuid::Uuid::new_v4(), 1, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let intent = outbox.delivery_intent(&claims[0]).unwrap();
    let proposal_expires_at = OffsetDateTime::parse(
        intent
            .terms()
            .unwrap()
            .proposal_expires_at
            .as_deref()
            .unwrap(),
        &Rfc3339,
    )
    .unwrap();

    assert!(activated.activated_at() < proposal_expires_at);
    assert!(proposal_expires_at < activated.payment_deadline());
    assert_eq!(
        activated.payment_deadline() - activated.activated_at(),
        time::Duration::seconds(1)
    );
    database.cleanup().await;
}

#[tokio::test]
async fn authenticated_invalid_stored_intent_is_corrupt_not_caller_input() {
    let database = TestDatabase::create().await;
    let store = store(&database).await;
    let creator = creator();
    let reader = reader();
    let prepared = store
        .prepare(input(
            &creator,
            &reader,
            "marketplace-payment:invalid-stored-intent:attempt-1",
            b"invalid-stored-intent",
        ))
        .await
        .unwrap();
    let encrypted: Vec<u8> = sqlx::query_scalar(
        "SELECT preparation_envelope FROM marketplace_payment_preparations WHERE id = $1",
    )
    .bind(prepared.invoice_id())
    .fetch_one(database.pool())
    .await
    .unwrap();
    let crypto = crypto();
    let context = EnvelopeContext::marketplace_preparation(
        crypto.lookup_hash(creator.to_string().as_bytes()),
        prepared.invoice_id(),
    );
    let plaintext = crypto
        .decrypt(&context, &EncryptedEnvelope::from_bytes(encrypted))
        .unwrap();
    let mut envelope: StoredPreparationEnvelopeV1 = postcard::from_bytes(&plaintext).unwrap();
    envelope.reference = "53efc9fd-72b2-47b7-94c4-1d66dcad0018".into();
    let tampered = crypto
        .encrypt(&context, &postcard::to_allocvec(&envelope).unwrap())
        .unwrap();
    sqlx::query(
        "UPDATE marketplace_payment_preparations SET preparation_envelope = $2 WHERE id = $1",
    )
    .bind(prepared.invoice_id())
    .bind(tampered.as_bytes())
    .execute(database.pool())
    .await
    .unwrap();

    assert_eq!(
        store
            .activate(MarketplaceActivationInput {
                creator: &creator,
                invoice_id: prepared.invoice_id(),
                total_sats: 100,
                proposal_acceptance_seconds: 30 * 60,
            })
            .await,
        Err(PersistenceError::CorruptOrMissing)
    );
    database.cleanup().await;
}
