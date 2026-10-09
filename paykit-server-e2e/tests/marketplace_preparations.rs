use std::sync::Arc;

use paykit_sdk::PaykitIdentitySecretKey;
use paykit_server::{
    crypto::Crypto,
    domain::locks::{CreatorPubky, ReaderPubky, parse_creator, parse_reader},
    persistence::{
        CreatorCredentials, CreatorStore, MarketplacePreparationInput,
        MarketplacePreparationPayloadFactory, MarketplacePreparationPayloads,
        MarketplacePreparationStore, PersistenceError, run_migrations,
    },
};
use paykit_server_e2e::postgres::TestDatabase;

mod common;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

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
    MarketplacePreparationInput {
        creator,
        reader,
        reference: "7cceb26d-9042-4ea6-bfcb-01bbd778d76e",
        operation_id,
        request_binding,
        total_sats: 100,
        payment_window_seconds: 24 * 60 * 60,
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
