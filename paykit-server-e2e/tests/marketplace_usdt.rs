//! Verified USDT0 proofs on active Marketplace requests, and the status route that
//! reports them next to Bitcoin. Real services and PostgreSQL; only the BTC/USD feed,
//! the Bitcoin chain and the Arbitrum RPC are doubles.

use std::sync::atomic::Ordering;

use axum::http::StatusCode;
use paykit_server::{
    bitcoin::ObservedOutput, config::BitcoinNetwork, crypto::Crypto, usdt::ArbitrumVerifier,
    workers::observer::observe_marketplace_once_at,
};
use serde_json::{Value, json};
use time::OffsetDateTime;
use uuid::Uuid;

mod common;

use common::{
    arbitrum::{Rpc, fixture, record},
    marketplace_rig::*,
};

/// USD 0.05: payable as 50,000 millionths of USDT, which the fixture receipt carries,
/// and as 62 sats at the 81,000 USD feed.
const AMOUNT: &str = "5";
const REQUIRED_USDT: u64 = 50_000;
const REQUIRED_SATS: u64 = 62;

struct Offered {
    invoice: String,
    activated_at: OffsetDateTime,
    payment_deadline: OffsetDateTime,
}

impl Offered {
    fn at(&self, seconds: i64) -> i64 {
        self.activated_at.unix_timestamp() + seconds
    }
}

fn reference(index: usize) -> String {
    fixture(index)["binding"]["paymentReference"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Prepares and activates a request whose reference and request id match proof fixture
/// `index`, and records the SDK lifecycle attribution the background worker would.
async fn offered(rig: &Rig, index: usize, accepted: &[&str]) -> Offered {
    offered_with_window(rig, index, accepted, 1_800).await
}

async fn offered_with_window(rig: &Rig, index: usize, accepted: &[&str], window: u64) -> Offered {
    let (status, prepared) = rig
        .prepare_with_reference(
            &reference(index),
            &format!("usdt-{index}"),
            AMOUNT,
            "USD",
            accepted,
            window,
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&prepared)
    );
    let invoice = invoice_id(&prepared);
    let (status, activated) = rig.activate(&invoice, AMOUNT, "USD").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&activated)
    );
    let parse = |field: &str| {
        OffsetDateTime::parse(
            json(&activated)[field].as_str().unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap()
    };
    sqlx::query(
        "INSERT INTO payment_request_lifecycles (
             marketplace_preparation_id, sdk_payment_request_id, request_state,
             state_event_id, last_stream_item_id, last_event_at
         ) VALUES ($1, $2, 'proof_submitted', $3, 1, clock_timestamp())",
    )
    .bind(Uuid::parse_str(&invoice).unwrap())
    .bind(
        fixture(index)["binding"]["paymentRequestId"]
            .as_str()
            .unwrap(),
    )
    .bind(Uuid::new_v4().to_string())
    .execute(rig.database.pool())
    .await
    .unwrap();
    Offered {
        invoice,
        activated_at: parse("activated_at"),
        payment_deadline: parse("payment_deadline"),
    }
}

async fn creator_row(rig: &Rig) -> Uuid {
    sqlx::query_scalar("SELECT id FROM creators")
        .fetch_one(rig.database.pool())
        .await
        .unwrap()
}

/// One background pass over the SDK's record for proof fixture `index`. A fresh
/// verifier per pass keeps the five-second chain-tip cache from hiding a chain change.
async fn submit(rig: &Rig, rpc: &Rpc, index: usize) {
    let verifier = ArbitrumVerifier::new(rpc.url.clone()).unwrap();
    rig.store
        .observe_usdt_request(
            creator_row(rig).await,
            &creator(),
            &record(index),
            &verifier,
        )
        .await
        .unwrap();
}

/// The background pass defers a still-present receipt for a minute; age it to re-check now.
async fn age_receipts(rig: &Rig) {
    sqlx::query("UPDATE usdt_observations SET checked_at = NOW() - INTERVAL '2 minutes'")
        .execute(rig.database.pool())
        .await
        .unwrap();
}

fn chain(rpc: &Rpc, change: impl FnOnce(&mut common::arbitrum::Chain)) {
    change(&mut rpc.chain.lock().unwrap());
}

fn transfer(chain: &mut common::arbitrum::Chain, amount: u64) {
    chain.receipt["logs"][1]["data"] = json!(format!("0x{amount:064x}"));
}

async fn observe_bitcoin(rig: &Rig, sats: u64, confirmations: u32, byte: u8, at: OffsetDateTime) {
    let targets = rig.store.observation_targets_at(at).await.unwrap();
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
        &rig.store,
        &BitcoinNetwork::Mainnet,
        &targets,
        at,
    )
    .await
    .unwrap();
}

const TX_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

#[tokio::test]
async fn verified_usdt_proof_reaches_a_dual_asset_request_and_sets_the_paid_asset() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;

    let before = rig.status(&request.invoice).await;
    assert_eq!(before["accepted_assets"], json!(["BTC", "USDT"]));
    assert_eq!(
        before["payment_amounts"],
        json!({"BTC": REQUIRED_SATS.to_string(), "USDT": REQUIRED_USDT.to_string()})
    );
    assert!(before["usdt_arbitrum"].is_null());
    assert!(before["paid_asset"].is_null());
    assert_eq!(before["payment_state"], "undetected");
    assert_status_fixture(&rig, &request.invoice, "dual-asset").await;

    chain(&rpc, |chain| chain.timestamp = request.at(10));
    submit(&rig, &rpc, 0).await;
    let included = rig.status(&request.invoice).await;
    assert_eq!(
        included["usdt_arbitrum"],
        json!({
            "tx_hash": TX_HASH,
            "observed_amount": REQUIRED_USDT.to_string(),
            "confirmations": 2,
            "amount_matched": true,
            "paid_on_time": true,
            "finalized": false,
        })
    );
    assert_eq!(included["paid_asset"], "USDT");
    assert_eq!(included["payment_state"], "confirmed");
    assert!(included["bitcoin"].is_null());
    assert_status_fixture(&rig, &request.invoice, "usdt-included").await;

    chain(&rpc, |chain| chain.finalized = true);
    age_receipts(&rig).await;
    submit(&rig, &rpc, 0).await;
    let finalized = rig.status(&request.invoice).await;
    assert_eq!(finalized["usdt_arbitrum"]["finalized"], true);
    assert_eq!(finalized["paid_asset"], "USDT");
    assert_status_fixture(&rig, &request.invoice, "usdt-finalized").await;

    rig.store.scan_observation_integrity().await.unwrap();
    rig.cleanup().await;
}

async fn assert_status_fixture(rig: &Rig, invoice: &str, name: &str) {
    let (status, body) = rig
        .post(STATUS, json!({"creator": CREATOR, "invoice_id": invoice}))
        .await;
    assert_eq!(status, StatusCode::OK);
    common::assert_fixture(STATUS_FIXTURES, name, &body, &VOLATILE);
}

#[tokio::test]
async fn reorged_usdt_receipt_reports_null_and_returns_when_the_block_does() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;
    chain(&rpc, |chain| chain.timestamp = request.at(10));
    submit(&rig, &rpc, 0).await;
    assert_eq!(rig.status(&request.invoice).await["paid_asset"], "USDT");

    chain(&rpc, |chain| chain.reorg = true);
    age_receipts(&rig).await;
    submit(&rig, &rpc, 0).await;
    let reorged = rig.status(&request.invoice).await;
    assert!(reorged["usdt_arbitrum"].is_null());
    assert!(reorged["paid_asset"].is_null());
    assert_eq!(reorged["payment_state"], "undetected");
    assert_status_fixture(&rig, &request.invoice, "usdt-reorged").await;
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM usdt_observations")
        .fetch_one(rig.database.pool())
        .await
        .unwrap();
    assert_eq!(kept, 1, "the receipt row is kept as evidence");

    chain(&rpc, |chain| chain.reorg = false);
    age_receipts(&rig).await;
    submit(&rig, &rpc, 0).await;
    assert_eq!(rig.status(&request.invoice).await["paid_asset"], "USDT");
    rig.cleanup().await;
}

#[tokio::test]
async fn finalized_receipt_survives_a_reorg_signal() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;
    chain(&rpc, |chain| {
        chain.timestamp = request.at(10);
        chain.finalized = true;
    });
    submit(&rig, &rpc, 0).await;
    chain(&rpc, |chain| chain.reorg = true);
    age_receipts(&rig).await;
    submit(&rig, &rpc, 0).await;
    let status = rig.status(&request.invoice).await;
    assert_eq!(status["usdt_arbitrum"]["finalized"], true);
    assert_eq!(status["paid_asset"], "USDT");
    rig.cleanup().await;
}

#[tokio::test]
async fn underpaid_early_and_late_receipts_are_visible_but_never_the_paid_asset() {
    for (label, amount, offset, matched, on_time) in [
        ("underpaid", REQUIRED_USDT - 1, 10, false, false),
        ("before activation", REQUIRED_USDT, -120, true, false),
        ("after the deadline", REQUIRED_USDT, 1_801, true, false),
        ("exactly at the deadline", REQUIRED_USDT, 1_800, true, true),
        ("overpaid", REQUIRED_USDT + 1, 10, true, true),
    ] {
        let rig = rig(BOTH_RAILS, true).await;
        let rpc = Rpc::start().await;
        let request = offered(&rig, 0, &[]).await;
        chain(&rpc, |chain| {
            transfer(chain, amount);
            chain.timestamp = request.at(offset);
        });
        submit(&rig, &rpc, 0).await;
        let status = rig.status(&request.invoice).await;
        let usdt = &status["usdt_arbitrum"];
        assert_eq!(usdt["amount_matched"], matched, "{label}");
        assert_eq!(usdt["paid_on_time"], on_time, "{label}");
        assert_eq!(usdt["observed_amount"], amount.to_string(), "{label}");
        assert_eq!(
            status["paid_asset"],
            if on_time { json!("USDT") } else { Value::Null },
            "{label}"
        );
        if label == "underpaid" {
            assert_status_fixture(&rig, &request.invoice, "usdt-underpaid").await;
        }
        if label == "after the deadline" {
            assert_eq!(status["payment_state"], "expired", "{label}");
            assert_status_fixture(&rig, &request.invoice, "usdt-late").await;
        }
        assert!(
            request.payment_deadline > request.activated_at,
            "window sanity"
        );
        rig.cleanup().await;
    }
}

#[tokio::test]
async fn bitcoin_payment_on_a_dual_asset_request_leaves_usdt_null() {
    let rig = rig(BOTH_RAILS, true).await;
    let request = offered(&rig, 0, &[]).await;
    observe_bitcoin(
        &rig,
        REQUIRED_SATS,
        2,
        1,
        request.activated_at + time::Duration::seconds(5),
    )
    .await;
    let status = rig.status(&request.invoice).await;
    assert_eq!(status["bitcoin"]["amount_matched"], true);
    assert_eq!(status["bitcoin"]["paid_on_time"], true);
    assert!(status["usdt_arbitrum"].is_null());
    assert_eq!(status["paid_asset"], "BTC");
    assert_eq!(status["payment_state"], "confirmed");
    assert_status_fixture(&rig, &request.invoice, "btc-on-dual-request").await;
    rig.cleanup().await;
}

#[tokio::test]
async fn cross_rail_double_payment_keeps_both_observations_and_names_the_earlier_rail() {
    for (label, usdt_first, fixture_name) in [
        ("usdt first", true, "cross-rail-usdt-first"),
        ("bitcoin first", false, "cross-rail-bitcoin-first"),
    ] {
        let rig = rig(BOTH_RAILS, true).await;
        let rpc = Rpc::start().await;
        let request = offered(&rig, 0, &[]).await;
        let (usdt_offset, bitcoin_offset) = if usdt_first { (10, 60) } else { (60, 10) };
        chain(&rpc, |chain| chain.timestamp = request.at(usdt_offset));
        submit(&rig, &rpc, 0).await;
        observe_bitcoin(
            &rig,
            REQUIRED_SATS,
            1,
            1,
            request.activated_at + time::Duration::seconds(bitcoin_offset),
        )
        .await;
        let status = rig.status(&request.invoice).await;
        assert_eq!(status["bitcoin"]["paid_on_time"], true, "{label}");
        assert_eq!(status["usdt_arbitrum"]["paid_on_time"], true, "{label}");
        assert_eq!(
            status["paid_asset"],
            if usdt_first { "USDT" } else { "BTC" },
            "{label}"
        );
        assert_status_fixture(&rig, &request.invoice, fixture_name).await;
        let rows: (i64, i64) = (
            sqlx::query_scalar("SELECT count(*) FROM bitcoin_observations")
                .fetch_one(rig.database.pool())
                .await
                .unwrap(),
            sqlx::query_scalar("SELECT count(*) FROM usdt_observations")
                .fetch_one(rig.database.pool())
                .await
                .unwrap(),
        );
        assert_eq!(rows, (1, 1), "{label}");
        rig.cleanup().await;
    }
}

#[tokio::test]
async fn partial_payments_across_rails_are_never_combined() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;
    chain(&rpc, |chain| {
        transfer(chain, REQUIRED_USDT / 2);
        chain.timestamp = request.at(10);
    });
    submit(&rig, &rpc, 0).await;
    observe_bitcoin(
        &rig,
        REQUIRED_SATS / 2,
        1,
        1,
        request.activated_at + time::Duration::seconds(10),
    )
    .await;
    let status = rig.status(&request.invoice).await;
    assert_eq!(status["bitcoin"]["amount_matched"], false);
    assert_eq!(status["usdt_arbitrum"]["amount_matched"], false);
    assert!(status["paid_asset"].is_null());
    rig.cleanup().await;
}

#[tokio::test]
async fn one_transfer_belongs_to_one_request_for_good() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let first = offered(&rig, 0, &[]).await;
    let second = offered(&rig, 1, &[]).await;
    chain(&rpc, |chain| chain.timestamp = first.at(10));
    submit(&rig, &rpc, 0).await;
    submit(&rig, &rpc, 1).await;
    assert_eq!(rig.status(&first.invoice).await["paid_asset"], "USDT");
    let other = rig.status(&second.invoice).await;
    assert!(other["usdt_arbitrum"].is_null());
    assert!(other["paid_asset"].is_null());
    let owners: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT marketplace_preparation_id) FROM usdt_observations",
    )
    .fetch_one(rig.database.pool())
    .await
    .unwrap();
    assert_eq!(owners, 1);
    rig.cleanup().await;
}

#[tokio::test]
async fn a_request_that_did_not_offer_usdt_ignores_a_usdt_proof() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &["BTC"]).await;
    chain(&rpc, |chain| chain.timestamp = request.at(10));
    submit(&rig, &rpc, 0).await;
    let status = rig.status(&request.invoice).await;
    assert_eq!(status["accepted_assets"], json!(["BTC"]));
    assert!(status["usdt_arbitrum"].is_null());
    assert!(status["paid_asset"].is_null());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM usdt_observations")
        .fetch_one(rig.database.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    rig.cleanup().await;
}

#[tokio::test]
async fn a_usdt_only_request_is_settled_by_its_proof_alone() {
    let rig = rig(
        Seller {
            bitcoin: false,
            usdt: true,
        },
        true,
    )
    .await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;
    assert!(rig.store.observation_targets().await.unwrap().is_empty());
    chain(&rpc, |chain| chain.timestamp = request.at(10));
    submit(&rig, &rpc, 0).await;
    let status = rig.status(&request.invoice).await;
    assert_eq!(status["accepted_assets"], json!(["USDT"]));
    assert!(status["bitcoin"].is_null());
    assert_eq!(status["paid_asset"], "USDT");
    assert_eq!(status["payment_state"], "confirmed");
    rig.cleanup().await;
}

#[tokio::test]
async fn proof_before_activation_attribution_is_ignored_and_prepared_requests_never_match() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let (status, prepared) = rig
        .prepare_with_reference(&reference(0), "prepared-only", AMOUNT, "USD", &[], 1_800)
        .await;
    assert_eq!(status, StatusCode::OK);
    let invoice = invoice_id(&prepared);
    submit(&rig, &rpc, 0).await;
    let status = rig.status(&invoice).await;
    assert_eq!(status["state"], "prepared");
    assert!(status["usdt_arbitrum"].is_null());
    assert_eq!(status["accepted_assets"], json!(["BTC", "USDT"]));
    rig.cleanup().await;
}

/// A status read overlapping the commit that applies a proof sees the request either
/// entirely before it or entirely after it, never a half-applied mixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_reads_racing_a_proof_submission_never_see_a_half_applied_state() {
    let rig = std::sync::Arc::new(rig(BOTH_RAILS, true).await);
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;
    chain(&rpc, |chain| chain.timestamp = request.at(10));

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..3 {
        let rig = rig.clone();
        let stop = stop.clone();
        let invoice = request.invoice.clone();
        readers.push(tokio::spawn(async move {
            let mut seen = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                seen.push(rig.status(&invoice).await);
            }
            seen.push(rig.status(&invoice).await);
            seen
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    submit(&rig, &rpc, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    stop.store(true, Ordering::SeqCst);

    let mut before = 0;
    let mut after = 0;
    for reader in readers {
        let seen = reader.await.unwrap();
        let mut applied = false;
        for status in &seen {
            let has_usdt = !status["usdt_arbitrum"].is_null();
            assert!(!applied || has_usdt, "a read went back to before the proof");
            applied |= has_usdt;
            if has_usdt {
                after += 1;
                assert_eq!(status["paid_asset"], "USDT", "{status}");
                assert_eq!(status["payment_state"], "confirmed", "{status}");
                assert_eq!(status["usdt_arbitrum"]["amount_matched"], true);
                assert_eq!(status["usdt_arbitrum"]["paid_on_time"], true);
            } else {
                before += 1;
                assert!(status["paid_asset"].is_null(), "{status}");
                assert_eq!(status["payment_state"], "undetected", "{status}");
            }
        }
    }
    assert!(before > 0 && after > 0, "before={before} after={after}");
    std::sync::Arc::into_inner(rig).unwrap().cleanup().await;
}

#[tokio::test]
async fn usdt_receipts_are_authenticated_by_the_integrity_scan() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered(&rig, 0, &[]).await;
    chain(&rpc, |chain| chain.timestamp = request.at(10));
    submit(&rig, &rpc, 0).await;
    rig.store.scan_observation_integrity().await.unwrap();
    sqlx::query("UPDATE usdt_observations SET transfer_lookup_hash = $1")
        .bind(
            Crypto::from_master_key(&[7; 32])
                .unwrap()
                .lookup_hash(b"other")
                .as_bytes()
                .as_slice(),
        )
        .execute(rig.database.pool())
        .await
        .expect_err("receipt identity cannot be rewritten");
    sqlx::query("UPDATE usdt_observations SET observation_envelope = $1")
        .bind(vec![0_u8; 32])
        .execute(rig.database.pool())
        .await
        .unwrap();
    assert!(rig.store.scan_observation_integrity().await.is_err());
    let (status, _) = rig
        .post(
            STATUS,
            json!({"creator": CREATOR, "invoice_id": request.invoice}),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a tampered receipt fails closed"
    );
    rig.cleanup().await;
}

/// A pass that starts after the payment window has closed, and after Bitcoin target
/// discovery has stamped the settlement expired, is an ordinary pass: the receipt is
/// judged by its block time, not skipped and not an error.
#[tokio::test]
async fn proof_pass_after_the_settlement_expired_records_the_receipt_by_block_time() {
    for (label, block_offset, on_time) in [("on time by block time", 0, true), ("late", 5, false)] {
        let rig = rig(BOTH_RAILS, true).await;
        let rpc = Rpc::start().await;
        let request = offered_with_window(&rig, 0, &[], 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(1_300)).await;
        rig.store.observation_targets().await.unwrap();
        let expired: Option<OffsetDateTime> =
            sqlx::query_scalar("SELECT payment_expired_at FROM marketplace_settlements")
                .fetch_one(rig.database.pool())
                .await
                .unwrap();
        assert!(expired.is_some(), "{label}: discovery stamped the expiry");

        chain(&rpc, |chain| chain.timestamp = request.at(block_offset));
        submit(&rig, &rpc, 0).await;
        let status = rig.status(&request.invoice).await;
        assert_eq!(status["usdt_arbitrum"]["paid_on_time"], on_time, "{label}");
        assert_eq!(
            status["paid_asset"],
            if on_time { json!("USDT") } else { Value::Null },
            "{label}"
        );
        assert_eq!(
            status["payment_state"],
            if on_time { "confirmed" } else { "expired" },
            "{label}"
        );
        rig.cleanup().await;
    }
}

/// The settlement a proof pass locks cannot disappear or leave `active` underneath it:
/// the database refuses the state change, so a failed lock is a broken invariant.
#[tokio::test]
async fn an_active_settlement_cannot_leave_active_during_a_pass() {
    let rig = rig(BOTH_RAILS, true).await;
    let rpc = Rpc::start().await;
    let request = offered_with_window(&rig, 0, &[], 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1_300)).await;
    rig.store.observation_targets().await.unwrap();
    for statement in [
        "UPDATE marketplace_payment_preparations SET state = 'voided', activated_at = NULL, payment_deadline = NULL, voided_at = NOW()",
        "UPDATE marketplace_payment_preparations SET state = 'prepared', activated_at = NULL, payment_deadline = NULL",
        "DELETE FROM marketplace_settlements",
    ] {
        sqlx::query(statement)
            .execute(rig.database.pool())
            .await
            .expect_err(statement);
    }
    chain(&rpc, |chain| chain.timestamp = request.at(0));
    submit(&rig, &rpc, 0).await;
    assert_eq!(rig.status(&request.invoice).await["paid_asset"], "USDT");
    rig.cleanup().await;
}
