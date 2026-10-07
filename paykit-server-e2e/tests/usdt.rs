use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::State, routing::post};
use paykit_lib::{
    PaykitAppId, PaymentAmount, PaymentEndpointIdentifier, PaymentEndpointPayload,
    PaymentReference, PaymentRequestTerms,
};
use paykit_sdk::{PaykitIdentitySecretKey, PaymentProofRecord, PaymentRequestRecord};
use paykit_server::{
    application::semantic_intent::DeliveryIntentV1,
    crypto::Crypto,
    domain::{
        invoice::CriterionAsset,
        locks::{parse_creator, parse_reader},
        receiving::{USDT_ENDPOINT, USDT_TOKEN, UsdtAddress},
    },
    persistence::{
        AtomicInvoiceInput, CreatorCredentials, CreatorStore, InvoicePayloadFactory,
        InvoicePayloads, InvoiceStore, PersistenceError, run_migrations,
    },
    usdt::{ArbitrumVerifier, PaymentContext, VerificationError},
};
use paykit_server_e2e::postgres::TestDatabase;
use serde_json::{Value, json};
use time::OffsetDateTime;
use uuid::Uuid;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const RECIPIENT: &str = "0x2222222222222222222222222222222222222222";
const BLOCK: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn fixture(index: usize) -> Value {
    serde_json::from_str::<Value>(include_str!("fixtures/usdt-proofs.json")).unwrap()["proofs"]
        [index]
        .clone()
}

fn proof(index: usize) -> PaymentProofRecord {
    let fixture = fixture(index);
    serde_json::from_value(json!({
        "event_id": Uuid::new_v4().to_string(), "stream_item_id":1,
        "payment_reference": fixture["binding"]["paymentReference"],
        "payment_app_id":"paykit-server", "payment_endpoint_identifier":USDT_ENDPOINT,
        "proof":fixture["proof"], "recorded_at":"2026-10-07T00:00:00Z"
    }))
    .unwrap()
}

fn record(index: usize) -> PaymentRequestRecord {
    serde_json::from_value(json!({
        "counterparty":CREATOR.trim_start_matches("pubky"),
        "payment_request_id":fixture(index)["binding"]["paymentRequestId"],
        "local_role":"Payee", "state":"ProofSubmitted", "conversion_quotes":[],
        "payment_proofs":[proof(index)],
    }))
    .unwrap()
}

#[derive(Clone)]
struct Chain {
    receipt: Value,
    timestamp: i64,
    finalized: bool,
    reorg: bool,
    unavailable: bool,
}

impl Chain {
    fn new() -> Self {
        let hash = fixture(0)["proof"]["transaction_hash"].clone();
        let sender = "0x0000000000000000000000007e5f4552091a69125d5dfcb7b8c2659029395bdf";
        let topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let recipient = format!("0x{}{}", "0".repeat(24), &RECIPIENT[2..]);
        Self {
            receipt: json!({"transactionHash":hash,"status":"0x1","blockNumber":"0x64","blockHash":BLOCK,
            "logs":[{}, {"address":USDT_TOKEN,"transactionHash":hash,"blockHash":BLOCK,"removed":false,
                "logIndex":"0xf", "topics":[topic,sender,recipient],"data":format!("0x{:064x}",50_000)}]}),
            timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            finalized: false,
            reorg: false,
            unavailable: false,
        }
    }
}

struct Rpc {
    chain: Arc<Mutex<Chain>>,
    verifier: ArbitrumVerifier,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Rpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Rpc {
    async fn start() -> Self {
        async fn handle(
            State(state): State<Arc<Mutex<Chain>>>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            let chain = state.lock().unwrap();
            if chain.unavailable {
                return Json(
                    json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"unavailable"}}),
                );
            }
            let result = match request["method"].as_str().unwrap() {
                "eth_chainId" => json!("0xa4b1"),
                "eth_getTransactionReceipt" => chain.receipt.clone(),
                "eth_blockNumber" => json!("0x65"),
                "eth_getBlockByNumber" if request["params"][0] == "finalized" => {
                    json!({"number":if chain.finalized {"0x64"}else{"0x63"}})
                }
                "eth_getBlockByNumber" => {
                    json!({"number":"0x64","hash":if chain.reorg {"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}else{BLOCK},"timestamp":format!("0x{:x}",chain.timestamp)})
                }
                _ => panic!("unexpected RPC"),
            };
            Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let chain = Arc::new(Mutex::new(Chain::new()));
        let app = Router::new()
            .route("/", post(handle))
            .with_state(chain.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            chain,
            verifier: ArbitrumVerifier::new(url).unwrap(),
            task,
        }
    }
}

#[tokio::test]
async fn account_attestation_and_canonical_receipt_are_both_required() {
    let rpc = Rpc::start().await;
    let recipient = UsdtAddress::try_from(RECIPIENT.to_owned()).unwrap();
    let f = fixture(0);
    let context = PaymentContext {
        payer: CREATOR.trim_start_matches("pubky"),
        payee: CREATOR.trim_start_matches("pubky"),
        request_id: f["binding"]["paymentRequestId"].as_str().unwrap(),
        reference: f["binding"]["paymentReference"].as_str().unwrap(),
        recipient: &recipient,
    };
    let valid = proof(0);
    assert!(
        rpc.verifier
            .verify(&context, &valid)
            .await
            .unwrap()
            .is_some()
    );
    for field in ["chain_id", "signature", "receipt_log_index"] {
        let mut wrong = valid.clone();
        wrong.proof.insert(field.into(), json!("0"));
        assert!(matches!(
            rpc.verifier.verify(&context, &wrong).await,
            Err(VerificationError::InvalidProof)
        ));
    }
    assert!(matches!(
        rpc.verifier.verify(&context, &proof(1)).await,
        Err(VerificationError::InvalidProof)
    ));
    let good = rpc.chain.lock().unwrap().clone();
    for mutation in ["token", "recipient", "sender", "reverted"] {
        let mut chain = good.clone();
        match mutation {
            "token" => chain.receipt["logs"][1]["address"] = json!(RECIPIENT),
            "recipient" => chain.receipt["logs"][1]["topics"][2] = json!(BLOCK),
            "sender" => chain.receipt["logs"][1]["topics"][1] = json!(BLOCK),
            _ => chain.receipt["status"] = json!("0x0"),
        }
        *rpc.chain.lock().unwrap() = chain;
        assert!(
            matches!(
                rpc.verifier.verify(&context, &valid).await,
                Err(VerificationError::InvalidProof)
            ),
            "{mutation}"
        );
    }
    *rpc.chain.lock().unwrap() = good;
    rpc.chain.lock().unwrap().reorg = true;
    assert!(
        rpc.verifier
            .verify(&context, &valid)
            .await
            .unwrap()
            .is_none()
    );
    rpc.chain.lock().unwrap().receipt = Value::Null;
    assert!(
        rpc.verifier
            .verify(&context, &valid)
            .await
            .unwrap()
            .is_none()
    );
    rpc.chain.lock().unwrap().unavailable = true;
    assert!(matches!(
        rpc.verifier.verify(&context, &valid).await,
        Err(VerificationError::Unavailable)
    ));
}

struct Payload {
    index: usize,
}
impl InvoicePayloadFactory for Payload {
    fn for_child_index(&self, _index: i64) -> Result<InvoicePayloads, PersistenceError> {
        let endpoint = PaymentEndpointIdentifier::new(USDT_ENDPOINT).unwrap();
        let address = UsdtAddress::try_from(RECIPIENT.to_owned()).unwrap();
        let terms = PaymentRequestTerms::builder(
            PaymentAmount::new("0.050000", "usdt").unwrap(),
            PaymentReference::new(
                fixture(self.index)["binding"]["paymentReference"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap(),
            vec![endpoint.clone()],
        )
        .required_app_id(Some(PaykitAppId::new("paykit-server").unwrap()))
        .payment_endpoints(Some(
            [(
                endpoint,
                PaymentEndpointPayload::new(address.endpoint().to_string()),
            )]
            .into_iter()
            .collect(),
        ))
        .build()
        .unwrap();
        Ok(InvoicePayloads {
            payment_request_intent: DeliveryIntentV1::payment_request(
                CREATOR.to_owned(),
                PaykitAppId::new("paykit-server").unwrap(),
                &terms,
            )
            .unwrap(),
            receiving_address: RECIPIENT.into(),
            asset: CriterionAsset::Usdt,
        })
    }
}

#[tokio::test]
async fn invoices_share_an_address_but_never_share_payment_evidence() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let crypto = Arc::new(Crypto::from_master_key(&[7; 32]).unwrap());
    let creator = parse_creator(CREATOR).unwrap();
    let creator_id = CreatorStore::new(database.pool(), crypto.clone())
        .create(&CreatorCredentials::new(
            creator.clone(),
            "test-session".into(),
            PaykitIdentitySecretKey::new([9; 32], 1).unwrap(),
            None,
            Some(UsdtAddress::try_from(RECIPIENT.to_owned()).unwrap()),
        ))
        .await
        .unwrap()
        .id();
    let store = InvoiceStore::new(database.pool(), crypto.clone());
    let mut ids = Vec::new();
    for index in 0..2 {
        let binding = format!("bundle-{index}");
        let invoice = store
            .create_atomic(AtomicInvoiceInput {
                creator: &creator,
                reader: &parse_reader(CREATOR).unwrap(),
                bundle_binding: binding.as_bytes(),
                lock_resource_binding: b"lock",
                payment_request_binding: binding.as_bytes(),
                invoice_payloads: &Payload { index },
                required_amount: 50_000,
                proposal_acceptance_seconds: 3600,
                payment_window_seconds: 86400,
            })
            .await
            .unwrap();
        let id = invoice.invoice_id();
        ids.push(id);
        // Establish the same durable attribution produced by the SDK lifecycle projection.
        sqlx::query("INSERT INTO payment_request_lifecycles (invoice_id,sdk_payment_request_id,request_state,last_event_at,last_stream_item_id) VALUES ($1,$2,'proof_submitted',NOW(),1)").bind(id).bind(record(index).payment_request_id).execute(database.pool()).await.unwrap();
    }
    let rpc = Rpc::start().await;
    let paid_at = rpc.chain.lock().unwrap().timestamp;
    for (amount, offset, matched) in [
        (49_999, 0, false),
        (50_001, 86_401, false),
        (50_001, 0, true),
    ] {
        {
            let mut chain = rpc.chain.lock().unwrap();
            chain.receipt["logs"][1]["data"] = json!(format!("0x{amount:064x}"));
            chain.timestamp = paid_at + offset;
        }
        store
            .observe_usdt_request(creator_id, &creator, &record(0), &rpc.verifier, None)
            .await
            .unwrap();
        assert_eq!(
            status(&database, ids[0]).await,
            (matched, 2),
            "amount={amount}, offset={offset}"
        );
    }
    store.scan_payment_record_integrity().await.unwrap();
    // Chain timestamps have second precision; invoice timestamps retain subsecond precision.
    let mut evidence = record(0);
    let mut incorrect = proof(1);
    incorrect.payment_reference = evidence.payment_proofs[0].payment_reference.clone();
    evidence.payment_proofs.insert(0, incorrect.clone());
    evidence.payment_proofs.push(incorrect);
    store
        .observe_usdt_request(creator_id, &creator, &evidence, &rpc.verifier, None)
        .await
        .unwrap();
    assert_eq!(status(&database, ids[0]).await, (true, 2));
    // An independently valid second signature over the same receipt cannot buy another lock.
    store
        .observe_usdt_request(creator_id, &creator, &record(1), &rpc.verifier, None)
        .await
        .unwrap();
    assert_eq!(status(&database, ids[1]).await, (false, 0));
    let restarted = InvoiceStore::new(database.pool(), crypto);
    rpc.chain.lock().unwrap().reorg = true;
    restarted
        .observe_usdt_request(creator_id, &creator, &evidence, &rpc.verifier, None)
        .await
        .unwrap();
    assert_eq!(status(&database, ids[0]).await, (false, 0));
    let first_match: Option<OffsetDateTime> =
        sqlx::query_scalar("SELECT first_amount_matched_observed_at FROM invoices WHERE id=$1")
            .bind(ids[0])
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert!(first_match.is_some(), "drain history survives a reorg");
    rpc.chain.lock().unwrap().reorg = false;
    rpc.chain.lock().unwrap().finalized = true;
    restarted
        .observe_usdt_request(creator_id, &creator, &evidence, &rpc.verifier, None)
        .await
        .unwrap();
    rpc.chain.lock().unwrap().unavailable = true;
    restarted
        .observe_usdt_request(creator_id, &creator, &evidence, &rpc.verifier, None)
        .await
        .unwrap();
    assert_eq!(status(&database, ids[0]).await, (true, 2));
    database.cleanup().await;
}

async fn status(database: &TestDatabase, id: Uuid) -> (bool, i32) {
    sqlx::query_as("SELECT amount_matched,confirmation_count FROM invoices WHERE id=$1")
        .bind(id)
        .fetch_one(database.pool())
        .await
        .unwrap()
}
