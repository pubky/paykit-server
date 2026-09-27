use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use axum::{
    Extension,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use paykit_server::{
    application::{
        payment_request_status::{
            PaymentRequestStatusError, PaymentRequestStatusOperations, PaymentRequestStatusSummary,
            PaymentState,
        },
        payment_status::{PaymentStatusError, PaymentStatusService},
    },
    config::{Config, ConfigEnvironment},
    domain::{
        locks::{BundleId, CreatorPubky},
        payment_request_lifecycle::PaymentRequestLifecycleState,
    },
    http::{auth::SignedLocksAuth, status::status_router},
};
use time::OffsetDateTime;
use tower::ServiceExt;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

struct FakeStatusOperations {
    status: Result<Option<PaymentRequestStatusSummary>, PaymentRequestStatusError>,
    calls: AtomicUsize,
}

#[async_trait]
impl PaymentRequestStatusOperations for FakeStatusOperations {
    async fn lookup(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
    ) -> Result<Option<PaymentRequestStatusSummary>, PaymentRequestStatusError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.status
    }
}

fn service(
    status: Result<Option<PaymentRequestStatusSummary>, PaymentRequestStatusError>,
) -> (Arc<PaymentStatusService>, Arc<FakeStatusOperations>) {
    let operations = Arc::new(FakeStatusOperations {
        status,
        calls: AtomicUsize::default(),
    });
    (
        Arc::new(PaymentStatusService::from_canonical(operations.clone())),
        operations,
    )
}

fn status(
    request_state: PaymentRequestLifecycleState,
    payment_state: PaymentState,
    confirmations: u32,
    amount_matched: bool,
) -> PaymentRequestStatusSummary {
    let created_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    PaymentRequestStatusSummary::new(
        request_state,
        payment_state,
        created_at,
        created_at + time::Duration::hours(24),
        confirmations,
        amount_matched,
    )
}

fn config_for(key: &SigningKey) -> Config {
    let key = pubky::PublicKey::from(
        pubky::pkarr::PublicKey::try_from(key.verifying_key().as_bytes()).unwrap(),
    )
    .to_string();
    Config::from_toml_and_environment(
        &format!(
            r#"
[http]
listen_addr = "127.0.0.1:8080"
[locks]
trusted_public_key = "{key}"
[setup]
allowed_origins = ["https://app.example"]
[paykit]
client_id = "app.paykit.server"
receiver_path = "paykit/server"
network = "testnet"
[bitcoin]
network = "testnet"
[electrum]
endpoint = "ssl://electrum.example:50002"
[outbox]
poll_interval = "5s"
[limits]
request_body_bytes = 16384
[rate_limits]
signed_requests_per_second = 100
signed_burst = 200
"#,
        ),
        ConfigEnvironment {
            database_url: Some("postgres://paykit:secret@localhost/paykit".to_owned()),
            master_key: Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE".to_owned()),
        },
    )
    .unwrap()
}

fn router(
    key: &SigningKey,
    status: Result<Option<PaymentRequestStatusSummary>, PaymentRequestStatusError>,
) -> axum::Router {
    let (service, _) = service(status);
    status_router(service).layer(Extension(Arc::new(SignedLocksAuth::from_config(
        &config_for(key),
    ))))
}

fn signed_request(key: &SigningKey) -> Request<Body> {
    signed_request_with_body(
        key,
        format!(r#"{{"bundle_id":"{BUNDLE}","creator":"{CREATOR}"}}"#).into_bytes(),
    )
}

fn signed_request_with_body(key: &SigningKey, body: Vec<u8>) -> Request<Body> {
    let preimage =
        paykit_server::http::auth::signature_preimage("POST", "/transactions/status", &body);
    Request::builder()
        .method(Method::POST)
        .uri("/transactions/status")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes()),
        )
        .body(Body::from(body))
        .unwrap()
}

async fn response_body(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 32 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn unknown_creator_or_bundle_returns_a_safe_404() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let response = router(&key, Ok(None))
        .oneshot(signed_request(&key))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn known_unobserved_status_is_exactly_undetected() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let response = router(
        &key,
        Ok(Some(status(
            PaymentRequestLifecycleState::Accepted,
            PaymentState::Undetected,
            0,
            false,
        ))),
    )
    .oneshot(signed_request(&key))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_body(response).await,
        r#"{"status":"undetected","confirmations":0,"amount_matched":false}"#
    );
}

#[tokio::test]
async fn cancelled_request_preserves_observed_bitcoin_facts() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let response = router(
        &key,
        Ok(Some(status(
            PaymentRequestLifecycleState::Canceled,
            PaymentState::Confirmed,
            3,
            true,
        ))),
    )
    .oneshot(signed_request(&key))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_body(response).await,
        r#"{"status":"cancelled","confirmations":3,"amount_matched":true}"#
    );
}

#[tokio::test]
async fn expired_request_preserves_observed_bitcoin_facts() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let response = router(
        &key,
        Ok(Some(status(
            PaymentRequestLifecycleState::Accepted,
            PaymentState::Expired,
            1,
            true,
        ))),
    )
    .oneshot(signed_request(&key))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_body(response).await,
        r#"{"status":"expired","confirmations":1,"amount_matched":true}"#
    );
}

#[tokio::test]
async fn known_observed_statuses_serialize_only_factual_fields() {
    for (status, expected) in [
        (
            status(
                PaymentRequestLifecycleState::Accepted,
                PaymentState::Detected,
                0,
                false,
            ),
            r#"{"status":"detected","confirmations":0,"amount_matched":false}"#,
        ),
        (
            status(
                PaymentRequestLifecycleState::Accepted,
                PaymentState::Confirmed,
                3,
                true,
            ),
            r#"{"status":"confirmed","confirmations":3,"amount_matched":true}"#,
        ),
    ] {
        let key = SigningKey::from_bytes(&[7; 32]);
        let response = router(&key, Ok(Some(status)))
            .oneshot(signed_request(&key))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body(response).await, expected);
    }
}

#[tokio::test]
async fn status_route_uses_task9_signed_authentication() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let canonical = status(
        PaymentRequestLifecycleState::Accepted,
        PaymentState::Undetected,
        0,
        false,
    );
    let valid = router(&key, Ok(Some(canonical)))
        .oneshot(signed_request(&key))
        .await
        .unwrap();
    assert_eq!(valid.status(), StatusCode::OK);

    let invalid = router(&key, Ok(Some(canonical)))
        .oneshot(signed_request(&SigningKey::from_bytes(&[8; 32])))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn status_route_rejects_noncanonical_or_nonclosed_bodies() {
    let key = SigningKey::from_bytes(&[7; 32]);
    for body in [
        format!(r#"{{ "bundle_id":"{BUNDLE}","creator":"{CREATOR}" }}"#).into_bytes(),
        format!(r#"{{"bundle_id":"{BUNDLE}","creator":"{CREATOR}","unexpected":true}}"#)
            .into_bytes(),
    ] {
        let response = router(
            &key,
            Ok(Some(status(
                PaymentRequestLifecycleState::Accepted,
                PaymentState::Undetected,
                0,
                false,
            ))),
        )
        .oneshot(signed_request_with_body(&key, body))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn status_service_uses_the_fresh_canonical_status_operation() {
    let (service, operations) = service(Ok(Some(status(
        PaymentRequestLifecycleState::Accepted,
        PaymentState::Undetected,
        0,
        false,
    ))));
    let creator = paykit_server::domain::locks::parse_creator(CREATOR).unwrap();
    let bundle_id = paykit_server::domain::locks::parse_bundle_id(BUNDLE).unwrap();

    let response = service.status(&creator, &bundle_id).await.unwrap();

    assert_eq!(response.status(), "undetected");
    assert_eq!(operations.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn incomplete_intake_is_unavailable_instead_of_using_stale_lifecycle() {
    let (service, operations) = service(Err(PaymentRequestStatusError::Unavailable));
    let creator = paykit_server::domain::locks::parse_creator(CREATOR).unwrap();
    let bundle_id = paykit_server::domain::locks::parse_bundle_id(BUNDLE).unwrap();

    assert_eq!(
        service.status(&creator, &bundle_id).await,
        Err(PaymentStatusError::Unavailable)
    );
    assert_eq!(operations.calls.load(Ordering::SeqCst), 1);
}
