use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::Extension,
    http::{Method, Request, StatusCode},
    routing::post,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use paykit_server::{
    Server,
    config::{Config, ConfigEnvironment},
    http::auth::{AuthProcessingObserver, AuthenticatedJson, SignedServiceAuth},
    runtime::{DependencyCheck, Runtime, operational_router},
};
use serde::Deserialize;
use tower::ServiceExt;

#[derive(Default)]
struct ReadyDependency;

#[async_trait::async_trait]
impl DependencyCheck for ReadyDependency {
    async fn postgres_ready(&self) -> bool {
        true
    }
}

#[derive(Deserialize)]
struct TestRequest {
    value: u64,
}

#[derive(Default)]
struct ProcessingCounter {
    signature_verifications: AtomicUsize,
    canonicalizations: AtomicUsize,
}

impl AuthProcessingObserver for ProcessingCounter {
    fn signature_verification_started(&self) {
        self.signature_verifications.fetch_add(1, Ordering::Relaxed);
    }

    fn canonicalization_started(&self) {
        self.canonicalizations.fetch_add(1, Ordering::Relaxed);
    }
}

fn rendered_key(key: &SigningKey) -> String {
    pubky::PublicKey::from(
        pubky::pkarr::PublicKey::try_from(key.verifying_key().as_bytes()).unwrap(),
    )
    .to_string()
}

fn config_for_keys(keys: &[&SigningKey], rate: u64, burst: u64, request_body_bytes: u64) -> Config {
    let keys = keys
        .iter()
        .map(|key| format!("\"{}\"", rendered_key(key)))
        .collect::<Vec<_>>()
        .join(", ");
    Config::from_toml_and_environment(
        &format!(
            r#"
[http]
listen_addr = "127.0.0.1:8080"
[signed_services]
trusted_public_keys = [{keys}]
[setup]
allowed_origins = ["https://app.example"]
[paykit]
client_id = "app.paykit.server"
app_id = "paykit-server"
network = "testnet"
[bitcoin]
network = "testnet"
[electrum]
endpoint = "ssl://electrum.example:50002"
[outbox]
poll_interval = "5s"
[limits]
request_body_bytes = {request_body_bytes}
[rate_limits]
signed_requests_per_second = {rate}
signed_burst = {burst}
"#,
        ),
        ConfigEnvironment {
            database_url: Some("postgres://paykit:secret@localhost/paykit".to_owned()),
            master_key: Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE".to_owned()),
        },
    )
    .unwrap()
}

fn config_for(key: &SigningKey, rate: u64, burst: u64, request_body_bytes: u64) -> Config {
    config_for_keys(&[key], rate, burst, request_body_bytes)
}

fn router(key: &SigningKey, rate: u64, burst: u64) -> Router {
    let auth = Arc::new(SignedServiceAuth::from_config(&config_for(
        key,
        rate,
        burst,
        16 * 1024,
    )));
    Router::new()
        .route("/test", post(test_endpoint))
        .layer(Extension(auth))
}

fn router_with_observer(
    key: &SigningKey,
    rate: u64,
    burst: u64,
    observer: Arc<dyn AuthProcessingObserver>,
) -> Router {
    let config = config_for(key, rate, burst, 16 * 1024);
    let auth = Arc::new(SignedServiceAuth::with_observer(&config, observer));
    Router::new()
        .route("/test", post(test_endpoint))
        .layer(Extension(auth))
}

async fn test_endpoint(
    AuthenticatedJson(payload): AuthenticatedJson<TestRequest>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({"value": payload.value}))
}

#[tokio::test]
async fn every_signed_business_path_accepts_each_trusted_service_key() {
    const SIGNED_PATHS: [&str; 14] = [
        "/invoices",
        "/v2/invoices",
        "/v2/invoices/status",
        "/marketplace/payment-requests/prepare",
        "/marketplace/payment-requests/activate",
        "/marketplace/payment-requests/void",
        "/marketplace/payment-requests/resolve",
        "/connections/status",
        "/transactions/status",
        "/setup/status",
        "/payment-requests/status",
        "/payment-request-drains",
        "/payment-request-drain-lookups",
        "/payment-request-drain-cleanups",
    ];
    let first = SigningKey::from_bytes(&[7; 32]);
    let second = SigningKey::from_bytes(&[8; 32]);
    let unlisted = SigningKey::from_bytes(&[9; 32]);
    let config = config_for_keys(&[&first, &second], 100, 200, 16 * 1024);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://127.0.0.1:1/paykit")
        .unwrap();
    let router = Server::build(config, pool).await.unwrap().router();

    for path in SIGNED_PATHS {
        for key in [&first, &second] {
            let response = router
                .clone()
                .oneshot(signed_request_for_path(
                    key,
                    path,
                    br#"{"value":1}"#.as_slice(),
                ))
                .await
                .unwrap();
            assert_ne!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            assert_ne!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
        let response = router
            .clone()
            .oneshot(signed_request_for_path(
                &unlisted,
                path,
                br#"{"value":1}"#.as_slice(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
    }
}

fn signed_request(key: &SigningKey, body: impl Into<Vec<u8>>) -> Request<Body> {
    signed_request_for_path(key, "/test", body)
}

fn signed_request_for_path(
    key: &SigningKey,
    path: &str,
    body: impl Into<Vec<u8>>,
) -> Request<Body> {
    let body = body.into();
    let preimage = paykit_server::http::auth::signature_preimage("POST", path, &body);
    let signature = URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes());
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("X-Paykit-Signature", signature)
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
async fn missing_malformed_and_invalid_signatures_have_the_same_safe_401_envelope() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let router = router(&key, 100, 200);
    let body = br#"{"value":1}"#;
    let preimage = paykit_server::http::auth::signature_preimage("POST", "/test", body);
    let missing = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .body(Body::from(body.as_slice()))
        .unwrap();
    let malformed = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .header("X-Paykit-Signature", "not_base64!")
        .body(Body::from(body.as_slice()))
        .unwrap();
    let padded = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .header(
            "X-Paykit-Signature",
            format!(
                "{}=",
                URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes())
            ),
        )
        .body(Body::from(body.as_slice()))
        .unwrap();
    let duplicate_signature = URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes());
    let duplicate = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .header("X-Paykit-Signature", &duplicate_signature)
        .header("X-Paykit-Signature", &duplicate_signature)
        .body(Body::from(body.as_slice()))
        .unwrap();
    let invalid = signed_request(&SigningKey::from_bytes(&[8; 32]), body.as_slice());

    for request in [missing, malformed, padded, duplicate, invalid] {
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_body(response).await,
            r#"{"error":{"code":"invalid_signature","message":"request authentication failed"}}"#
        );
    }
}

#[tokio::test]
async fn exact_path_prefix_is_part_of_the_signed_preimage() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let auth = Arc::new(SignedServiceAuth::from_config(&config_for(
        &key,
        100,
        200,
        16 * 1024,
    )));
    let router = Router::new()
        .route("/api/paykit/test", post(test_endpoint))
        .layer(Extension(auth));
    let body = br#"{"value":1}"#;

    let accepted = router
        .clone()
        .oneshot(signed_request_for_path(
            &key,
            "/api/paykit/test",
            body.as_slice(),
        ))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);

    let wrong_preimage = paykit_server::http::auth::signature_preimage("POST", "/test", body);
    let wrong_prefix = Request::builder()
        .method(Method::POST)
        .uri("/api/paykit/test")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&wrong_preimage).to_bytes()),
        )
        .body(Body::from(body.as_slice()))
        .unwrap();
    assert_eq!(
        router.oneshot(wrong_prefix).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn body_only_and_wrong_method_or_path_signatures_are_rejected() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let body = br#"{"value":1}"#;
    let body_only = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(body).to_bytes()),
        )
        .body(Body::from(body.as_slice()))
        .unwrap();
    let wrong_method_preimage = paykit_server::http::auth::signature_preimage("GET", "/test", body);
    let wrong_method = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&wrong_method_preimage).to_bytes()),
        )
        .body(Body::from(body.as_slice()))
        .unwrap();
    let wrong_path_preimage = paykit_server::http::auth::signature_preimage("POST", "/other", body);
    let wrong_path = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&wrong_path_preimage).to_bytes()),
        )
        .body(Body::from(body.as_slice()))
        .unwrap();

    for request in [body_only, wrong_method, wrong_path] {
        assert_eq!(
            router(&key, 100, 200)
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn signed_query_string_is_rejected_even_when_path_and_body_match() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let body = br#"{"value":1}"#;
    let preimage = paykit_server::http::auth::signature_preimage("POST", "/test", body);
    let request = Request::builder()
        .method(Method::POST)
        .uri("/test?unsigned=true")
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes()),
        )
        .body(Body::from(body.as_slice()))
        .unwrap();

    assert_eq!(
        router(&key, 100, 200)
            .oneshot(request)
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn signature_is_verified_over_raw_bytes_before_json_is_parsed() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let response = router(&key, 100, 200)
        .oneshot(signed_request(&key, br#"{"value":not-json}"#.as_slice()))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body(response).await,
        r#"{"error":{"code":"invalid_request","message":"request is invalid"}}"#
    );
}

#[tokio::test]
async fn signed_noncanonical_malformed_and_schema_invalid_json_are_safe_400s() {
    let key = SigningKey::from_bytes(&[7; 32]);
    for body in [
        br#"{ "value": 1 }"#.as_slice(),
        br#"{"value":"not-a-number"}"#.as_slice(),
        br#"{"value":1,"value":2}"#.as_slice(),
    ] {
        let response = router(&key, 100, 200)
            .oneshot(signed_request(&key, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_body(response).await,
            r#"{"error":{"code":"invalid_request","message":"request is invalid"}}"#
        );
    }
}

#[tokio::test]
async fn signed_unknown_field_is_rejected_for_a_plain_serde_dto() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let response = router(&key, 100, 200)
        .oneshot(signed_request(
            &key,
            br#"{"value":1,"unknown":true}"#.as_slice(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body(response).await,
        r#"{"error":{"code":"invalid_request","message":"request is invalid"}}"#
    );
}

#[tokio::test]
async fn provider_v2_routes_enforce_signed_closed_canonical_wire_contract() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let untrusted = SigningKey::from_bytes(&[8; 32]);
    let config = config_for(&key, 100, 200, 16 * 1024);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://127.0.0.1:1/paykit")
        .unwrap();
    let router = Server::build(config, pool).await.unwrap().router();
    let creator = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    let bundle = "000G40R40M30E209185GR38E1W";
    let lock_id = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    let valid = format!(
        r#"{{"bundle_id":"{bundle}","deadline":"2030-01-01T00:00:00Z","lock_resource":"{creator}/pub/app.locks/{lock_id}.json","payment":{{"amount":"1","asset":"BTC","minimum_confirmations":0,"recipient_pubky":"{creator}"}},"reader":"{creator}"}}"#,
    );

    let unsigned = Request::builder()
        .method(Method::POST)
        .uri("/v2/invoices")
        .body(Body::from(valid.clone()))
        .unwrap();
    let malformed = Request::builder()
        .method(Method::POST)
        .uri("/v2/invoices")
        .header("X-Paykit-Signature", "not_base64!")
        .body(Body::from(valid.clone()))
        .unwrap();
    for request in [
        unsigned,
        malformed,
        signed_request_for_path(&untrusted, "/v2/invoices", valid.as_bytes()),
    ] {
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    let invalid_bodies = [
        format!("{{ {} }}", &valid[1..valid.len() - 1]),
        valid.replace("\"BTC\"", "\"btc\""),
        valid.replace("\"amount\":\"1\"", "\"amount\":\"0\""),
        valid.replace("\"minimum_confirmations\":0", "\"minimum_confirmations\":7"),
        valid.replace("2030-01-01T00:00:00Z", "2030-01-01T01:00:00+01:00"),
        format!("{},\"unknown\":true}}", &valid[..valid.len() - 1]),
    ];
    for body in invalid_bodies {
        let response = router
            .clone()
            .oneshot(signed_request_for_path(
                &key,
                "/v2/invoices",
                body.as_bytes(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
    }
    let query = router
        .clone()
        .oneshot(signed_request_for_path(
            &key,
            "/v2/invoices?unexpected=true",
            valid.as_bytes(),
        ))
        .await
        .unwrap();
    assert_eq!(query.status(), StatusCode::BAD_REQUEST);

    let status_unknown =
        format!(r#"{{"bundle_id":"{bundle}","creator":"{creator}","unknown":true}}"#,);
    assert_eq!(
        router
            .oneshot(signed_request_for_path(
                &key,
                "/v2/invoices/status",
                status_unknown.as_bytes(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn provider_v2_signature_vector_is_byte_exact() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let preimage = paykit_server::http::auth::signature_preimage("POST", "/v2/invoices", b"{}");
    assert_eq!(
        preimage,
        b"paykit-http-signature-v1\0POST\0/v2/invoices\0{}"
    );
    assert_eq!(
        URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes()),
        "UR4WdtdlFSOXLmImDszN6FyQ68-SY2lcFpDlzAmKvwcKuNIJW8VbxBw4o5-JRYhDyeTNALR4li3FfSL5MfLlBw"
    );
}

#[tokio::test]
async fn raw_body_limit_precedes_signature_and_json_processing() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let oversized = vec![b'x'; 16 * 1024 + 1];
    let request = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .body(Body::from(oversized))
        .unwrap();

    let response = router(&key, 100, 200).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response_body(response).await,
        r#"{"error":{"code":"payload_too_large","message":"request body is too large"}}"#
    );
}

#[tokio::test]
async fn configured_signed_body_limit_is_enforced() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let auth = Arc::new(SignedServiceAuth::from_config(&config_for(
        &key, 100, 200, 8,
    )));
    let router = Router::new()
        .route("/test", post(test_endpoint))
        .layer(Extension(auth));

    let response = router
        .oneshot(signed_request(&key, br#"{"value":1}"#.as_slice()))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn global_signed_request_limiter_allows_burst_then_returns_safe_429() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let router = router(&key, 1, 2);
    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(signed_request(&key, br#"{"value":1}"#.as_slice()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let response = router
        .oneshot(signed_request(&key, br#"{"value":1}"#.as_slice()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "1");
    assert_eq!(
        response_body(response).await,
        r#"{"error":{"code":"rate_limited","message":"request rate limit exceeded"}}"#
    );
}

#[tokio::test]
async fn invalid_and_schema_invalid_requests_do_not_consume_signed_policy_capacity() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let observer = Arc::new(ProcessingCounter::default());
    let router = router_with_observer(&key, 1, 1, observer.clone());

    let wrong_key = SigningKey::from_bytes(&[8; 32]);
    let invalid_requests = [
        Request::builder()
            .method(Method::POST)
            .uri("/test")
            .body(Body::from(br#"{"value":1}"#.as_slice()))
            .unwrap(),
        Request::builder()
            .method(Method::POST)
            .uri("/test")
            .header("X-Paykit-Signature", "not_base64!")
            .body(Body::from(br#"{"value":1}"#.as_slice()))
            .unwrap(),
        signed_request(&wrong_key, br#"{"value":1}"#.as_slice()),
        signed_request(&key, br#"{not-json}"#.as_slice()),
        signed_request(&key, br#"{"value":"wrong"}"#.as_slice()),
        signed_request(&key, br#"{"value":1,"unknown":true}"#.as_slice()),
    ];
    for request in invalid_requests {
        let response = router.clone().oneshot(request).await.unwrap();
        assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    let accepted = router
        .clone()
        .oneshot(signed_request(&key, br#"{"value":1}"#.as_slice()))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);

    let limited = router
        .oneshot(signed_request(&key, br#"{"value":1}"#.as_slice()))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn operational_capacity_rejects_before_signature_verification() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let observer = Arc::new(ProcessingCounter::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let auth = Arc::new(SignedServiceAuth::with_observer(
        &config_for(&key, 100, 200, 16 * 1024),
        observer.clone(),
    ));
    let handler_entered = entered.clone();
    let handler_release = release.clone();
    let auth_router = Router::new()
        .route(
            "/test",
            post(
                move |AuthenticatedJson(_): AuthenticatedJson<TestRequest>| {
                    let entered = handler_entered.clone();
                    let release = handler_release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        StatusCode::OK
                    }
                },
            ),
        )
        .layer(Extension(auth));
    let router = operational_router(
        auth_router,
        Arc::new(Runtime::new(Arc::new(ReadyDependency), 1)),
    );

    let entered_wait = entered.notified();
    let first_router = router.clone();
    let first_key = key.clone();
    let first = tokio::spawn(async move {
        first_router
            .oneshot(signed_request(&first_key, br#"{"value":1}"#.as_slice()))
            .await
            .unwrap()
    });
    entered_wait.await;
    assert_eq!(observer.signature_verifications.load(Ordering::Relaxed), 1);

    let rejected = router
        .oneshot(signed_request(&key, br#"{"value":1}"#.as_slice()))
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(rejected.headers()["retry-after"], "1");
    assert_eq!(observer.signature_verifications.load(Ordering::Relaxed), 1);

    release.notify_one();
    assert_eq!(first.await.unwrap().status(), StatusCode::OK);
}

#[test]
fn authentication_state_debug_redacts_every_trusted_public_key() {
    let first = SigningKey::from_bytes(&[7; 32]);
    let second = SigningKey::from_bytes(&[8; 32]);
    let auth =
        SignedServiceAuth::from_config(&config_for_keys(&[&first, &second], 100, 200, 16 * 1024));
    let rendered = format!("{auth:?}");

    assert!(!rendered.contains(&rendered_key(&first)));
    assert!(!rendered.contains(&rendered_key(&second)));
}
