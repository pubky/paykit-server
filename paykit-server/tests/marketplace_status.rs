use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Extension,
    body::Body,
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use paykit_server::{
    application::marketplace_status::{
        MarketplaceBitcoinStatus, MarketplaceInvoiceStatus, MarketplaceStatusError,
        MarketplaceStatusPersistence, MarketplaceStatusService,
    },
    config::{Config, ConfigEnvironment},
    domain::locks::CreatorPubky,
    http::{
        auth::{SignedServiceAuth, signature_preimage},
        marketplace_status::marketplace_status_router,
    },
};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const INVOICE_ID: Uuid = Uuid::from_u128(0x4c6e66c7_8a8b_4d54_b97b_340cedb3e1d0);

struct FakeStore {
    result: Result<Option<MarketplaceInvoiceStatus>, MarketplaceStatusError>,
}

#[async_trait]
impl MarketplaceStatusPersistence for FakeStore {
    async fn status(
        &self,
        _creator: &CreatorPubky,
        _invoice_id: Uuid,
    ) -> Result<Option<MarketplaceInvoiceStatus>, MarketplaceStatusError> {
        self.result.clone()
    }
}

fn active_status() -> MarketplaceInvoiceStatus {
    MarketplaceInvoiceStatus::active(
        INVOICE_ID,
        "delivered",
        Some("accepted"),
        "confirmed",
        OffsetDateTime::UNIX_EPOCH,
        OffsetDateTime::UNIX_EPOCH + time::Duration::days(1),
        Some(MarketplaceBitcoinStatus::new(
            "0000000000000000000000000000000000000000000000000000000000000001".into(),
            2,
            50_000,
            OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            3,
            true,
            true,
            true,
        )),
        Some("paid_manually"),
        Some(OffsetDateTime::UNIX_EPOCH + time::Duration::hours(2)),
    )
}

fn signed_auth(key: &SigningKey) -> Arc<SignedServiceAuth> {
    let key = pubky::PublicKey::from(
        pubky::pkarr::PublicKey::try_from(key.verifying_key().as_bytes()).unwrap(),
    )
    .to_string();
    let config = Config::from_toml_and_environment(
        &format!(
            r#"
[http]
listen_addr = "127.0.0.1:8080"
[signed_services]
trusted_public_keys = ["{key}"]
[setup]
allowed_origins = ["https://app.example"]
[paykit]
client_id = "app.paykit.server"
app_id = "paykit-server"
network = "testnet"
[bitcoin]
network = "mainnet"
[electrum]
endpoint = "ssl://electrum.example:50002"
[outbox]
poll_interval = "5s"
"#
        ),
        ConfigEnvironment {
            database_url: Some("postgres://paykit:***@localhost/paykit".into()),
            master_key: Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE".into()),
        },
    )
    .unwrap();
    Arc::new(SignedServiceAuth::from_config(&config))
}

fn request(key: &SigningKey, value: serde_json::Value) -> Request<Body> {
    const PATH: &str = "/marketplace/payment-requests/status";
    let body = serde_json_canonicalizer::to_vec(&value).unwrap();
    let preimage = signature_preimage("POST", PATH, &body);
    Request::builder()
        .method(Method::POST)
        .uri(PATH)
        .header(
            "X-Paykit-Signature",
            URL_SAFE_NO_PAD.encode(key.sign(&preimage).to_bytes()),
        )
        .body(Body::from(body))
        .unwrap()
}

async fn body(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn signed_status_has_exact_closed_active_fixture() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let router = marketplace_status_router(Arc::new(MarketplaceStatusService::new(Arc::new(
        FakeStore {
            result: Ok(Some(active_status())),
        },
    ))))
    .layer(Extension(signed_auth(&key)));
    let response = router
        .oneshot(request(
            &key,
            serde_json::json!({"creator":CREATOR,"invoice_id":INVOICE_ID}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body(response).await,
        serde_json::json!({
            "activated_at":"1970-01-01T00:00:00Z",
            "bitcoin":{
                "amount_matched":true,
                "confirmations":3,
                "observed_sats":50000,
                "paid_on_time":true,
                "txid":"0000000000000000000000000000000000000000000000000000000000000001"
            },
            "invoice_id":INVOICE_ID,
            "outcome":"paid_manually",
            "payment_deadline":"1970-01-02T00:00:00Z",
            "payment_state":"confirmed",
            "proposal_delivery_state":"delivered",
            "request_state":"accepted",
            "resolved_at":"1970-01-01T02:00:00Z",
            "state":"active"
        })
    );
}

#[tokio::test]
async fn unrepresentable_persisted_timestamp_is_unavailable() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let unrepresentable = time::Date::from_calendar_date(-1, time::Month::January, 1)
        .unwrap()
        .midnight()
        .assume_utc();
    let status = MarketplaceInvoiceStatus::active(
        INVOICE_ID,
        "delivered",
        Some("accepted"),
        "confirmed",
        unrepresentable,
        unrepresentable,
        None,
        None,
        None,
    );
    let response = marketplace_status_router(Arc::new(MarketplaceStatusService::new(Arc::new(
        FakeStore {
            result: Ok(Some(status)),
        },
    ))))
    .layer(Extension(signed_auth(&key)))
    .oneshot(request(
        &key,
        serde_json::json!({"creator":CREATOR,"invoice_id":INVOICE_ID}),
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body(response).await["error"]["code"], "unavailable");
}

#[tokio::test]
async fn status_rejects_unknown_fields_and_maps_closed_errors() {
    let key = SigningKey::from_bytes(&[11; 32]);
    let invalid = marketplace_status_router(Arc::new(MarketplaceStatusService::new(Arc::new(
        FakeStore { result: Ok(None) },
    ))))
    .layer(Extension(signed_auth(&key)))
    .oneshot(request(
        &key,
        serde_json::json!({"creator":CREATOR,"invoice_id":INVOICE_ID,"operation_id":"forbidden"}),
    ))
    .await
    .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let mut query_request = request(
        &key,
        serde_json::json!({"creator":CREATOR,"invoice_id":INVOICE_ID}),
    );
    *query_request.uri_mut() = "/marketplace/payment-requests/status?operation_id=forbidden"
        .parse()
        .unwrap();
    let invalid_query = marketplace_status_router(Arc::new(MarketplaceStatusService::new(
        Arc::new(FakeStore { result: Ok(None) }),
    )))
    .layer(Extension(signed_auth(&key)))
    .oneshot(query_request)
    .await
    .unwrap();
    assert_eq!(invalid_query.status(), StatusCode::BAD_REQUEST);

    for (result, status, code) in [
        (Ok(None), StatusCode::NOT_FOUND, "not_found"),
        (
            Err(MarketplaceStatusError::Conflict),
            StatusCode::CONFLICT,
            "conflict",
        ),
        (
            Err(MarketplaceStatusError::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
    ] {
        let response = marketplace_status_router(Arc::new(MarketplaceStatusService::new(
            Arc::new(FakeStore { result }),
        )))
        .layer(Extension(signed_auth(&key)))
        .oneshot(request(
            &key,
            serde_json::json!({"creator":CREATOR,"invoice_id":INVOICE_ID}),
        ))
        .await
        .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(body(response).await["error"]["code"], code);
    }
}
