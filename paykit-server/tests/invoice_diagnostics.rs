use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use paykit_server::{
    http::error::ApiError,
    runtime::{DependencyCheck, Runtime, operational_router},
};
use tower::ServiceExt;
use tracing::{Event, Subscriber, instrument::WithSubscriber};
use tracing_subscriber::{EnvFilter, Layer, fmt::MakeWriter, layer::Context, prelude::*};
use uuid::{Uuid, Version};

const REQUEST_ID_HEADER: &str = "x-request-id";

type CapturedEvents = Vec<Vec<(String, String)>>;

#[derive(Clone, Default)]
struct EventCapture(Arc<Mutex<CapturedEvents>>);

#[derive(Clone, Default)]
struct JsonWriter(Arc<Mutex<Vec<u8>>>);

struct JsonWriteGuard(JsonWriter);

impl Write for JsonWriteGuard {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for JsonWriter {
    type Writer = JsonWriteGuard;

    fn make_writer(&'a self) -> Self::Writer {
        JsonWriteGuard(self.clone())
    }
}

impl<S> Layer<S> for EventCapture
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut fields = Vec::new();
        event.record(&mut FieldVisitor(&mut fields));
        self.0.lock().unwrap().push(fields);
    }
}

struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

impl EventCapture {
    fn invoice_events(&self) -> Vec<Vec<(String, String)>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|fields| field(fields, "event") == Some("paykit_invoice_outcome"))
            .cloned()
            .collect()
    }
}

fn field<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.trim_matches('"'))
}

struct ReadyDependency;

#[async_trait]
impl DependencyCheck for ReadyDependency {
    async fn postgres_ready(&self) -> bool {
        true
    }
}

fn runtime() -> Arc<Runtime> {
    Arc::new(Runtime::new(Arc::new(ReadyDependency), 1))
}

fn request(request_id: &str) -> Request<Body> {
    request_for("/invoices", request_id)
}

fn request_for(path: &str, request_id: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(REQUEST_ID_HEADER, request_id)
        .body(Body::empty())
        .unwrap()
}

async fn intentional_panic_handler() -> StatusCode {
    panic!("intentional handler panic")
}

#[tokio::test]
async fn cancelled_status_request_emits_one_safe_event_without_fabricated_response() {
    let request_id = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let entered = Arc::new(tokio::sync::Notify::new());
    let handler_entered = entered.clone();
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let router = operational_router(
        Router::new().route(
            "/payment-requests/status",
            post(move || {
                let entered = handler_entered.clone();
                async move {
                    entered.notify_one();
                    std::future::pending::<StatusCode>().await
                }
            }),
        ),
        runtime(),
    );

    let _subscriber_guard = tracing::subscriber::set_default(subscriber);
    let request = tokio::spawn(router.oneshot(request_for("/payment-requests/status", request_id)));
    entered.notified().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());

    let events = capture.0.lock().unwrap();
    let events = events
        .iter()
        .filter(|fields| field(fields, "event") == Some("paykit_locks_status_outcome"))
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 1);
    let event = events[0];
    assert_eq!(field(event, "operation"), Some("payment_request_status"));
    assert_eq!(field(event, "failure_class"), Some("cancelled"));
    assert_eq!(field(event, "request_id"), Some(request_id));
    assert!(field(event, "elapsed_ms").unwrap().parse::<u64>().is_ok());
    assert_eq!(field(event, "http_status"), None);
    assert!(event.iter().all(|(name, _)| {
        matches!(
            name.as_str(),
            "message" | "event" | "operation" | "failure_class" | "elapsed_ms" | "request_id"
        )
    }));
}

#[tokio::test]
async fn panicking_invoice_request_emits_one_safe_event_without_fabricated_response() {
    let request_id = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let router = operational_router(
        Router::new().route("/invoices", post(intentional_panic_handler)),
        runtime(),
    );

    let request = tokio::spawn(
        router
            .oneshot(request(request_id))
            .with_subscriber(subscriber),
    );
    assert!(request.await.unwrap_err().is_panic());

    let events = capture.0.lock().unwrap();
    let warnings = events
        .iter()
        .filter(|event| field(event, "failure_class") == Some("panic"))
        .collect::<Vec<_>>();
    assert_eq!(warnings.len(), 1);
    let event = warnings[0];
    assert_eq!(field(event, "event"), Some("paykit_http_request_completed"));
    assert_eq!(field(event, "outcome"), Some("panic"));
    assert_eq!(field(event, "request_id"), Some(request_id));
    assert!(Uuid::parse_str(field(event, "request_instance_id").unwrap()).is_ok());
    assert!(field(event, "elapsed_ms").unwrap().parse::<u64>().is_ok());
    assert_eq!(field(event, "http_status"), None);
    assert_eq!(field(event, "operation"), Some("invoice_create"));
    assert!(
        events
            .iter()
            .all(|event| field(event, "event") != Some("paykit_invoice_outcome"))
    );
}

#[tokio::test]
async fn typed_invoice_failure_emits_one_safe_event_without_changing_response() {
    let request_id = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let error = ApiError::CreatorSessionUnavailable;
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let router = operational_router(
        Router::new().route("/invoices", post(move || async move { error })),
        runtime(),
    );
    let expected = error.into_response();
    let expected_headers = expected.headers().clone();
    let expected_body = to_bytes(expected.into_body(), 4096).await.unwrap();

    let response = router
        .oneshot(request(request_id))
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get("retry-after"),
        expected_headers.get("retry-after")
    );
    assert_eq!(response.headers()[REQUEST_ID_HEADER], request_id);
    assert_eq!(
        to_bytes(response.into_body(), 4096).await.unwrap(),
        expected_body
    );

    let events = capture.invoice_events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(field(event, "http_status"), Some("503"));
    assert_eq!(
        field(event, "failure_class"),
        Some("creator_session_unavailable")
    );
    assert_eq!(field(event, "request_id"), Some(request_id));
    assert!(field(event, "elapsed_ms").unwrap().parse::<u64>().is_ok());
    assert!(event.iter().all(|(name, _)| {
        matches!(
            name.as_str(),
            "message" | "event" | "http_status" | "failure_class" | "elapsed_ms" | "request_id"
        )
    }));
}

#[tokio::test]
async fn shutdown_503_rejects_unsafe_request_id_without_echo_or_log() {
    let unsafe_value = "secret-identity-and-query-data";
    let runtime = runtime();
    runtime.begin_shutdown();
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let router = operational_router(
        Router::new().route("/invoices", post(|| async { StatusCode::OK })),
        runtime,
    );

    let response = router
        .oneshot(request(unsafe_value))
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let response_id = response.headers()[REQUEST_ID_HEADER]
        .to_str()
        .unwrap()
        .to_owned();
    let parsed = Uuid::parse_str(&response_id).unwrap();
    assert_eq!(parsed.get_version(), Some(Version::Random));
    assert_ne!(response_id, unsafe_value);
    assert!(to_bytes(response.into_body(), 1).await.unwrap().is_empty());

    let events = capture.invoice_events();
    assert_eq!(events.len(), 1);
    assert_eq!(field(&events[0], "failure_class"), Some("shutdown"));
    assert_eq!(field(&events[0], "request_id"), Some(response_id.as_str()));
    assert!(!format!("{events:?}").contains(unsafe_value));
}

#[tokio::test]
async fn every_operational_route_emits_info_start_and_terminal_events_with_template() {
    let request_id = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(capture.clone());
    let mut app = Router::new()
        .route("/setup", get(|| async { StatusCode::OK }))
        .route("/setup/reconnect", get(|| async { StatusCode::OK }))
        .route("/setup/creator-held", get(|| async { StatusCode::OK }))
        .route(
            "/setup/{flow_id}/complete",
            post(|| async { StatusCode::REQUEST_TIMEOUT }),
        );
    let post_routes = [
        "/invoices",
        "/v2/invoices",
        "/v2/invoices/status",
        "/transactions/status",
        "/connections/status",
        "/setup/status",
        "/payment-requests/status",
        "/payment-request-drains",
        "/payment-request-drain-lookups",
        "/payment-request-drain-cleanups",
        "/marketplace/payment-requests/prepare",
        "/marketplace/payment-requests/activate",
        "/marketplace/payment-requests/void",
        "/marketplace/payment-requests/resolve",
        "/marketplace/payment-requests/status",
        "/marketplace/payment-requests/receipt",
    ];
    for route in post_routes {
        app = app.route(route, post(|| async { StatusCode::OK }));
    }
    let router = operational_router(app, runtime());

    let setup_routes = [
        "/setup?return_to=https%3A%2F%2Fexample.test%2Fsecret&state=secret",
        "/setup/reconnect?return_to=https%3A%2F%2Fexample.test%2Fsecret&state=secret",
        "/setup/creator-held?creator=secret",
    ];
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);
    for route in setup_routes {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(route)
                    .header(REQUEST_ID_HEADER, request_id)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()[REQUEST_ID_HEADER], request_id);
    }
    for route in post_routes {
        router
            .clone()
            .oneshot(request_for(route, request_id))
            .await
            .unwrap();
    }
    router
        .clone()
        .oneshot(request_for(
            "/setup/sensitive-flow-token/complete",
            request_id,
        ))
        .await
        .unwrap();

    let events = capture.0.lock().unwrap();
    let starts = events
        .iter()
        .filter(|fields| field(fields, "event") == Some("paykit_http_request_started"))
        .collect::<Vec<_>>();
    let outcomes = events
        .iter()
        .filter(|fields| field(fields, "event") == Some("paykit_http_request_completed"))
        .collect::<Vec<_>>();
    let expected_count = setup_routes.len() + post_routes.len() + 1;
    assert_eq!(starts.len(), expected_count);
    assert_eq!(outcomes.len(), expected_count);
    let templates = starts
        .iter()
        .map(|event| field(event, "route").unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(templates.contains("/setup"));
    assert!(templates.contains("/setup/reconnect"));
    assert!(templates.contains("/setup/creator-held"));
    assert!(templates.contains("/setup/{flow_id}/complete"));
    for route in post_routes {
        assert!(templates.contains(route));
    }
    assert!(events.iter().all(|event| {
        let rendered = format!("{event:?}");
        !rendered.contains("sensitive-flow-token")
            && !rendered.contains("return_to")
            && !rendered.contains("example.test")
            && !rendered.contains("creator=secret")
    }));
}

#[tokio::test]
async fn duplicate_request_id_values_are_replaced_without_cross_request_merging() {
    let supplied = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture);
    let router = operational_router(
        Router::new().route("/invoices", post(|| async { StatusCode::OK })),
        runtime(),
    );
    let duplicate = Request::builder()
        .method("POST")
        .uri("/invoices")
        .header(REQUEST_ID_HEADER, supplied)
        .header(REQUEST_ID_HEADER, supplied)
        .body(Body::empty())
        .unwrap();

    let response = router
        .oneshot(duplicate)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    let actual = response.headers()[REQUEST_ID_HEADER].to_str().unwrap();
    assert_ne!(actual, supplied);
    assert_eq!(
        Uuid::parse_str(actual).unwrap().get_version(),
        Some(Version::Random)
    );
}

#[tokio::test]
async fn concurrent_requests_reusing_valid_client_id_keep_distinct_instances() {
    let supplied = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let capture = EventCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let router = operational_router(
        Router::new().route(
            "/invoices",
            post({
                let barrier = barrier.clone();
                move || {
                    let barrier = barrier.clone();
                    async move {
                        barrier.wait().await;
                        StatusCode::CREATED
                    }
                }
            }),
        ),
        Arc::new(Runtime::new(Arc::new(ReadyDependency), 2)),
    );

    async {
        let (left, right) = tokio::join!(
            router.clone().oneshot(request(supplied)),
            router.oneshot(request(supplied))
        );
        assert_eq!(left.unwrap().status(), StatusCode::CREATED);
        assert_eq!(right.unwrap().status(), StatusCode::CREATED);
    }
    .with_subscriber(subscriber)
    .await;

    let events = capture.0.lock().unwrap();
    let completed = events
        .iter()
        .filter(|event| field(event, "event") == Some("paykit_http_request_completed"))
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert!(
        completed
            .iter()
            .all(|event| field(event, "request_id") == Some(supplied))
    );
    let left_instance = field(completed[0], "request_instance_id").unwrap();
    let right_instance = field(completed[1], "request_instance_id").unwrap();
    assert_ne!(left_instance, right_instance);
    assert!(Uuid::parse_str(left_instance).is_ok());
    assert!(Uuid::parse_str(right_instance).is_ok());
}

#[tokio::test]
async fn production_json_subscriber_serializes_http_correlation_fields() {
    let request_id = "d9428888-122b-4b85-bc8f-2c2e0bf7c874";
    let output = JsonWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::new("off,paykit_server=info"))
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(output.clone())
        .finish();
    let router = operational_router(
        Router::new().route("/invoices", post(|| async { StatusCode::CREATED })),
        runtime(),
    );

    let response = router
        .oneshot(request(request_id))
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let bytes = output.0.lock().unwrap().clone();
    let records = String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let completed = records
        .iter()
        .find(|record| record["fields"]["event"] == "paykit_http_request_completed")
        .unwrap();
    assert_eq!(completed["level"], "INFO");
    assert_eq!(completed["fields"]["request_id"], request_id);
    assert_eq!(completed["fields"]["route"], "/invoices");
    assert_eq!(completed["fields"]["http_status"], 201);
    assert!(Uuid::parse_str(completed["fields"]["request_instance_id"].as_str().unwrap()).is_ok());
}
