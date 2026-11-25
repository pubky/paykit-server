use std::time::Instant;

use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, Method, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use uuid::{Uuid, Version};

use super::error::ApiError;

pub(crate) const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone, Copy, Debug)]
pub(crate) struct OutcomeFailureClass(&'static str);

impl OutcomeFailureClass {
    pub(crate) const OVERLOAD: Self = Self("overload");
    pub(crate) const SHUTDOWN: Self = Self("shutdown");

    pub(crate) const fn api(error: ApiError) -> Self {
        Self(error.code())
    }

    const fn as_str(self) -> &'static str {
        self.0
    }
}

pub(crate) fn annotate(response: &mut Response, class: OutcomeFailureClass) {
    response.extensions_mut().insert(class);
}

#[derive(Clone, Copy)]
struct DiagnosedRoute {
    method: &'static str,
    route: &'static str,
    event: &'static str,
    operation: Option<&'static str>,
    source_operation: &'static str,
    frequent_poll: bool,
}

struct RequestOutcomeGuard {
    route: DiagnosedRoute,
    started: Instant,
    request_id: Uuid,
    request_instance_id: Uuid,
    armed: bool,
}

impl RequestOutcomeGuard {
    fn new(
        route: DiagnosedRoute,
        started: Instant,
        request_id: Uuid,
        request_instance_id: Uuid,
    ) -> Self {
        Self {
            route,
            started,
            request_id,
            request_instance_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RequestOutcomeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let route = self.route;
        let elapsed_ms = elapsed_ms(self.started);
        let request_id = self.request_id;
        let request_instance_id = self.request_instance_id;
        let panicking = std::thread::panicking();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if !panicking && route.event != "paykit_http_request_completed" {
                emit_incomplete(route, elapsed_ms, request_id, false);
            }
            emit_http_incomplete(
                route,
                elapsed_ms,
                request_id,
                request_instance_id,
                panicking,
            );
        }));
    }
}

fn diagnosed_route(request: &Request<Body>) -> Option<DiagnosedRoute> {
    let (method, path) = (request.method(), request.uri().path());
    let (method, route, operation) = match (method, path) {
        (&Method::GET, "/setup") => ("GET", "/setup", "setup_start"),
        (&Method::GET, "/setup/reconnect") => ("GET", "/setup/reconnect", "setup_reconnect"),
        (&Method::GET, "/setup/creator-held") => {
            ("GET", "/setup/creator-held", "setup_creator_held")
        }
        (&Method::POST, path)
            if path
                .strip_prefix("/setup/")
                .and_then(|rest| rest.strip_suffix("/complete"))
                .is_some_and(|flow_id| !flow_id.is_empty() && !flow_id.contains('/')) =>
        {
            ("POST", "/setup/{flow_id}/complete", "setup_complete")
        }
        (&Method::POST, "/invoices") => ("POST", "/invoices", "invoice_create"),
        (&Method::POST, "/v2/invoices") => ("POST", "/v2/invoices", "provider_v2_invoice_create"),
        (&Method::POST, "/v2/invoices/status") => {
            ("POST", "/v2/invoices/status", "provider_v2_invoice_status")
        }
        (&Method::POST, "/transactions/status") => {
            ("POST", "/transactions/status", "transaction_status")
        }
        (&Method::POST, "/connections/status") => {
            ("POST", "/connections/status", "connection_status")
        }
        (&Method::POST, "/setup/status") => ("POST", "/setup/status", "setup_status"),
        (&Method::POST, "/payment-requests/status") => {
            ("POST", "/payment-requests/status", "payment_request_status")
        }
        (&Method::POST, "/payment-request-drains") => (
            "POST",
            "/payment-request-drains",
            "payment_request_drain_create",
        ),
        (&Method::POST, "/payment-request-drain-lookups") => (
            "POST",
            "/payment-request-drain-lookups",
            "payment_request_drain_lookup",
        ),
        (&Method::POST, "/payment-request-drain-cleanups") => (
            "POST",
            "/payment-request-drain-cleanups",
            "payment_request_drain_cleanup",
        ),
        (&Method::POST, "/marketplace/payment-requests/prepare") => (
            "POST",
            "/marketplace/payment-requests/prepare",
            "marketplace_prepare",
        ),
        (&Method::POST, "/marketplace/payment-requests/activate") => (
            "POST",
            "/marketplace/payment-requests/activate",
            "marketplace_activate",
        ),
        (&Method::POST, "/marketplace/payment-requests/void") => (
            "POST",
            "/marketplace/payment-requests/void",
            "marketplace_void",
        ),
        (&Method::POST, "/marketplace/payment-requests/resolve") => (
            "POST",
            "/marketplace/payment-requests/resolve",
            "marketplace_resolve",
        ),
        (&Method::POST, "/marketplace/payment-requests/status") => (
            "POST",
            "/marketplace/payment-requests/status",
            "marketplace_status",
        ),
        (&Method::POST, "/marketplace/payment-requests/receipt") => (
            "POST",
            "/marketplace/payment-requests/receipt",
            "marketplace_receipt",
        ),
        _ => return None,
    };
    let (event, legacy_operation, frequent_poll) = match path {
        "/invoices" => ("paykit_invoice_outcome", None, false),
        "/payment-requests/status" => (
            "paykit_locks_status_outcome",
            Some("payment_request_status"),
            true,
        ),
        "/connections/status" => (
            "paykit_locks_status_outcome",
            Some("connection_status"),
            true,
        ),
        "/setup/status" => ("paykit_locks_status_outcome", Some("setup_status"), true),
        "/marketplace/payment-requests/prepare" => {
            ("paykit_marketplace_prepare_outcome", None, false)
        }
        _ => ("paykit_http_request_completed", Some(operation), false),
    };
    Some(DiagnosedRoute {
        method,
        route,
        event,
        operation: legacy_operation,
        source_operation: operation,
        frequent_poll,
    })
}

pub(crate) async fn middleware(request: Request<Body>, next: Next) -> Response {
    let Some(route) = diagnosed_route(&request) else {
        return next.run(request).await;
    };

    let started = Instant::now();
    let request_id = accepted_request_id(request.headers()).unwrap_or_else(Uuid::new_v4);
    let request_instance_id = Uuid::new_v4();
    emit_http_started(route, request_id, request_instance_id);
    let mut outcome_guard =
        RequestOutcomeGuard::new(route, started, request_id, request_instance_id);
    let (mut response, source_failure) = crate::diagnostics::scope(
        request_id,
        request_instance_id,
        route.source_operation,
        next.run(request),
    )
    .await;
    outcome_guard.disarm();
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id.hyphenated().to_string())
            .expect("UUID is a valid header value"),
    );

    let status = response.status();
    let failure_class = response
        .extensions()
        .get::<OutcomeFailureClass>()
        .copied()
        .map(OutcomeFailureClass::as_str)
        .unwrap_or(if status.is_success() {
            "none"
        } else {
            "unclassified"
        });
    let elapsed_ms = elapsed_ms(started);
    emit_http_completed(
        route,
        status,
        failure_class,
        elapsed_ms,
        request_id,
        request_instance_id,
    );
    if route.event != "paykit_http_request_completed" {
        emit(
            route,
            status,
            failure_class,
            elapsed_ms,
            request_id,
            source_failure,
        );
    }
    response
}

fn emit_http_started(route: DiagnosedRoute, request_id: Uuid, request_instance_id: Uuid) {
    tracing::info!(
        event = "paykit_http_request_started",
        operation = route.source_operation,
        method = route.method,
        route = route.route,
        request_id = %request_id.hyphenated(),
        request_instance_id = %request_instance_id.hyphenated(),
        "Paykit HTTP request started"
    );
}

fn emit_http_completed(
    route: DiagnosedRoute,
    status: StatusCode,
    failure_class: &'static str,
    elapsed_ms: u64,
    request_id: Uuid,
    request_instance_id: Uuid,
) {
    tracing::info!(
        event = "paykit_http_request_completed",
        operation = route.source_operation,
        method = route.method,
        route = route.route,
        http_status = status.as_u16(),
        failure_class,
        elapsed_ms,
        request_id = %request_id.hyphenated(),
        request_instance_id = %request_instance_id.hyphenated(),
        "Paykit HTTP request completed"
    );
}

fn emit_http_incomplete(
    route: DiagnosedRoute,
    elapsed_ms: u64,
    request_id: Uuid,
    request_instance_id: Uuid,
    panicking: bool,
) {
    tracing::warn!(
        event = "paykit_http_request_completed",
        operation = route.source_operation,
        method = route.method,
        route = route.route,
        outcome = if panicking { "panic" } else { "cancelled" },
        failure_class = if panicking { "panic" } else { "cancelled" },
        elapsed_ms,
        request_id = %request_id.hyphenated(),
        request_instance_id = %request_instance_id.hyphenated(),
        "Paykit HTTP request ended without a response"
    );
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn accepted_request_id(headers: &axum::http::HeaderMap) -> Option<Uuid> {
    let values = headers.get_all(&REQUEST_ID_HEADER);
    if values.iter().count() != 1 {
        return None;
    }
    let value = values.iter().next()?.to_str().ok()?;
    let parsed = Uuid::parse_str(value).ok()?;
    (parsed.get_version() == Some(Version::Random) && parsed.hyphenated().to_string() == value)
        .then_some(parsed)
}

fn emit(
    route: DiagnosedRoute,
    status: StatusCode,
    failure_class: &'static str,
    elapsed_ms: u64,
    request_id: Uuid,
    source_failure: bool,
) {
    let http_status = status.as_u16();
    let request_id = request_id.hyphenated();
    if route.frequent_poll && (status.is_success() || failure_class != "unclassified") {
        tracing::debug!(
            event = route.event,
            operation = route.operation.expect("poll routes have an operation"),
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit Locks status request completed"
        );
    } else if route.frequent_poll {
        tracing::warn!(
            event = route.event,
            operation = route.operation.expect("poll routes have an operation"),
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit Locks status request completed"
        );
    } else if status.is_server_error() && !source_failure {
        tracing::warn!(
            event = route.event,
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit invoice request completed"
        );
    } else if status.is_success() || source_failure {
        tracing::debug!(
            event = route.event,
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit invoice request completed"
        );
    } else {
        tracing::info!(
            event = route.event,
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit invoice request completed"
        );
    }
}

fn emit_incomplete(route: DiagnosedRoute, elapsed_ms: u64, request_id: Uuid, panicking: bool) {
    let failure_class = "cancelled";
    let request_id = request_id.hyphenated();
    if panicking {
        if let Some(operation) = route.operation {
            tracing::warn!(
                event = route.event,
                operation,
                failure_class,
                elapsed_ms,
                request_id = %request_id,
                "Paykit request ended without a response"
            );
        } else {
            tracing::warn!(
                event = route.event,
                failure_class,
                elapsed_ms,
                request_id = %request_id,
                "Paykit request ended without a response"
            );
        }
    } else if route.frequent_poll {
        tracing::debug!(
            event = route.event,
            operation = route.operation.expect("poll routes have an operation"),
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit request ended without a response"
        );
    } else {
        tracing::info!(
            event = route.event,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit request ended without a response"
        );
    }
}
