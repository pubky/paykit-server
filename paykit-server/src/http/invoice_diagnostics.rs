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
pub(crate) struct InvoiceFailureClass(&'static str);

impl InvoiceFailureClass {
    pub(crate) const OVERLOAD: Self = Self("overload");
    pub(crate) const SHUTDOWN: Self = Self("shutdown");

    pub(crate) const fn api(error: ApiError) -> Self {
        Self(error.code())
    }

    const fn as_str(self) -> &'static str {
        self.0
    }
}

pub(crate) fn annotate(response: &mut Response, class: InvoiceFailureClass) {
    response.extensions_mut().insert(class);
}

pub(crate) async fn middleware(request: Request<Body>, next: Next) -> Response {
    if request.method() != Method::POST || request.uri().path() != "/invoices" {
        return next.run(request).await;
    }

    let started = Instant::now();
    let request_id = accepted_request_id(request.headers()).unwrap_or_else(Uuid::new_v4);
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id.hyphenated().to_string())
            .expect("UUID is a valid header value"),
    );

    let status = response.status();
    let failure_class = response
        .extensions()
        .get::<InvoiceFailureClass>()
        .copied()
        .map(InvoiceFailureClass::as_str)
        .unwrap_or(if status.is_success() {
            "none"
        } else {
            "unclassified"
        });
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    emit(status, failure_class, elapsed_ms, request_id);
    response
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

fn emit(status: StatusCode, failure_class: &'static str, elapsed_ms: u64, request_id: Uuid) {
    let http_status = status.as_u16();
    let request_id = request_id.hyphenated();
    if status.is_server_error() {
        tracing::warn!(
            event = "paykit_invoice_outcome",
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit invoice request completed"
        );
    } else if status.is_success() {
        tracing::debug!(
            event = "paykit_invoice_outcome",
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit invoice request completed"
        );
    } else {
        tracing::info!(
            event = "paykit_invoice_outcome",
            http_status,
            failure_class,
            elapsed_ms,
            request_id = %request_id,
            "Paykit invoice request completed"
        );
    }
}
