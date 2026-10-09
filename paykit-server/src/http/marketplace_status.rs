use std::sync::Arc;

use axum::{
    Router,
    extract::{OriginalUri, State},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    application::marketplace_status::{
        MarketplaceBitcoinStatus, MarketplaceInvoiceStatus, MarketplaceStatusError,
        MarketplaceStatusService,
    },
    domain::locks::parse_creator,
    http::{auth::AuthenticatedJson, error::ApiError},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusBody {
    creator: String,
    invoice_id: String,
}

#[derive(Serialize)]
struct BitcoinResponse {
    txid: String,
    vout: u32,
    observed_sats: u64,
    first_observed_at: String,
    confirmations: u32,
    present: bool,
    amount_matched: bool,
    paid_on_time: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    invoice_id: String,
    state: &'static str,
    proposal_delivery_state: &'static str,
    request_state: Option<&'static str>,
    payment_state: Option<&'static str>,
    activated_at: Option<String>,
    payment_deadline: Option<String>,
    bitcoin: Option<BitcoinResponse>,
    outcome: Option<&'static str>,
    resolved_at: Option<String>,
}

pub fn marketplace_status_router(service: Arc<MarketplaceStatusService>) -> Router {
    Router::new()
        .route("/marketplace/payment-requests/status", post(status))
        .with_state(service)
}

async fn status(
    State(service): State<Arc<MarketplaceStatusService>>,
    OriginalUri(uri): OriginalUri,
    AuthenticatedJson(body): AuthenticatedJson<StatusBody>,
) -> Response {
    if uri.query().is_some() {
        return ApiError::InvalidRequest.into_response();
    }
    let creator = match parse_creator(&body.creator) {
        Ok(creator) => creator,
        Err(_) => return ApiError::InvalidRequest.into_response(),
    };
    let invoice_id = match parse_uuid(&body.invoice_id) {
        Ok(invoice_id) => invoice_id,
        Err(error) => return error.into_response(),
    };
    match service.status(&creator, invoice_id).await {
        Ok(Some(status)) => match response(status) {
            Ok(response) => axum::Json(response).into_response(),
            Err(error) => error.into_response(),
        },
        Ok(None) => ApiError::InvoiceNotFound.into_response(),
        Err(MarketplaceStatusError::Conflict) => ApiError::Conflict.into_response(),
        Err(MarketplaceStatusError::Unavailable) => ApiError::Unavailable.into_response(),
    }
}

fn response(status: MarketplaceInvoiceStatus) -> Result<StatusResponse, ApiError> {
    Ok(StatusResponse {
        invoice_id: status.invoice_id.hyphenated().to_string(),
        state: status.state,
        proposal_delivery_state: status.proposal_delivery_state,
        request_state: status.request_state,
        payment_state: status.payment_state,
        activated_at: format_optional(status.activated_at)?,
        payment_deadline: format_optional(status.payment_deadline)?,
        bitcoin: status.bitcoin.map(bitcoin_response).transpose()?,
        outcome: status.outcome,
        resolved_at: format_optional(status.resolved_at)?,
    })
}

fn bitcoin_response(status: MarketplaceBitcoinStatus) -> Result<BitcoinResponse, ApiError> {
    Ok(BitcoinResponse {
        txid: status.txid,
        vout: status.vout,
        observed_sats: status.observed_sats,
        first_observed_at: format(status.first_observed_at)?,
        confirmations: status.confirmations,
        present: status.present,
        amount_matched: status.amount_matched,
        paid_on_time: status.paid_on_time,
    })
}

fn format_optional(value: Option<OffsetDateTime>) -> Result<Option<String>, ApiError> {
    value.map(format).transpose()
}

fn format(value: OffsetDateTime) -> Result<String, ApiError> {
    value.format(&Rfc3339).map_err(|_| ApiError::InternalError)
}

fn parse_uuid(value: &str) -> Result<Uuid, ApiError> {
    let parsed = Uuid::parse_str(value).map_err(|_| ApiError::InvalidRequest)?;
    if parsed.get_version_num() != 4
        || parsed.get_variant() != uuid::Variant::RFC4122
        || parsed.hyphenated().to_string() != value
    {
        return Err(ApiError::InvalidRequest);
    }
    Ok(parsed)
}
