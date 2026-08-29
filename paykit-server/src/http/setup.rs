use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, Path, RawQuery, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use qrcode::{QrCode, render::svg};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;

use crate::setup::{BeginError, CompanionAuthRequestResult, PollResult, SetupService, StartedFlow};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompanionAuthRequestBody {
    version: u8,
    handle: String,
}

#[derive(Serialize)]
struct CompanionAuthRequestResponse {
    version: u8,
    auth_url: String,
}

pub fn setup_router(service: SetupService) -> Router {
    Router::new()
        .route("/setup", get(begin))
        .route(
            "/setup/companion-auth-request",
            post(companion_auth_request),
        )
        .route("/setup/{flow_id}/complete", post(complete))
        .with_state(service)
}

async fn begin(
    State(service): State<SetupService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RawQuery(query): RawQuery,
) -> Response<Body> {
    let Some((return_to, state)) = parse_setup_query(query.as_deref()) else {
        return invalid_request();
    };
    match service.begin(peer.ip(), &return_to, &state).await {
        Ok(flow) => iframe_response(flow),
        Err(BeginError::InvalidRequest) => invalid_request(),
        Err(BeginError::RateLimited) => safe_response_with_retry(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"rate_limited"}),
            "60",
        ),
        Err(BeginError::Unavailable) => safe_response_with_retry(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"unavailable"}),
            "1",
        ),
    }
}

async fn companion_auth_request(
    State(service): State<SetupService>,
    request: Result<Json<CompanionAuthRequestBody>, JsonRejection>,
) -> Response<Body> {
    let Ok(Json(request)) = request else {
        return invalid_request();
    };
    if request.version != 1 {
        return invalid_request();
    }
    match service.companion_auth_request(&request.handle).await {
        CompanionAuthRequestResult::Ready { authorization_url } => safe_response(
            StatusCode::OK,
            serde_json::to_value(CompanionAuthRequestResponse {
                version: 1,
                auth_url: authorization_url,
            })
            .expect("companion auth response serializes"),
        ),
        CompanionAuthRequestResult::Unavailable => {
            safe_response(StatusCode::NOT_FOUND, json!({"error":"not_found"}))
        }
    }
}

async fn complete(
    State(service): State<SetupService>,
    Path(flow_id): Path<String>,
) -> Response<Body> {
    response_for_poll(service.complete_and_poll(&flow_id).await)
}

fn parse_setup_query(query: Option<&str>) -> Option<(String, String)> {
    let mut return_to = None;
    let mut state = None;
    for (key, value) in url::form_urlencoded::parse(query?.as_bytes()) {
        match key.as_ref() {
            "return_to" if return_to.is_none() => return_to = Some(value.into_owned()),
            "state" if state.is_none() => state = Some(value.into_owned()),
            _ => return None,
        }
    }
    Some((return_to?, state?))
}

fn iframe_response(flow: StartedFlow) -> Response<Body> {
    let flow_id = json_for_script(&flow.flow_id);
    let state = json_for_script(&flow.state);
    let origin = json_for_script(&flow.origin);
    let authorization_url = html_for_text(&flow.authorization_url);
    let companion_handle = html_for_text(&flow.companion_handle);
    let qr_svg = render_authorization_qr_svg(&flow.authorization_url);
    let companion_command =
        "npm --prefix examples/js-sdk run authenticate-paykit -- --role content-creator";
    let css = SETUP_CSS;
    let shell = format!(
        "<!doctype html><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><style>{css}</style><main><span class=\"qr\" data-testid=\"paykit-auth-qr\">{qr_svg}</span><a class=\"bitkit-btn\" href=\"{authorization_url}\">Continue with Bitkit</a><section class=\"companion\"><span>Local demo companion handle</span><code data-testid=\"paykit-companion-handle\">{companion_handle}</code><span>Run from the Locks repository host</span><code data-testid=\"paykit-companion-command\">{companion_command}</code></section></main><script>\nconst flowId={flow_id};const state={state};const targetOrigin={origin};\nconst retryable=new Set([408,425,429,502,503,504]);let delay=500;\nasync function poll(){{try{{const response=await fetch('/setup/'+flowId+'/complete',{{method:'POST'}});if(response.status===200){{window.parent.postMessage({{type:'paykit-setup-callback',state}},targetOrigin);return;}}if(!retryable.has(response.status)){{window.parent.postMessage({{type:'paykit-setup-callback',state,error:'setup-failed'}},targetOrigin);return;}}}}catch(_error){{}}setTimeout(poll,delay);delay=Math.min(delay*2,5000);}}setTimeout(poll,delay);\n</script>"
    );
    let mut response = Response::new(Body::from(shell));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_str(&format!("frame-ancestors {}", flow.origin))
            .expect("validated origin is a header value"),
    );
    response
}

/// Setup shell styles. The embedding app owns the modal chrome, so this page paints only the code
/// the creator acts on.
const SETUP_CSS: &str = r#"html,body{margin:0;height:100%}
body{display:flex;align-items:center;justify-content:center;background:transparent;font:700 14px/20px system-ui,-apple-system,sans-serif;color:#d4d4db}
/* Without this the flex item shrinks to its content, so the touch button's width:100% only
   reaches the QR panel's width instead of the frame's. */
main{width:100%;display:flex;flex-direction:column;gap:12px;align-items:center;justify-content:center}
.qr{display:flex;align-items:center;justify-content:center;box-sizing:border-box;width:192px;height:192px;padding:12px;border-radius:8px;background:#fff}
.qr svg{display:block;width:100%;height:100%}
.bitkit-btn{display:none}
.companion{display:flex;max-width:100%;flex-direction:column;gap:4px;text-align:center;font:500 12px/16px system-ui,-apple-system,sans-serif}.companion code{max-width:100%;overflow-wrap:anywhere;user-select:all}
/* Touch devices cannot scan their own screen, so they get the same URL as a deep link. Keyed on the
   pointer type, not viewport width: this page renders inside a small parent iframe, which would
   always read as narrow. */
@media (hover:none) and (pointer:coarse){.qr{display:none}.bitkit-btn{display:flex;align-items:center;justify-content:center;box-sizing:border-box;width:100%;height:60px;padding:20px 32px;border-radius:9999px;background:#303034;color:#d4d4db;text-decoration:none}}
"#;

/// The QR a creator scans with Bitkit. High error correction so a centered brand badge added by the
/// embedder stays scannable.
fn render_authorization_qr_svg(authorization_url: &str) -> String {
    QrCode::with_error_correction_level(authorization_url.as_bytes(), qrcode::EcLevel::H)
        .expect("Pubky authorization URL fits QR capacity")
        .render::<svg::Color>()
        .min_dimensions(192, 192)
        // No built-in quiet zone: the white panel's padding is the margin.
        .quiet_zone(false)
        .dark_color(svg::Color("#111111"))
        .light_color(svg::Color("transparent"))
        .build()
        // Strip the XML prolog — this SVG is inlined into HTML, not served as a document.
        .replace("<?xml version=\"1.0\" standalone=\"yes\"?>", "")
        .replace(
            "<svg",
            "<svg aria-label=\"Bitkit authorization QR code\" role=\"img\"",
        )
}

fn html_for_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn json_for_script(value: &str) -> String {
    serde_json::to_string(value)
        .expect("strings serialize")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn response_for_poll(result: PollResult) -> Response<Body> {
    match result {
        PollResult::Complete => safe_response(StatusCode::OK, json!({"status":"complete"})),
        PollResult::PendingTimeout => {
            safe_response(StatusCode::REQUEST_TIMEOUT, json!({"status":"pending"}))
        }
        PollResult::Unknown => safe_response(StatusCode::NOT_FOUND, json!({"error":"not_found"})),
        PollResult::Expired => safe_response(StatusCode::GONE, json!({"error":"expired"})),
        PollResult::Failed => safe_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error":"setup_failed"}),
        ),
        PollResult::Overloaded => safe_response_with_retry(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"overloaded"}),
            "60",
        ),
        PollResult::Unavailable => safe_response_with_retry(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"unavailable"}),
            "1",
        ),
    }
}

fn invalid_request() -> Response<Body> {
    safe_response(StatusCode::BAD_REQUEST, json!({"error":"invalid_request"}))
}

fn safe_response(status: StatusCode, payload: serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Body::from(payload.to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn safe_response_with_retry(
    status: StatusCode,
    payload: serde_json::Value,
    retry_after: &'static str,
) -> Response<Body> {
    let mut response = safe_response(status, payload);
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static(retry_after));
    response
}
