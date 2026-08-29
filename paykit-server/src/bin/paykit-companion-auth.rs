use std::{
    io::{self, Write},
    process::ExitCode,
    str::FromStr,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bitcoin::bip32::Xpub;
use paykit_sdk::{
    PubkyAuthCompanionClaim, PubkyLocalSecretKey, PubkySessionBootstrap, parse_pubky_auth_url,
};
use paykit_server::{
    bitkit_claim::{CLAIM_TYPE, LOCAL_DEMO_CAPABILITIES, QUERY_PARAMETER, encode_unsigned_payload},
    config::{BitcoinNetwork, PAYKIT_CLIENT_ID},
    real_setup::validate_xpub,
};
use serde::{Deserialize, Serialize};

const SERVER_URL_ENV: &str = "PAYKIT_SERVER_URL";
const MAX_RESPONSE_BYTES: usize = 16 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    version: u8,
    companion_handle: String,
    creator_secret: String,
    account_xpub: String,
    account_index: u32,
}

#[derive(Serialize)]
struct CompanionAuthRequest<'a> {
    version: u8,
    handle: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompanionAuthResponse {
    version: u8,
    auth_url: String,
}

enum Failure {
    InvalidInput,
    Authentication,
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = run().await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::InvalidInput) => {
            write_failure(b"invalid input\n");
            ExitCode::FAILURE
        }
        Err(Failure::Authentication) => {
            write_failure(b"companion authentication failed\n");
            ExitCode::FAILURE
        }
    }
}

fn write_failure(message: &[u8]) {
    let mut stderr = io::stderr().lock();
    if stderr.write_all(message).is_ok() {
        let _ = stderr.flush();
    }
}

async fn run() -> Result<(), Failure> {
    if std::env::args_os().len() != 1 {
        return Err(Failure::InvalidInput);
    }
    let input: Input =
        serde_json::from_reader(io::stdin().lock()).map_err(|_| Failure::InvalidInput)?;
    if input.version != 1 {
        return Err(Failure::InvalidInput);
    }
    validate_companion_handle(&input.companion_handle)?;
    let paykit_server_url = trusted_paykit_server_url()?;
    let creator_secret: [u8; 32] = URL_SAFE_NO_PAD
        .decode(&input.creator_secret)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(Failure::InvalidInput)?;
    if input.account_index >= (1 << 31) {
        return Err(Failure::InvalidInput);
    }
    let xpub = Xpub::from_str(&input.account_xpub).map_err(|_| Failure::InvalidInput)?;
    if !input.account_xpub.starts_with("tpub") || input.account_xpub != xpub.to_string() {
        return Err(Failure::InvalidInput);
    }
    validate_xpub(
        &xpub.encode(),
        input.account_index,
        &BitcoinNetwork::Regtest,
    )
    .map_err(|_| Failure::InvalidInput)?;
    let payload = encode_unsigned_payload(input.account_index, &xpub.encode());
    let claim = PubkyAuthCompanionClaim::new(QUERY_PARAMETER, CLAIM_TYPE, payload.to_vec())
        .map_err(|_| Failure::Authentication)?;
    let auth_url = resolve_companion_auth_url(&paykit_server_url, &input.companion_handle).await?;
    let auth = parse_pubky_auth_url(&auth_url).map_err(|_| Failure::Authentication)?;
    if auth.client_id != PAYKIT_CLIENT_ID {
        return Err(Failure::Authentication);
    }
    let bootstrap =
        PubkySessionBootstrap::new(PAYKIT_CLIENT_ID).map_err(|_| Failure::Authentication)?;
    bootstrap
        .approve_auth_with_companion_claim(
            &auth_url,
            LOCAL_DEMO_CAPABILITIES,
            &PubkyLocalSecretKey::new(creator_secret),
            &claim,
        )
        .await
        .map_err(|_| Failure::Authentication)?;
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(b"{\"version\":1,\"status\":\"approved\"}\n")
        .and_then(|()| stdout.flush())
        .map_err(|_| Failure::Authentication)?;
    Ok(())
}

fn validate_companion_handle(value: &str) -> Result<(), Failure> {
    let decoded: [u8; 32] = URL_SAFE_NO_PAD
        .decode(value)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(Failure::InvalidInput)?;
    if URL_SAFE_NO_PAD.encode(decoded) != value {
        return Err(Failure::InvalidInput);
    }
    Ok(())
}

fn trusted_paykit_server_url() -> Result<reqwest::Url, Failure> {
    let value = std::env::var(SERVER_URL_ENV).map_err(|_| Failure::InvalidInput)?;
    let url = reqwest::Url::parse(&value).map_err(|_| Failure::InvalidInput)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Failure::InvalidInput);
    }
    Ok(url)
}

async fn resolve_companion_auth_url(
    server_url: &reqwest::Url,
    handle: &str,
) -> Result<String, Failure> {
    let endpoint = server_url
        .join("setup/companion-auth-request")
        .map_err(|_| Failure::InvalidInput)?;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| Failure::Authentication)?;
    let mut response = client
        .post(endpoint)
        .json(&CompanionAuthRequest { version: 1, handle })
        .send()
        .await
        .map_err(|_| Failure::Authentication)?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    let no_store = response
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|directive| directive.trim() == "no-store")
        });
    if !response.status().is_success()
        || content_type != Some("application/json")
        || !no_store
        || response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(Failure::Authentication);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Failure::Authentication)?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(Failure::Authentication);
        }
        body.extend_from_slice(&chunk);
    }
    let resolved: CompanionAuthResponse =
        serde_json::from_slice(&body).map_err(|_| Failure::Authentication)?;
    if resolved.version != 1 || resolved.auth_url.is_empty() {
        return Err(Failure::Authentication);
    }
    Ok(resolved.auth_url)
}
