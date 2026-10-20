//! Concrete SDK AUTH start boundary for Bitkit setup.

use url::Url;

use crate::bitkit_claim::{
    AuthRequest, ClaimError, PAYKIT_ACCESS_CLAIM, QUERY_PARAMETER, USDT_ADDRESS_CLAIM,
    parse_auth_request, parse_reconnect_auth_request, required_capabilities, setup_claim_selection,
};

/// A secret-bearing request is retained only in the in-memory flow. Callers may
/// render `authorization_url` inside the server-origin iframe but must never put
/// it in postMessage or completion responses.
pub struct StartedBitkitAuth {
    pub authorization_url: String,
    pub request: AuthRequest,
    pub auth_request: paykit_sdk::PubkyAuthRequest,
    capabilities: String,
}

impl StartedBitkitAuth {
    pub fn capabilities(&self) -> &str {
        &self.capabilities
    }
}

impl core::fmt::Debug for StartedBitkitAuth {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StartedBitkitAuth")
            .field("authorization_url", &"<redacted>")
            .finish()
    }
}

/// Starts the normal SDK-owned sign-in request and safely binds the exact
/// Bitkit query pair. This is the production construction path; mocks remain
/// confined to the setup-flow tests.
#[derive(Clone, Debug)]
pub struct BitkitAuthStarter {
    bootstrap: paykit_sdk::PubkySessionBootstrap,
    capabilities: String,
    setup_selection: String,
    usdt: bool,
}

impl BitkitAuthStarter {
    pub fn new(bootstrap: paykit_sdk::PubkySessionBootstrap) -> Self {
        Self {
            bootstrap,
            capabilities: required_capabilities(),
            setup_selection: setup_claim_selection(),
            usdt: false,
        }
    }

    pub fn with_usdt(mut self) -> Self {
        self.setup_selection = format!("{}.{}", setup_claim_selection(), USDT_ADDRESS_CLAIM);
        self.usdt = true;
        self
    }

    pub fn usdt_enabled(&self) -> bool {
        self.usdt
    }

    pub async fn start(&self) -> Result<StartedBitkitAuth, ClaimError> {
        self.start_request(&self.setup_selection, false).await
    }

    /// Refreshes delegated authority and authenticates optional replacement receiving details.
    pub async fn start_reconnect(&self) -> Result<StartedBitkitAuth, ClaimError> {
        self.start_request(&self.setup_selection, true).await
    }

    /// Refreshes delegated authority and asks for the USDT address alone, so Bitkit
    /// prepares no Bitcoin account and the bound one stays.
    pub async fn start_usdt_reconnect(&self) -> Result<StartedBitkitAuth, ClaimError> {
        if !self.usdt {
            return Err(ClaimError::InvalidAuthRequest);
        }
        self.start_request(&usdt_reconnect_selection(), true).await
    }

    async fn start_request(
        &self,
        selection: &str,
        reconnect: bool,
    ) -> Result<StartedBitkitAuth, ClaimError> {
        let request = self
            .bootstrap
            .start_sign_in_auth(&self.capabilities)
            .await
            .map_err(|_| ClaimError::InvalidAuthRequest)?;
        let mut url =
            Url::parse(request.authorization_url()).map_err(|_| ClaimError::InvalidAuthRequest)?;
        if url.query_pairs().any(|(key, _)| key == QUERY_PARAMETER) {
            return Err(ClaimError::InvalidAuthRequest);
        }
        url.query_pairs_mut()
            .append_pair(QUERY_PARAMETER, selection);
        let authorization_url = url.to_string();
        let companion = if reconnect {
            parse_reconnect_auth_request(&authorization_url, &self.capabilities)?
        } else {
            parse_auth_request(&authorization_url, &self.capabilities)?
        };
        Ok(StartedBitkitAuth {
            authorization_url,
            request: companion,
            auth_request: request,
            capabilities: self.capabilities.clone(),
        })
    }
}

fn usdt_reconnect_selection() -> String {
    [PAYKIT_ACCESS_CLAIM, USDT_ADDRESS_CLAIM].join(".")
}

pub fn append_bitkit_claim(
    auth_url: &str,
    expected_capabilities: &str,
) -> Result<String, ClaimError> {
    let mut url = Url::parse(auth_url).map_err(|_| ClaimError::InvalidAuthRequest)?;
    if url.query_pairs().any(|(key, _)| key == QUERY_PARAMETER) {
        return Err(ClaimError::InvalidAuthRequest);
    }
    url.query_pairs_mut()
        .append_pair(QUERY_PARAMETER, &setup_claim_selection());
    let value = url.to_string();
    parse_auth_request(&value, expected_capabilities)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitkit_claim::LOCAL_DEMO_CAPABILITIES;
    use crate::config::PAYKIT_CLIENT_ID;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

    #[tokio::test]
    async fn appends_one_exact_companion_query_pair_to_grant_url() {
        let request = paykit_sdk::PubkySessionBootstrap::new(PAYKIT_CLIENT_ID)
            .unwrap()
            .start_sign_in_auth(LOCAL_DEMO_CAPABILITIES)
            .await
            .unwrap();
        assert!(
            request
                .authorization_url()
                .starts_with("pubkyauth://signin_grant?")
        );
        let url = request.authorization_url();
        let expected_secret: [u8; 32] = URL_SAFE_NO_PAD
            .decode(
                Url::parse(url)
                    .unwrap()
                    .query_pairs()
                    .find_map(|(key, value)| (key == "secret").then(|| value.into_owned()))
                    .unwrap(),
            )
            .unwrap()
            .try_into()
            .unwrap();
        let augmented = append_bitkit_claim(url, LOCAL_DEMO_CAPABILITIES).unwrap();
        assert!(augmented.contains("x-bitkit-claim=paykit-access-v1.watch-only-account-v1"));
        assert_eq!(
            parse_auth_request(&augmented, LOCAL_DEMO_CAPABILITIES)
                .unwrap()
                .secret(),
            &expected_secret
        );
        assert_eq!(
            append_bitkit_claim(&augmented, LOCAL_DEMO_CAPABILITIES),
            Err(ClaimError::InvalidAuthRequest)
        );
    }

    #[tokio::test]
    async fn reconnect_requests_authenticated_watch_only_account_material() {
        let started = BitkitAuthStarter::new(
            paykit_sdk::PubkySessionBootstrap::new(PAYKIT_CLIENT_ID).unwrap(),
        )
        .start_reconnect()
        .await
        .unwrap();
        assert_eq!(
            started.request.claim_type(),
            "paykit-access-v1.watch-only-account-v1"
        );
        assert!(started.request.reconnect());
    }

    #[tokio::test]
    async fn usdt_reconnect_requests_no_watch_only_account() {
        let starter = BitkitAuthStarter::new(
            paykit_sdk::PubkySessionBootstrap::new(PAYKIT_CLIENT_ID).unwrap(),
        );
        assert_eq!(
            starter.start_usdt_reconnect().await.unwrap_err(),
            ClaimError::InvalidAuthRequest
        );
        let started = starter.with_usdt().start_usdt_reconnect().await.unwrap();
        assert_eq!(
            started.request.claim_type(),
            "paykit-access-v1.usdt-address-v1"
        );
        assert!(started.request.reconnect());
        assert!(!started.authorization_url.contains("watch-only-account-v1"));
    }
}
