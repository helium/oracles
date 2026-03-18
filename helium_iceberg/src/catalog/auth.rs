//! OAuth2 authentication for the Iceberg REST catalog.

use crate::{option_hashmap, Error, Result, Settings};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Default token lifetime in seconds if not provided by the OAuth2 server.
const DEFAULT_TOKEN_LIFETIME_SECS: u64 = 3600; // 1 hour

/// Safety margin before token expiration to trigger proactive refresh.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// OAuth2 token response from the token endpoint.
#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Token lifetime in seconds (standard OAuth2 response field).
    expires_in: Option<u64>,
}

/// A cached OAuth2 token with expiration tracking.
pub(super) struct CachedToken {
    pub(super) token: String,
    pub(super) expires_at: Instant,
}

/// Authentication strategy for direct REST API calls.
#[derive(Clone)]
pub(crate) enum EndpointAuth {
    None,
    Token(String),
    OAuth2 {
        token_endpoint: String,
        credential: String,
        extra_params: HashMap<String, String>,
        cached_token: Arc<Mutex<Option<CachedToken>>>,
    },
}

impl EndpointAuth {
    /// Build from settings, determining the auth strategy.
    pub(crate) fn from_settings(settings: &Settings) -> Self {
        if let Some(ref credential) = settings.auth.credential {
            let token_endpoint = settings
                .auth
                .oauth2_server_uri
                .clone()
                .unwrap_or_else(|| format!("{}/v1/oauth/tokens", settings.catalog_uri));

            let extra_params = option_hashmap! {
                "scope" => settings.auth.scope,
                "audience" => settings.auth.audience,
                "resource" => settings.auth.resource,
            };

            Self::OAuth2 {
                token_endpoint,
                credential: credential.clone(),
                extra_params,
                cached_token: Arc::new(Mutex::new(None)),
            }
        } else if let Some(ref token) = settings.auth.token {
            Self::Token(token.clone())
        } else {
            Self::None
        }
    }

    /// Fetch a fresh OAuth2 token from the token endpoint.
    ///
    /// Returns the token and its lifetime in seconds.
    async fn fetch_token(
        client: &reqwest::Client,
        token_endpoint: &str,
        credential: &str,
        extra_params: &HashMap<String, String>,
    ) -> Result<(String, u64)> {
        let (client_id, client_secret) = credential.split_once(':').unwrap_or((credential, ""));

        let mut params = vec![
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ];
        let extra: Vec<(&str, &str)> = extra_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        params.extend(extra);

        let response = client
            .post(token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| Error::Catalog(format!("OAuth2 token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Catalog(format!(
                "OAuth2 token request returned {status}: {body}"
            )));
        }

        let token_response: TokenResponse = response
            .json()
            .await
            .map_err(|e| Error::Catalog(format!("failed to parse OAuth2 token response: {e}")))?;

        let expires_in = token_response.expires_in.unwrap_or_else(|| {
            tracing::warn!(
                default_secs = DEFAULT_TOKEN_LIFETIME_SECS,
                "no expires_in in token response, using default"
            );
            DEFAULT_TOKEN_LIFETIME_SECS
        });

        Ok((token_response.access_token, expires_in))
    }

    /// Get a valid token, using the cache if available and not expired.
    ///
    /// Proactively refreshes the token if it's within the refresh margin of expiration.
    pub(crate) async fn get_token(&self, client: &reqwest::Client) -> Result<Option<String>> {
        match self {
            Self::None => Ok(None),
            Self::Token(token) => Ok(Some(token.clone())),
            Self::OAuth2 {
                token_endpoint,
                credential,
                extra_params,
                cached_token,
            } => {
                let mut guard = cached_token.lock().await;

                // Return cached token if it's still valid (with safety margin)
                if let Some(ref cached) = *guard {
                    let now = Instant::now();
                    if now + TOKEN_REFRESH_MARGIN < cached.expires_at {
                        return Ok(Some(cached.token.clone()));
                    }
                    tracing::debug!("token expiring soon, proactively refreshing");
                }

                // Fetch new token
                let (token, expires_in) =
                    Self::fetch_token(client, token_endpoint, credential, extra_params).await?;

                let expires_at = Instant::now() + Duration::from_secs(expires_in);
                *guard = Some(CachedToken {
                    token: token.clone(),
                    expires_at,
                });

                tracing::debug!(expires_in_secs = expires_in, "fetched new OAuth2 token");
                Ok(Some(token))
            }
        }
    }

    /// Invalidate the cached token (called on 401 to trigger a refresh).
    pub(crate) async fn invalidate(&self) {
        if let Self::OAuth2 { cached_token, .. } = self {
            let mut guard = cached_token.lock().await;
            *guard = None;
        }
    }
}
