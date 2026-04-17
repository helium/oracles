//! Catalog endpoint configuration.
//!
//! Resolves the catalog URI and optional prefix from the REST catalog's
//! `/v1/config` endpoint and holds the authentication strategy used by
//! higher-level catalog operations.

use super::auth::EndpointAuth;
use crate::{Result, Settings};
use reqwest::StatusCode;

/// Resolved endpoint configuration.
#[derive(Clone)]
pub(crate) struct RestEndpoint {
    /// Authentication strategy.
    pub(super) auth: EndpointAuth,
}

impl std::fmt::Debug for RestEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestEndpoint").finish_non_exhaustive()
    }
}

impl RestEndpoint {
    /// Resolve the REST endpoint by fetching the catalog config from the server.
    pub(super) async fn resolve(settings: &Settings) -> Result<Self> {
        let client = reqwest::Client::new();
        let auth = EndpointAuth::from_settings(settings);

        let mut config_url = format!("{}/v1/config", settings.catalog_uri);
        if let Some(ref warehouse) = settings.warehouse {
            config_url = format!("{config_url}?warehouse={warehouse}");
        }

        // Helper to fetch config with a token
        let fetch_config = |token: Option<String>| {
            let client = &client;
            let config_url = &config_url;
            async move {
                let mut request = client.get(config_url);
                if let Some(t) = token {
                    request = request.bearer_auth(t);
                }
                request.send().await
            }
        };

        // Try to fetch server config, with 401 retry
        let token = auth.get_token(&client).await?;
        let response = fetch_config(token).await;

        match response {
            Ok(resp) if resp.status() == StatusCode::UNAUTHORIZED => {
                // Token expired, invalidate and retry
                tracing::debug!("config fetch returned 401, invalidating token and retrying");
                auth.invalidate().await;
                let new_token = auth.get_token(&client).await?;
                match fetch_config(new_token).await {
                    Ok(retry_resp) if retry_resp.status().is_success() => {}
                    Ok(retry_resp) => {
                        tracing::warn!(
                            status = %retry_resp.status(),
                            "config fetch failed after retry, using defaults"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(%e, "config fetch failed after retry, using defaults");
                    }
                }
            }
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                tracing::warn!(
                    status = %resp.status(),
                    "config fetch failed, using defaults"
                );
            }
            Err(e) => {
                tracing::warn!(%e, "config fetch failed, using defaults");
            }
        }

        Ok(Self { auth })
    }
}
