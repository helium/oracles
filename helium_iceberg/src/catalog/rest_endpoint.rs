//! Direct REST API access for the Iceberg catalog.
//!
//! This module provides low-level HTTP access to the REST catalog API,
//! used for operations that cannot go through the standard `iceberg::Catalog` trait.

use super::auth::EndpointAuth;
use crate::{Error, Result, Settings};
use iceberg::TableIdent;
use iceberg_catalog_rest::CommitTableRequest;
use reqwest::StatusCode;
use std::collections::HashMap;

/// Response from the REST catalog's config endpoint.
#[derive(serde::Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
}

/// Resolved endpoint configuration for making direct REST API calls.
#[derive(Clone)]
pub(crate) struct RestEndpoint {
    /// The base URL for the REST catalog (may be overridden by server config).
    pub(super) uri: String,
    /// The optional prefix from the REST catalog config.
    pub(super) prefix: Option<String>,
    /// HTTP client for direct API calls.
    pub(super) client: reqwest::Client,
    /// Authentication strategy.
    pub(super) auth: EndpointAuth,
}

impl std::fmt::Debug for RestEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestEndpoint")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
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

        let (uri, prefix) = match response {
            Ok(resp) if resp.status() == StatusCode::UNAUTHORIZED => {
                // Token expired, invalidate and retry
                tracing::debug!("config fetch returned 401, invalidating token and retrying");
                auth.invalidate().await;
                let new_token = auth.get_token(&client).await?;
                match fetch_config(new_token).await {
                    Ok(retry_resp) if retry_resp.status().is_success() => {
                        Self::parse_config_response(retry_resp, settings).await?
                    }
                    Ok(retry_resp) => {
                        tracing::warn!(
                            status = %retry_resp.status(),
                            "config fetch failed after retry, using defaults"
                        );
                        (settings.catalog_uri.clone(), None)
                    }
                    Err(e) => {
                        tracing::warn!(%e, "config fetch failed after retry, using defaults");
                        (settings.catalog_uri.clone(), None)
                    }
                }
            }
            Ok(resp) if resp.status().is_success() => {
                Self::parse_config_response(resp, settings).await?
            }
            Ok(resp) => {
                tracing::warn!(
                    status = %resp.status(),
                    "config fetch failed, using defaults"
                );
                (settings.catalog_uri.clone(), None)
            }
            Err(e) => {
                tracing::warn!(%e, "config fetch failed, using defaults");
                (settings.catalog_uri.clone(), None)
            }
        };

        Ok(Self {
            uri,
            prefix,
            client,
            auth,
        })
    }

    /// Parse the catalog config response to extract URI and prefix.
    async fn parse_config_response(
        response: reqwest::Response,
        settings: &Settings,
    ) -> Result<(String, Option<String>)> {
        let config: CatalogConfigResponse = response
            .json()
            .await
            .map_err(|e| Error::Catalog(format!("failed to parse config: {e}")))?;

        let uri = config
            .overrides
            .get("uri")
            .cloned()
            .unwrap_or_else(|| settings.catalog_uri.clone());

        // Prefix can come from defaults or overrides (overrides take precedence)
        let prefix = config
            .overrides
            .get("prefix")
            .or_else(|| config.defaults.get("prefix"))
            .cloned();

        Ok((uri, prefix))
    }

    /// Build the URL for a table endpoint.
    fn table_url(&self, table_ident: &TableIdent) -> String {
        let namespace = table_ident.namespace.to_url_string();
        let parts: Vec<&str> = [self.uri.as_str(), "v1"]
            .into_iter()
            .chain(self.prefix.as_deref())
            .chain(["namespaces", &namespace, "tables", &table_ident.name])
            .collect();
        parts.join("/")
    }

    /// Send a commit table request directly to the REST catalog.
    pub(crate) async fn commit_table(&self, request: &CommitTableRequest) -> Result<()> {
        let url = request
            .identifier
            .as_ref()
            .map(|ident| self.table_url(ident))
            .ok_or_else(|| Error::Catalog("table identifier required for commit".into()))?;

        let response = self.send_commit(&url, request).await?;

        match response.status() {
            StatusCode::OK => return Ok(()),
            StatusCode::UNAUTHORIZED => {
                // Invalidate cached token and retry once
                self.auth.invalidate().await;
                let retry_response = self.send_commit(&url, request).await?;
                return Self::handle_commit_response(retry_response).await;
            }
            _ => {}
        }

        Self::handle_commit_response(response).await
    }

    /// Send the HTTP POST for a commit request, attaching auth.
    async fn send_commit(
        &self,
        url: &str,
        request: &CommitTableRequest,
    ) -> Result<reqwest::Response> {
        let mut http_request = self.client.post(url).json(request);

        if let Some(token) = self.auth.get_token(&self.client).await? {
            http_request = http_request.bearer_auth(token);
        }

        http_request
            .send()
            .await
            .map_err(|e| Error::Catalog(format!("commit request failed: {e}")))
    }

    /// Map an HTTP response to a Result.
    async fn handle_commit_response(response: reqwest::Response) -> Result<()> {
        let status = response.status();
        match status {
            StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => {
                let body = response.text().await.unwrap_or_default();
                Err(Error::CommitConflict(body))
            }
            StatusCode::NOT_FOUND => Err(Error::Catalog("table not found".into())),
            _ => {
                let body = response.text().await.unwrap_or_default();
                if status == StatusCode::BAD_REQUEST && is_conflict_body(&body) {
                    Err(Error::CommitConflict(body))
                } else {
                    Err(Error::Catalog(format!(
                        "unexpected status {status}: {body}"
                    )))
                }
            }
        }
    }
}

/// Check whether a 400 response body indicates a commit conflict.
///
/// Some catalogs (e.g. Polaris) return 400 instead of 409 when table
/// requirements fail. This detects those cases by looking for known
/// conflict indicators in the response body.
fn is_conflict_body(body: &str) -> bool {
    let lower = body.to_lowercase();
    lower.contains("sequence number") || lower.contains("requirement")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_conflict_body_sequence_number() {
        assert!(is_conflict_body(
            "Cannot add snapshot with sequence number 6520 older than last sequence number 6520"
        ));
    }

    #[test]
    fn test_is_conflict_body_requirement() {
        assert!(is_conflict_body(
            "Table requirement check failed: ref main snapshot 42 != 43"
        ));
    }

    #[test]
    fn test_is_conflict_body_case_insensitive() {
        assert!(is_conflict_body("SEQUENCE NUMBER mismatch"));
        assert!(is_conflict_body("Requirement failed"));
    }

    #[test]
    fn test_is_conflict_body_unrelated_error() {
        assert!(!is_conflict_body("invalid schema"));
        assert!(!is_conflict_body("malformed request body"));
    }

    #[test]
    fn test_is_conflict_body_empty() {
        assert!(!is_conflict_body(""));
    }
}
