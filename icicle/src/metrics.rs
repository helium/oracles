//! The Prometheus scrape endpoint.
//!
//! The service emits no metrics of its own: the routes do. See
//! `glacon_helium::metrics` and `glacon_core::metrics` for the series --
//! `glacon_records_decoded_total`, `glacon_decode_failures_total`,
//! `glacon_output_rows_total`, `glacon_processed_batches_total`,
//! `glacon_batch_lag_ms`, `glacon_last_batch_timestamp_ms`, ... -- each
//! labelled with `route` and either `input` or `output`.

use anyhow::Context;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Settings {
    #[serde(default = "default_endpoint")]
    pub endpoint: SocketAddr,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            endpoint: default_endpoint(),
        }
    }
}

fn default_endpoint() -> SocketAddr {
    "0.0.0.0:19000".parse().expect("valid socket address")
}

pub fn start(settings: &Settings) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(settings.endpoint)
        .install()
        .with_context(|| {
            format!(
                "installing the Prometheus scrape endpoint on {}",
                settings.endpoint
            )
        })?;
    tracing::info!(endpoint = %settings.endpoint, "metrics scrape endpoint listening");
    Ok(())
}
