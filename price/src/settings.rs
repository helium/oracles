use anyhow::Result;
use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "price=debug"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Full Hermes price update URL including the `ids[]=<feed_id>` query
    /// parameter for the HNT feed. Required.
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub file_store: file_store::Settings,
    pub output_bucket: String,
    /// Folder for local cache of ingest data
    #[serde(default = "default_cache")]
    pub cache: PathBuf,
    /// Metrics settings
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    /// Tick interval (secs). Default = 60s.
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
    /// Optional static price override for testing. When set, the service
    /// emits this value every tick instead of calling the Hermes API.
    #[serde(default)]
    pub default_price: Option<u64>,
    /// How long to use a stale price in minutes
    #[serde(with = "humantime_serde", default = "default_stale_price_duration")]
    pub stale_price_duration: Duration,
    /// Database settings. Required when running `backfill`; unused by the
    /// server path.
    #[serde(default)]
    pub database: Option<db_store::Settings>,
    /// Iceberg catalog settings. When provided, live ticks also write to
    /// the `rewards.price` Iceberg table. Required by `backfill`.
    #[serde(default)]
    pub iceberg_settings: Option<helium_iceberg::Settings>,
}

/// Settings controlling the Iceberg backfill window.
///
/// Backfill covers [`start_after`, `stop_after`). Set `stop_after` to the date
/// Iceberg was first enabled in production so the backfiller does not overlap
/// with the daemon's real-time Iceberg writes.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BackfillSettings {
    /// Start of the backfill window. Defaults to UNIX_EPOCH (all available history).
    #[serde(default = "default_backfill_start_after")]
    pub start_after: DateTime<Utc>,
    /// End of the backfill window (exclusive).
    pub stop_after: DateTime<Utc>,
}

fn default_backfill_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_source() -> String {
    "https://hermes.pyth.network/v2/updates/price/latest?ids[]=649fdd7ec08e8e2a20f425729854e90293dcbe2376abc47197a14da6ff339756".to_string()
}

fn default_log() -> String {
    "price=info,file_store=info".to_string()
}

fn default_interval() -> Duration {
    humantime::parse_duration("1 minute").unwrap()
}

fn default_stale_price_duration() -> Duration {
    humantime::parse_duration("12 hours").unwrap()
}

fn default_cache() -> PathBuf {
    PathBuf::from("/opt/price/data")
}

impl Settings {
    /// Build a `BucketClient` for the output bucket using the shared
    /// `file_store` credentials.
    pub async fn output_bucket_client(&self) -> file_store::BucketClient {
        file_store::BucketClient::new(
            self.output_bucket.clone(),
            self.file_store.region.clone(),
            self.file_store.endpoint.clone(),
            self.file_store.access_key_id.clone(),
            self.file_store.secret_access_key.clone(),
        )
        .await
    }

    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "price_". For example
    /// "price_LOG_" will override the log setting.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `PRICE__DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(
                Environment::with_prefix("PRICE")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_default_price_override() -> anyhow::Result<()> {
        let settings = temp_env::with_vars(
            [
                ("PRICE__OUTPUT_BUCKET", Some("test-bucket".to_string())),
                ("PRICE__DEFAULT_PRICE", Some("100000000".to_string())),
            ],
            || Settings::new::<PathBuf>(None),
        )?;

        assert_eq!(settings.default_price, Some(100_000_000));
        assert_eq!(settings.output_bucket, "test-bucket");
        Ok(())
    }

    #[test]
    fn test_settings_template_parses() -> anyhow::Result<()> {
        let template = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pkg/settings-template.toml");
        // The template intentionally leaves output_bucket populated; no env
        // overrides so we exercise pure file parsing.
        let settings = temp_env::with_vars(Vec::<(&str, Option<String>)>::new(), || {
            Settings::new(Some(&template))
        })?;

        assert!(settings.source.contains("hermes.pyth.network"));
        assert_eq!(settings.output_bucket, "price");
        assert_eq!(settings.interval, Duration::from_secs(60));
        Ok(())
    }

    #[test]
    fn test_source_override() -> anyhow::Result<()> {
        let url = "https://example.test/v2/updates/price/latest?ids[]=abc";
        let settings = temp_env::with_vars(
            [
                ("PRICE__OUTPUT_BUCKET", Some("test-bucket".to_string())),
                ("PRICE__SOURCE", Some(url.to_string())),
            ],
            || Settings::new::<PathBuf>(None),
        )?;

        assert_eq!(settings.source, url);
        assert!(settings.default_price.is_none());
        Ok(())
    }
}
