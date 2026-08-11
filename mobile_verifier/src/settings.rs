use crate::authorization::AuthorizedKeys;
use anyhow::Context;
use chrono::{DateTime, Utc};
use config::{Config, ConfigError, Environment, File};
use helium_crypto::PublicKeyBinary;
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct Buckets {
    pub ingest: file_store::BucketSettings,
    pub output: file_store::BucketSettings,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Defsault to
    /// "mobile_verifier=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    pub buckets: Buckets,
    /// Cache location for generated verified reports
    #[serde(default = "default_cache")]
    pub cache: PathBuf,
    /// Reward period in hours. (Default is 24 hours)
    #[serde(with = "humantime_serde", default = "default_reward_period")]
    pub reward_period: Duration,
    #[serde(with = "humantime_serde", default = "default_reward_period_offset")]
    pub reward_period_offset: Duration,
    pub database: db_store::Settings,
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    /// Public keys authorized to submit hotspot ban reports
    /// (`NetworkKeyRole::Banning`). Comma-separated b58 keys. Required — at least
    /// one key; see [`Settings::authorized_keys`]. Replaces the mobile-config
    /// authorization lookup.
    #[serde(default)]
    pub banning_authorized_keys: String,
    /// How often the in-memory snapshot of known gateways is refreshed from the
    /// Trino inventory table (see [`crate::gateway`]).
    #[serde(with = "humantime_serde", default = "default_gateway_refresh_interval")]
    pub gateway_refresh_interval: Duration,
    #[serde(default = "default_start_after")]
    pub start_after: DateTime<Utc>,
    pub iceberg_settings: Option<helium_iceberg::Settings>,
    /// Trino query client. Required: each epoch the reward pipeline recovers the
    /// HNT price from on-chain deployer-cap data via Trino
    /// (`solana.public.dao_epoch_infos` joined to `sub_dao_epoch_infos`) instead
    /// of a price feed. It also reads the burned data-transfer sessions that
    /// size and split the reward pool (`data_transfer.burned_sessions`).
    pub trino: trino_client::Settings,
    // Geofencing settings
    #[serde(default = "default_usa_and_mexico_geofence_regions")]
    pub usa_and_mexico_geofence_regions: PathBuf,
    #[serde(default = "default_fencing_resolution")]
    pub usa_and_mexico_fencing_resolution: u8,
}

fn default_fencing_resolution() -> u8 {
    7
}

fn default_gateway_refresh_interval() -> Duration {
    humantime::parse_duration("1 hour").unwrap()
}

fn default_log() -> String {
    "mobile_verifier=info,file_store=info".to_string()
}

fn default_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_reward_period() -> Duration {
    humantime::parse_duration("24 hours").unwrap()
}

fn default_reward_period_offset() -> Duration {
    humantime::parse_duration("60 minutes").unwrap()
}

fn default_cache() -> PathBuf {
    PathBuf::from("/opt/mobile-verifier/data")
}

fn default_usa_and_mexico_geofence_regions() -> PathBuf {
    PathBuf::from("/opt/mobile-verifier/geofence")
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "VERIFY_". For example
    /// "VERIFY_DATABASE_URL" will override the data base url.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of VERIFY)
        // Eg.. `INJECT_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(
                Environment::with_prefix("MV")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn usa_and_mexico_region_paths(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let paths = std::fs::read_dir(&self.usa_and_mexico_geofence_regions)?;
        Ok(paths
            .into_iter()
            .collect::<Result<Vec<std::fs::DirEntry>, std::io::Error>>()?
            .into_iter()
            .map(|path| path.path())
            .collect())
    }

    pub fn usa_and_mexico_fencing_resolution(&self) -> anyhow::Result<h3o::Resolution> {
        Ok(h3o::Resolution::try_from(
            self.usa_and_mexico_fencing_resolution,
        )?)
    }

    pub fn store_base_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.cache)
    }

    /// The static authorization allow-list, parsed from settings. The list is
    /// required: an empty list is an error (mirrors mobile-packet-verifier's
    /// `routing_keys`), so a misconfiguration fails at startup rather than
    /// silently rejecting every report.
    pub fn authorized_keys(&self) -> anyhow::Result<AuthorizedKeys> {
        Ok(AuthorizedKeys::new(parse_authorized_keys(
            "banning_authorized_keys",
            &self.banning_authorized_keys,
        )?))
    }
}

/// Parse a comma-separated list of b58 public keys into a non-empty set. Blank
/// entries are ignored; a list that yields no keys is an error, since each
/// authorized-key role must be configured.
fn parse_authorized_keys(setting: &str, keys: &str) -> anyhow::Result<HashSet<PublicKeyBinary>> {
    let parsed: HashSet<PublicKeyBinary> = keys
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(|key| {
            PublicKeyBinary::from_str(key)
                .with_context(|| format!("settings parsing {setting}: {key}"))
        })
        .collect::<anyhow::Result<_>>()?;

    if parsed.is_empty() {
        anyhow::bail!("no keys provided in settings for {setting}");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::parse_authorized_keys;

    const KEY: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

    #[test]
    fn empty_authorized_keys_is_an_error() {
        assert!(parse_authorized_keys("banning_authorized_keys", "").is_err());
        // Whitespace / stray commas still yield no keys — also an error.
        assert!(parse_authorized_keys("banning_authorized_keys", "  , ,").is_err());
    }

    #[test]
    fn invalid_key_is_an_error() {
        assert!(parse_authorized_keys("banning_authorized_keys", "not-a-b58-key").is_err());
    }

    #[test]
    fn parses_and_dedupes_keys() {
        let keys = parse_authorized_keys("banning_authorized_keys", &format!("{KEY}, {KEY}"))
            .expect("valid keys");
        assert_eq!(keys.len(), 1);
    }
}
