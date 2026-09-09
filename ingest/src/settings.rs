use crate::authorization::AuthorizedKeys;
use anyhow::Context;
use config::{Config, Environment, File};
use file_store::BucketClient;
use helium_crypto::{Network, PublicKeyBinary};
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    path::Path,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default
    /// "ingest=debug,poc_store=info"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Mode to run the server in (iot or mobile). Required
    pub mode: Mode,
    /// Listen address. Required. Default is 0.0.0.0:9081
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,
    /// Local folder for storing intermediate files
    #[serde(default = "default_cache")]
    pub cache: PathBuf,
    /// Network required in all public keys:  mainnet | testnet
    #[serde(default = "default_network", skip_serializing)]
    pub network: Network,
    /// Timeout of session key offer in seconds
    #[serde(
        with = "humantime_serde",
        default = "default_session_key_offer_timeout"
    )]
    pub session_key_offer_timeout: Duration,
    /// Timeout of session key session in seconds
    #[serde(with = "humantime_serde", default = "default_session_key_timeout")]
    pub session_key_timeout: Duration,
    #[serde(default)]
    pub file_store: file_store::Settings,
    /// Primary bucket every ingested file is written to. Required.
    pub output_bucket: String,
    /// Optional extra buckets that receive a byte-for-byte copy of every file
    /// written to `output_bucket`, under the same key. Keyed by a name of the
    /// operator's choosing (`[additional_output_buckets.r2]`), which is what
    /// appears in log lines and config errors.
    ///
    /// Each entry is self-contained: it carries its own `region`, `endpoint`
    /// and credentials and inherits nothing from `[file_store]`. That is what
    /// lets a mirror live on a different provider from `output_bucket` — an R2
    /// bucket needs its own endpoint and its own key pair, and silently pairing
    /// those with the S3 account's region or credentials would only produce a
    /// client that cannot authenticate anywhere.
    ///
    /// A map rather than a list so that entries stay reachable from the
    /// environment: `config` builds nested maps from `INGEST__`-prefixed
    /// variables but cannot build sequences, so a list would force every
    /// mirror's secret into the settings file. See
    /// `ingest/tests/settings_env.rs`.
    ///
    /// A file is only removed from the local cache once every bucket has it.
    #[serde(default)]
    pub additional_output_buckets: BTreeMap<String, file_store::BucketSettings>,
    /// Timeout of session key session in seconds
    #[serde(with = "humantime_serde", default = "default_roll_time")]
    pub roll_time: Duration,
    /// API token required as part of a Bearer authentication GRPC request
    /// header. Used only by the mobile mode currently
    #[serde(skip_serializing)]
    pub token: Option<String>,
    /// Target output bucket details Metrics settings
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    /// Public keys authorized to submit carrier-signed reports
    /// (`NetworkKeyRole::MobileCarrier`). Comma-separated b58 keys. Required in
    /// "mobile" mode — at least one key; see [`Settings::authorized_keys`].
    /// Ignored in "chain" mode. Replaces the mobile-config authorization lookup.
    #[serde(default)]
    pub carrier_authorized_keys: String,
    /// Key that can sign Chain Rewardable Entities messages
    pub chain_rewardable_entities_auth_key: Option<String>,
    /// HIP-150: public keys authorized to issue data transfer multiplier
    /// tickets. Comma-separated b58 keys. Ignored in "chain" mode.
    ///
    /// Unlike `carrier_authorized_keys` this may be empty, and is by default.
    /// The oracle release ships before any ticket can be issued, so requiring a
    /// key here would block the release on provisioning one. Empty fails
    /// closed — every ticket is rejected — and logs a warning at startup so an
    /// unconfigured deployment is visible.
    #[serde(default)]
    pub data_transfer_multiplier_authorized_keys: String,
    /// HIP-150: how old a data transfer multiplier ticket's signed timestamp
    /// may be before ingest refuses it.
    ///
    /// A signature never expires, so without this a captured ticket is
    /// replayable forever. The packet verifier checks freshness again when it
    /// verifies; this is the cheap boundary check that keeps replayed tickets
    /// out of the pipeline entirely.
    #[serde(with = "humantime_serde", default = "default_ticket_max_age")]
    pub data_transfer_multiplier_ticket_max_age: Duration,
}

fn default_network() -> Network {
    Network::MainNet
}

fn default_cache() -> PathBuf {
    PathBuf::from("/opt/ingest/data")
}

fn default_ticket_max_age() -> Duration {
    humantime::parse_duration("10 minutes").unwrap()
}

fn default_roll_time() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

fn default_session_key_timeout() -> Duration {
    humantime::parse_duration("30 minutes").unwrap()
}

fn default_session_key_offer_timeout() -> Duration {
    humantime::parse_duration("5 seconds").unwrap()
}

fn default_listen_addr() -> SocketAddr {
    "0.0.0.0:9081".parse().unwrap()
}

fn default_log() -> String {
    "ingest=debug,poc_store=info".to_string()
}

/// Mode to deploy the ingest engine in. Each mode exposes different submission
/// grpc methods
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Mobile,
    Chain,
}

impl Settings {
    /// Load Settings from a given path. Settings are loaded from a given
    /// optional path and can be overridden with environment variables.
    ///
    /// Environment overrides have the same name as the entries in the settings
    /// file in uppercase and prefixed with "ENTROPY_". For example
    /// "ENTROPY_LOG" will override the log setting.
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, config::ConfigError> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            // Add optional settings file
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `MI_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(
                Environment::with_prefix("INGEST")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }

    /// Connects the primary output bucket and every additional bucket that
    /// mirrors it.
    ///
    /// Each additional bucket connects from its own settings alone. Nothing is
    /// inherited from `[file_store]`: a mirror may be on another provider
    /// entirely (an R2 bucket alongside an S3 one), where borrowing a region or
    /// a key pair from the primary would build a client that authenticates
    /// against neither. Clients are cached by connection parameters, so buckets
    /// that genuinely share settings still share one client.
    pub async fn output_buckets(&self) -> anyhow::Result<(BucketClient, Vec<BucketClient>)> {
        self.validate_additional_output_buckets()?;

        let primary = BucketClient {
            client: self.file_store.connect().await,
            bucket: self.output_bucket.clone(),
        };

        let mut additional = Vec::with_capacity(self.additional_output_buckets.len());
        for bucket in self.additional_output_buckets.values() {
            additional.push(bucket.connect().await);
        }

        Ok((primary, additional))
    }

    /// Rejects additional-bucket entries that cannot work as written.
    ///
    /// The S3 client only installs static credentials when it has *both* halves
    /// of the key pair, and otherwise silently falls back to the ambient AWS
    /// credential chain. Against a non-AWS endpoint that fallback cannot
    /// succeed, so a half-configured entry — a typo in one of the two key
    /// names, most likely — would fail every upload at runtime instead of at
    /// startup. Catch it here while the operator is still looking.
    fn validate_additional_output_buckets(&self) -> anyhow::Result<()> {
        for (name, bucket) in &self.additional_output_buckets {
            let settings = &bucket.settings;
            if bucket.bucket.trim().is_empty() {
                anyhow::bail!("additional_output_buckets entry {name:?} is missing a bucket name");
            }
            match (&settings.access_key_id, &settings.secret_access_key) {
                (Some(_), None) | (None, Some(_)) => anyhow::bail!(
                    "additional_output_buckets entry {name:?} (bucket {:?}) sets only one of \
                     access_key_id / secret_access_key; set both or neither",
                    bucket.bucket
                ),
                _ => {}
            }
        }
        Ok(())
    }

    /// The static authorization allow-list, parsed from settings. The list is
    /// required: an empty list is an error (mirrors mobile-verifier's
    /// `banning_authorized_keys` and mobile-packet-verifier's `routing_keys`),
    /// so a misconfiguration fails at startup rather than silently rejecting
    /// every carrier report.
    pub fn authorized_keys(&self) -> anyhow::Result<AuthorizedKeys> {
        let carrier =
            parse_authorized_keys("carrier_authorized_keys", &self.carrier_authorized_keys)?;

        // HIP-150 ticket signers, unlike carrier keys, are optional — see the
        // field docs. Warn rather than fail so the gap is visible in logs.
        let data_transfer_multiplier =
            parse_optional_authorized_keys(&self.data_transfer_multiplier_authorized_keys)
                .context("settings parsing data_transfer_multiplier_authorized_keys")?;
        if data_transfer_multiplier.is_empty() {
            tracing::warn!(
                "no data_transfer_multiplier_authorized_keys configured; \
                 all data transfer multiplier tickets will be rejected"
            );
        }

        Ok(AuthorizedKeys::new(carrier, data_transfer_multiplier))
    }
}

/// Parse a comma-separated list of b58 public keys into a non-empty set. Blank
/// entries are ignored; a list that yields no keys is an error, since each
/// authorized-key role must be configured.
fn parse_authorized_keys(setting: &str, keys: &str) -> anyhow::Result<HashSet<PublicKeyBinary>> {
    let parsed = parse_optional_authorized_keys(keys)
        .with_context(|| format!("settings parsing {setting}"))?;

    if parsed.is_empty() {
        anyhow::bail!("no keys provided in settings for {setting}");
    }
    Ok(parsed)
}

/// Parse a comma-separated list of b58 public keys, allowing the empty set.
/// Blank entries are ignored; a malformed key is still an error, so a typo
/// fails at startup rather than silently narrowing the allow-list.
fn parse_optional_authorized_keys(keys: &str) -> anyhow::Result<HashSet<PublicKeyBinary>> {
    keys.split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(|key| PublicKeyBinary::from_str(key).with_context(|| format!("invalid key: {key}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{parse_authorized_keys, Settings};

    const KEY: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

    #[test]
    fn additional_output_buckets_default_to_none() {
        let settings = settings_from_toml(
            r#"
mode = "mobile"
output_bucket = "ingest-bucket"
"#,
        );
        assert_eq!("ingest-bucket", settings.output_bucket);
        assert!(settings.additional_output_buckets.is_empty());
        assert!(settings.validate_additional_output_buckets().is_ok());
    }

    /// The shape this exists for: an S3 primary and an R2 mirror, each with its
    /// own endpoint and its own key pair.
    #[test]
    fn an_r2_mirror_keeps_its_own_endpoint_and_credentials() {
        let settings = settings_from_toml(
            r#"
mode = "mobile"
output_bucket = "ingest-bucket"

[additional_output_buckets.r2]
bucket = "ingest-mirror"
endpoint = "https://accountid.r2.cloudflarestorage.com"
region = "auto"
access_key_id = "r2-key-id"
secret_access_key = "r2-secret"

[file_store]
region = "us-west-2"
"#,
        );

        let mirror = &settings.additional_output_buckets["r2"];
        assert_eq!("ingest-mirror", mirror.bucket);
        assert_eq!(
            Some("https://accountid.r2.cloudflarestorage.com".to_string()),
            mirror.settings.endpoint
        );
        assert_eq!(Some("auto".to_string()), mirror.settings.region);
        assert_eq!(Some("r2-key-id".to_string()), mirror.settings.access_key_id);
        assert_eq!(
            Some("r2-secret".to_string()),
            mirror.settings.secret_access_key
        );

        // Nothing bled over from the S3 primary.
        assert_eq!(Some("us-west-2".to_string()), settings.file_store.region);
        assert_eq!(None, settings.file_store.endpoint);

        assert!(settings.validate_additional_output_buckets().is_ok());
    }

    #[test]
    fn a_mirror_inherits_nothing_from_the_primary() {
        let settings = settings_from_toml(
            r#"
mode = "mobile"
output_bucket = "ingest-bucket"

[additional_output_buckets.r2]
bucket = "ingest-mirror"

[file_store]
region = "us-west-2"
endpoint = "https://s3.example.com"
"#,
        );

        // An entry that names only a bucket carries only a bucket. Borrowing
        // the primary's endpoint would silently aim an R2 mirror at S3.
        let mirror = &settings.additional_output_buckets["r2"];
        assert_eq!(None, mirror.settings.region);
        assert_eq!(None, mirror.settings.endpoint);
    }

    /// `main` logs the whole settings struct as JSON at startup, so a mirror's
    /// credentials must not survive serialization. `file_store::Settings` marks
    /// the pair `skip_serializing`; this checks the attribute still bites
    /// through the `flatten` that `BucketSettings` puts in front of it.
    #[test]
    fn a_settings_dump_omits_mirror_credentials() {
        let settings = settings_from_toml(
            r#"
mode = "mobile"
output_bucket = "ingest-bucket"

[additional_output_buckets.r2]
bucket = "ingest-mirror"
endpoint = "https://accountid.r2.cloudflarestorage.com"
access_key_id = "r2-key-id"
secret_access_key = "r2-secret"
"#,
        );

        let dumped = serde_json::to_string(&settings).expect("serialize settings");
        assert!(!dumped.contains("r2-secret"), "{dumped}");
        assert!(!dumped.contains("r2-key-id"), "{dumped}");
        assert!(dumped.contains("ingest-mirror"), "{dumped}");
    }

    #[test]
    fn half_a_credential_pair_is_an_error() {
        let settings = settings_from_toml(
            r#"
mode = "mobile"
output_bucket = "ingest-bucket"

[additional_output_buckets.r2]
bucket = "ingest-mirror"
endpoint = "https://accountid.r2.cloudflarestorage.com"
access_key_id = "r2-key-id"
"#,
        );

        // Without both halves the client silently falls back to the ambient AWS
        // credential chain, which cannot authenticate against R2.
        let err = settings
            .validate_additional_output_buckets()
            .expect_err("half a credential pair must not start up");
        assert!(err.to_string().contains("ingest-mirror"), "{err}");
    }

    #[test]
    fn empty_authorized_keys_is_an_error() {
        assert!(parse_authorized_keys("carrier_authorized_keys", "").is_err());
        // Whitespace / stray commas still yield no keys — also an error.
        assert!(parse_authorized_keys("carrier_authorized_keys", "  , ,").is_err());
    }

    #[test]
    fn invalid_key_is_an_error() {
        assert!(parse_authorized_keys("carrier_authorized_keys", "not-a-b58-key").is_err());
    }

    #[test]
    fn parses_and_dedupes_keys() {
        let keys = parse_authorized_keys("carrier_authorized_keys", &format!("{KEY}, {KEY}"))
            .expect("valid keys");
        assert_eq!(keys.len(), 1);
    }

    fn settings_from_toml(toml: &str) -> Settings {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("settings.toml");
        std::fs::write(&path, toml).expect("write settings");
        Settings::new(Some(&path)).expect("parse settings")
    }
}
