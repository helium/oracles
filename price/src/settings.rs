use anyhow::Result;
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::Deserialize;
use serde_json;
use solana::Token;
use std::{path::Path, time::Duration};

#[derive(Debug, Deserialize, Clone)]
pub struct TokenSetting {
    pub token: Token,
    pub default_price: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "price=debug"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Source URL for price data. Required
    #[serde(default = "default_source")]
    pub source: String,
    /// Target output bucket details
    pub output: file_store::Settings,
    /// Folder for local cache of ingest data
    #[serde(default = "default_cache")]
    pub cache: String,
    /// Metrics settings
    #[serde(default)]
    pub metrics: poc_metrics::Settings,
    /// Tick interval (secs). Default = 60s.
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
    pub tokens: Vec<String>,
    /// How long to use a stale price in minutes
    #[serde(with = "humantime_serde", default = "default_stale_price_duration")]
    pub stale_price_duration: Duration,
}

fn default_source() -> String {
    "https://api.devnet.solana.com".to_string()
}

fn default_log() -> String {
    "price=debug".to_string()
}

fn default_interval() -> Duration {
    humantime::parse_duration("1 minute").unwrap()
}

fn default_stale_price_duration() -> Duration {
    humantime::parse_duration("12 hours").unwrap()
}

fn default_cache() -> String {
    "/var/data/price".to_string()
}

impl Settings {
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
        // Eg.. `MI_DEBUG=1 ./target/app` would set the `debug` key
        builder
            .add_source(
                Environment::with_prefix("PRICE")
                    .separator("__")
                    .list_separator(",")
                    .with_list_parse_key("tokens")
                    .try_parsing(true),
            )
            .build()
            .and_then(|config| config.try_deserialize())
    }

    pub fn tokens(&self) -> anyhow::Result<Vec<TokenSetting>> {
        let mut token_settings = Vec::new();
        let mut errors = Vec::new();

        for (i, token_json_str) in self.tokens.iter().enumerate() {
            match serde_json::from_str::<TokenSetting>(token_json_str) {
                Ok(token_setting) => token_settings.push(token_setting),
                Err(e) => {
                    errors.push(format!("Token {} at index {}: {}", token_json_str, i, e));
                }
            }
        }

        if !errors.is_empty() {
            anyhow::bail!("Failed to parse some tokens:\n{}", errors.join("\n"));
        }

        Ok(token_settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_file_store_settings() -> file_store::Settings {
        file_store::Settings {
            bucket: "test-bucket".to_string(),
            endpoint: None,
            region: "us-west-2".to_string(),
            access_key_id: None,
            secret_access_key: None,
        }
    }

    #[test]
    fn test_tokens_with_valid_json_strings() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![
                r#"{"token": "hnt"}"#.to_string(),
                r#"{"token": "mobile", "default_price": 1000000}"#.to_string(),
                r#"{"token": "iot"}"#.to_string(),
            ],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens().unwrap();
        assert_eq!(result.len(), 3);

        // Check first token (HNT)
        assert!(matches!(result[0].token, Token::Hnt));
        assert_eq!(result[0].default_price, None);

        // Check second token (Mobile with default price)
        assert!(matches!(result[1].token, Token::Mobile));
        assert_eq!(result[1].default_price, Some(1000000));

        // Check third token (IoT)
        assert!(matches!(result[2].token, Token::Iot));
        assert_eq!(result[2].default_price, None);
    }

    #[test]
    fn test_tokens_with_default_price_only() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![r#"{"token": "hnt", "default_price": 5000000}"#.to_string()],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens().unwrap();
        assert_eq!(result.len(), 1);

        assert!(matches!(result[0].token, Token::Hnt));
        assert_eq!(result[0].default_price, Some(5000000));
    }

    #[test]
    fn test_tokens_with_empty_tokens_list() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_tokens_with_invalid_json() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![
                r#"{"token": "hnt"}"#.to_string(),
                r#"invalid json"#.to_string(),
                r#"{"token": "mobile"}"#.to_string(),
            ],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens();
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to parse some tokens"));
        assert!(error.to_string().contains("invalid json"));
    }

    #[test]
    fn test_tokens_with_missing_token_field() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![r#"{"default_price": 1000000}"#.to_string()],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens();
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to parse some tokens"));
    }

    #[test]
    fn test_tokens_with_invalid_token_type() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![r#"{"token": "invalid_token"}"#.to_string()],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens();
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to parse some tokens"));
    }

    #[test]
    fn test_tokens_with_malformed_json() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![
                r#"{"token": "hnt"#.to_string(), // Missing closing brace
            ],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens();
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to parse some tokens"));
    }

    #[test]
    fn test_tokens_with_extra_fields() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![
                r#"{"token": "hnt", "default_price": 1000000, "extra_field": "ignored"}"#
                    .to_string(),
            ],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens().unwrap();
        assert_eq!(result.len(), 1);

        assert!(matches!(result[0].token, Token::Hnt));
        assert_eq!(result[0].default_price, Some(1000000));
    }

    #[test]
    fn test_tokens_with_null_default_price() {
        let settings = Settings {
            log: "debug".to_string(),
            custom_tracing: custom_tracing::Settings::default(),
            source: "https://api.devnet.solana.com".to_string(),
            output: create_test_file_store_settings(),
            cache: "/tmp/cache".to_string(),
            metrics: poc_metrics::Settings::default(),
            interval: std::time::Duration::from_secs(60),
            tokens: vec![r#"{"token": "hnt", "default_price": null}"#.to_string()],
            stale_price_duration: std::time::Duration::from_secs(43200),
        };

        let result = settings.tokens().unwrap();
        assert_eq!(result.len(), 1);

        assert!(matches!(result[0].token, Token::Hnt));
        assert_eq!(result[0].default_price, None);
    }
}
