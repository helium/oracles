use anyhow::Result;
use config::{Config, Environment, File};
use humantime_serde::re::humantime;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use solana::Token;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct TokenSetting {
    pub token: Token,
    pub default_price: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Settings {
    /// RUST_LOG compatible settings string. Default to
    /// "price=debug"
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,
    /// Source URL for price data. Required
    #[serde(default = "default_source", skip_serializing)]
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
    #[serde(default, deserialize_with = "from_json_or_struct")]
    pub tokens: Vec<TokenSetting>,
    /// How long to use a stale price in minutes
    #[serde(with = "humantime_serde", default = "default_stale_price_duration")]
    pub stale_price_duration: Duration,
}

fn default_source() -> String {
    "https://api.devnet.solana.com".to_string()
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

fn from_json_or_struct<'de, D, T>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let v = serde_json::Value::deserialize(de)?;
    match v {
        // Environment source provides a *string*; parse that string as JSON
        serde_json::Value::String(s) => serde_json::from_str(&s).map_err(serde::de::Error::custom),

        // Already a map/array/etc.; just deserialize it normally
        other => serde_json::from_value(other).map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    impl Settings {
        // Use Settings::new() constructor while injecting some env variables
        // that will be cleaned up when the test is done.
        fn test_with_token_env(token_setting: String) -> anyhow::Result<Self> {
            let settings = temp_env::with_vars(
                [
                    ("PRICE__OUTPUT_BUCKET", Some("test-bucket".to_string())),
                    (
                        "PRICE__TOKENS",
                        Some(token_setting).filter(|s| !s.is_empty()),
                    ),
                ],
                || Self::new::<PathBuf>(None),
            )?;

            Ok(settings)
        }
    }

    #[test]
    fn test_tokens_with_valid_json_strings_env() -> anyhow::Result<()> {
        let settings = Settings::test_with_token_env(
            serde_json::json!([
                {"token": "hnt"},
                {"token": "mobile", "default_price": 1000000},
                {"token": "iot"}
            ])
            .to_string(),
        )?;

        assert_eq!(
            settings.tokens,
            vec![
                TokenSetting {
                    token: Token::Hnt,
                    default_price: None
                },
                TokenSetting {
                    token: Token::Mobile,
                    default_price: Some(1000000)
                },
                TokenSetting {
                    token: Token::Iot,
                    default_price: None
                }
            ]
        );

        Ok(())
    }

    #[test]
    fn test_tokens_with_default_price_only() -> anyhow::Result<()> {
        let settings = Settings::test_with_token_env(
            r#"[{"token": "hnt", "default_price": 5000000}]"#.to_string(),
        )?;

        assert_eq!(
            settings.tokens,
            vec![TokenSetting {
                token: Token::Hnt,
                default_price: Some(5000000)
            }]
        );

        Ok(())
    }

    #[test]
    fn test_tokens_with_empty_tokens_list() -> anyhow::Result<()> {
        let settings = Settings::test_with_token_env("".to_string())?;
        assert!(settings.tokens.is_empty());
        Ok(())
    }

    #[test]
    fn test_tokens_with_invalid_json() -> anyhow::Result<()> {
        let settings = Settings::test_with_token_env(
            serde_json::json!([
                {"token": "hnt"},
                "invalid json",
                {"token": "mobile"},
            ])
            .to_string(),
        );

        let error = settings.unwrap_err().to_string();
        assert!(error.contains("expected struct TokenSetting"));
        assert!(error.contains("invalid json"));

        Ok(())
    }

    #[test]
    fn test_tokens_with_missing_token_field() {
        let settings = Settings::test_with_token_env(
            serde_json::json!([{"default_price": 1000000}]).to_string(),
        );

        let error = settings.unwrap_err().to_string();
        assert!(error.contains("missing field `token`"));
    }

    #[test]
    fn test_tokens_with_invalid_token_type() {
        let settings = Settings::test_with_token_env(
            serde_json::json!([{"token": "invalid_token"}]).to_string(),
        );

        let error = settings.unwrap_err().to_string();
        assert!(error.contains("unknown variant `invalid_token`"));
        assert!(error.contains("expected one of `sol`, `hnt`, `mobile`, `iot`, `dc`"));
    }

    #[test]
    fn test_tokens_with_malformed_json() {
        let settings = Settings::test_with_token_env(r#"{"token": "hnt"#.to_string());

        let error = settings.unwrap_err().to_string();
        assert!(error.contains("invalid type: map"));
    }

    #[test]
    fn test_tokens_with_extra_fields() -> anyhow::Result<()> {
        let settings = Settings::test_with_token_env(
            serde_json::json!([{
                "token": "hnt",
                "default_price": 1000000,
                "extra_field": "ignored"
            }])
            .to_string(),
        )?;

        assert_eq!(
            settings.tokens,
            vec![TokenSetting {
                token: Token::Hnt,
                default_price: Some(1000000)
            }]
        );
        Ok(())
    }

    #[test]
    fn test_tokens_with_null_default_price() -> anyhow::Result<()> {
        let settings = Settings::test_with_token_env(
            serde_json::json!([{"token": "hnt", "default_price": null}]).to_string(),
        )?;

        assert_eq!(
            settings.tokens,
            vec![TokenSetting {
                token: Token::Hnt,
                default_price: None
            }]
        );

        Ok(())
    }
}
