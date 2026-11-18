//! Process-wide configuration: an optional TOML file overlaid with `ICICLE__*`
//! environment variables.
//!
//! Only what is true of the whole process lives here. Everything about a data
//! type -- its bucket, its ledger, its table -- lives with that data type, in
//! `data::<type>::Settings`, and appears here as one optional section.

use crate::data::mobile_hotspot_history;
use config::{Config, Environment, File};
use glacon_helium::FreshState;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Settings {
    /// `RUST_LOG`-compatible log filter.
    #[serde(default = "default_log")]
    pub log: String,

    #[serde(default)]
    pub custom_tracing: custom_tracing::Settings,

    #[serde(default)]
    pub metrics: crate::metrics::Settings,

    /// The mobile hotspot history route. Omit the section to leave it out of
    /// this process.
    #[serde(default)]
    pub mobile_hotspot_history: Option<mobile_hotspot_history::Settings>,
}

fn default_log() -> String {
    "icicle=info".to_string()
}

impl Settings {
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> anyhow::Result<Self> {
        let mut builder = Config::builder();

        if let Some(file) = path {
            builder = builder
                .add_source(File::with_name(&file.as_ref().to_string_lossy()).required(false));
        }

        let settings: Self = builder
            .add_source(Environment::with_prefix("ICICLE").separator("__"))
            .build()?
            .try_deserialize()?;

        settings.validate()?;
        Ok(settings)
    }

    /// Names of the routes this process is configured to run.
    pub fn configured(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.mobile_hotspot_history.is_some() {
            names.push(mobile_hotspot_history::ROUTE);
        }
        names
    }

    fn validate(&self) -> anyhow::Result<()> {
        if let Some(history) = &self.mobile_hotspot_history {
            history.validate()?;
        }

        if self.configured().is_empty() {
            anyhow::bail!("no data types configured: nothing to run");
        }

        Ok(())
    }
}

/// `glacon`'s [`FreshState`] is not `Deserialize`, so configuration goes
/// through this mirror of it.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FreshStateSetting {
    /// Refuse to start when the ledger is empty but the table already holds
    /// rows. The default: a lost state volume becomes a loud failure instead
    /// of a silent re-ingest.
    #[default]
    Refuse,
    /// Start anyway, each input from its `start_after`.
    Proceed,
}

impl From<FreshStateSetting> for FreshState {
    fn from(value: FreshStateSetting) -> Self {
        match value {
            FreshStateSetting::Refuse => FreshState::Refuse,
            FreshStateSetting::Proceed => FreshState::Proceed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_settings(body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new()
            .suffix(".toml")
            .tempfile()
            .expect("temp file");
        f.write_all(body.as_bytes()).expect("write");
        f.flush().expect("flush");
        f
    }

    const HISTORY: &str = r#"
[mobile_hotspot_history.route]
state_dir = "/var/lib/icicle/mobile_hotspot_history/state"

[mobile_hotspot_history.output]
spool_dir = "/var/lib/icicle/mobile_hotspot_history/spool"

[mobile_hotspot_history.output.catalog]
catalog_uri = "http://polaris:8181/api/catalog"
catalog_name = "mobile"

[mobile_hotspot_history.inputs.chain]
start_after = "1970-01-01T00:00:00Z"
[mobile_hotspot_history.inputs.chain.bucket]
bucket = "ingest-chain"
"#;

    #[test]
    fn the_route_loads_and_defaults_to_chain_mobile_hotspot_history() {
        let f = write_settings(HISTORY);
        let settings = Settings::new(Some(f.path())).expect("valid settings");
        let history = settings
            .mobile_hotspot_history
            .as_ref()
            .expect("route configured");

        assert_eq!(
            history
                .inputs
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [mobile_hotspot_history::INPUT]
        );
        assert_eq!(history.inputs["chain"].bucket.bucket, "ingest-chain");
        assert_eq!(history.inputs["chain"].start_after.timestamp(), 0);
        assert_eq!(history.table.namespace, "chain");
        assert_eq!(history.table.name, "mobile_hotspot_history");
        assert_eq!(history.fresh_state, FreshStateSetting::Refuse);
        assert_eq!(settings.configured(), [mobile_hotspot_history::ROUTE]);
    }

    #[test]
    fn a_process_with_no_data_types_is_refused() {
        let f = write_settings("log = \"icicle=debug\"\n");
        let err = Settings::new(Some(f.path())).expect_err("must be refused");
        assert!(
            err.to_string().contains("no data types configured"),
            "unexpected error: {err}"
        );
    }

    /// A done batch forgotten and then listed again is delivered again, so
    /// this inversion must not reach the route.
    #[test]
    fn a_retention_shorter_than_the_poller_offset_is_refused() {
        let body = format!(
            "[mobile_hotspot_history.route]\nstate_dir = \"/tmp/s\"\nprocessed_retention = \"5m\"\n{}",
            HISTORY.replace(
                "[mobile_hotspot_history.route]\nstate_dir = \"/var/lib/icicle/mobile_hotspot_history/state\"\n",
                ""
            )
        );
        let f = write_settings(&body);
        let err = Settings::new(Some(f.path())).expect_err("must be refused");
        assert!(
            err.to_string().contains("processed_retention"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn a_route_with_no_inputs_is_refused() {
        let body = format!(
            "{}\n[mobile_hotspot_history.inputs]\n",
            HISTORY
                .split("[mobile_hotspot_history.inputs.chain]")
                .next()
                .expect("prefix")
        );
        let f = write_settings(&body);
        let err = Settings::new(Some(f.path())).expect_err("must be refused");
        assert!(
            err.to_string().contains("no inputs"),
            "unexpected error: {err}"
        );
    }

    /// The template ships as documentation and as the starting point for a
    /// deployment, so it has to stay loadable as the settings evolve.
    #[test]
    fn the_shipped_settings_template_loads() {
        let template = concat!(env!("CARGO_MANIFEST_DIR"), "/pkg/settings-template.toml");
        let settings = Settings::new(Some(template)).expect("template must load");
        let history = settings
            .mobile_hotspot_history
            .as_ref()
            .expect("route configured");

        assert_eq!(
            history
                .inputs
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [mobile_hotspot_history::INPUT]
        );
        assert_eq!(history.inputs["chain"].start_after.timestamp(), 0);
        assert_eq!(history.table.namespace, "chain");
        assert_eq!(history.table.name, "mobile_hotspot_history");
        assert_eq!(history.fresh_state, FreshStateSetting::Refuse);
    }

    #[test]
    fn fresh_state_is_spelled_in_snake_case() {
        let body = format!("[mobile_hotspot_history]\nfresh_state = \"proceed\"\n{HISTORY}");
        let f = write_settings(&body);
        let settings = Settings::new(Some(f.path())).expect("valid settings");
        assert_eq!(
            settings.mobile_hotspot_history.expect("route").fresh_state,
            FreshStateSetting::Proceed
        );
    }
}
