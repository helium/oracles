//! Settings of the mobile hotspot history route: its bucket, its ledger, its
//! table. The process-wide settings live in [`crate::settings`], which holds
//! this as one section.

use super::{MobileHotspotHistoryTable, DEFAULT_NAMESPACE, DEFAULT_TABLE_NAME};
use glacon_helium::{FileStoreInputSettings, FreshState, IcebergOutputSettings, RouteSettings};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

/// `file_store`'s own default when an input does not set `offset`.
const DEFAULT_POLLER_OFFSET: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    /// `state_dir` (the ledger) and `processed_retention`. `state_dir` MUST
    /// be on a durable volume: it is the only record of what was delivered.
    pub route: RouteSettings,

    /// One entry per bucket, keyed by input name: in practice one, named
    /// [`super::INPUT`]. In environment form:
    /// `ICICLE__MOBILE_HOTSPOT_HISTORY__INPUTS__CHAIN__BUCKET__BUCKET=...`.
    pub inputs: BTreeMap<String, FileStoreInputSettings>,

    /// The table written to. Defaults to `chain.mobile_hotspot_history`.
    #[serde(default)]
    pub table: TableSettings,

    /// The catalog, the spool directory and the commit batching.
    /// `spool_dir` MUST also be on a durable volume: rows live there between
    /// a durable write and the Iceberg commit.
    pub output: IcebergOutputSettings,

    /// What to do when the ledger is empty but the table already holds rows.
    #[serde(default)]
    pub fresh_state: FreshState,
}

/// Which table the route writes to; the configured half of
/// [`MobileHotspotHistoryTable`].
#[derive(Debug, Deserialize, Serialize)]
pub struct TableSettings {
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default = "default_name")]
    pub name: String,
}

impl Default for TableSettings {
    fn default() -> Self {
        Self {
            namespace: default_namespace(),
            name: default_name(),
        }
    }
}

impl From<&TableSettings> for MobileHotspotHistoryTable {
    fn from(settings: &TableSettings) -> Self {
        Self::new(&settings.namespace, &settings.name)
    }
}

fn default_namespace() -> String {
    DEFAULT_NAMESPACE.to_string()
}

fn default_name() -> String {
    DEFAULT_TABLE_NAME.to_string()
}

impl Settings {
    /// Catches the misconfigurations that are silent at runtime and expensive
    /// afterwards.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.inputs.is_empty() {
            anyhow::bail!(
                "mobile_hotspot_history: no inputs configured, the route needs at least one bucket"
            );
        }

        // A done batch that is forgotten and then listed again is delivered
        // again. `processed_retention` must outlive every poller's lookback.
        for (name, input) in &self.inputs {
            let offset = input.offset.unwrap_or(DEFAULT_POLLER_OFFSET);
            if self.route.processed_retention <= offset {
                anyhow::bail!(
                    "mobile_hotspot_history input {name}: processed_retention ({:?}) must be \
                     greater than the poller offset ({offset:?}), or re-listed files are \
                     delivered twice",
                    self.route.processed_retention,
                );
            }
        }

        Ok(())
    }
}
