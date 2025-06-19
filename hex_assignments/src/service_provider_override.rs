use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, HexAssignment};

#[derive(Default)]
pub struct ServiceProviderOverride {
    pub service_provider_override: Option<DiskTreeMap>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl ServiceProviderOverride {
    pub fn new(service_provider_override: Option<DiskTreeMap>) -> Self {
        Self {
            service_provider_override,
            timestamp: None,
        }
    }
}

impl HexAssignment for ServiceProviderOverride {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref service_provider_override) = self.service_provider_override else {
            anyhow::bail!("No service provider override hex data set has been loaded");
        };
        match service_provider_override.contains(cell) {
            Ok(true) => Ok(Assignment::A),
            _ => Ok(Assignment::C),
        }
    }
}
