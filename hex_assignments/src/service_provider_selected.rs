use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, HexAssignment};

pub struct ServiceProviderSelected {
    pub service_provider_selected: Option<DiskTreeMap>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl ServiceProviderSelected {
    pub fn new(service_provider_selected: Option<DiskTreeMap>) -> Self {
        Self {
            service_provider_selected,
            timestamp: None,
        }
    }
}

impl Default for ServiceProviderSelected {
    fn default() -> Self {
        Self::new(None)
    }
}

impl HexAssignment for ServiceProviderSelected {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref service_provider_selected) = self.service_provider_selected else {
            anyhow::bail!("No service provider selected hex data set has been loaded");
        };
        match service_provider_selected.contains(cell) {
            Ok(true) => Ok(Assignment::A),
            Ok(false) => Ok(Assignment::C),
            _ => Ok(Assignment::C),
        }
    }
}
