use crate::{
    hex::{HexDensityMap, SharedHexDensityMap},
    Settings,
};
use chrono::Duration;

pub struct Server {
    pub hex_density_map: SharedHexDensityMap,
    pub trigger_interval: Duration,
}

impl Server {
    pub async fn from_settings(settings: Settings) -> Self {
        Self {
            hex_density_map: SharedHexDensityMap::new(),
            trigger_interval: Duration::seconds(settings.trigger),
        }
    }

    pub fn hex_density_map(&self) -> impl HexDensityMap {
        self.hex_density_map.clone()
    }
}
