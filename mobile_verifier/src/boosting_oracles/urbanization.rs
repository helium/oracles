use std::path::Path;

use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, DataSet, DataSetType, DiskTreeLike};
use crate::geofence::GeofenceValidator;

pub struct Urbanization<DT, GF> {
    urbanized: Option<DT>,
    timestamp: Option<DateTime<Utc>>,
    usa_geofence: GF,
}

impl<DT, GF> Urbanization<DT, GF> {
    pub fn new(usa_geofence: GF) -> Self {
        Self {
            urbanized: None,
            timestamp: None,
            usa_geofence,
        }
    }

    pub fn new_mock(urbanized: DT, usa_geofence: GF) -> Self {
        Self {
            urbanized: Some(urbanized),
            usa_geofence,
            timestamp: None,
        }
    }
}

impl<DT, GF> Urbanization<DT, GF>
where
    DT: DiskTreeLike,
    GF: GeofenceValidator<u64>,
{
    pub fn is_ready(&self) -> bool {
        self.urbanized.is_some()
    }

    fn is_urbanized(&self, location: u64) -> anyhow::Result<bool> {
        let Some(ref urbanized) = self.urbanized else {
            anyhow::bail!("No urbanization data set file has been loaded");
        };
        let cell = hextree::Cell::from_raw(location)?;
        let result = urbanized.get(cell)?;
        Ok(result.is_some())
    }

    pub fn hex_assignment(&self, hex: u64) -> anyhow::Result<Assignment> {
        let assignment = if self.usa_geofence.in_valid_region(&hex) {
            if self.is_urbanized(hex)? {
                Assignment::A
            } else {
                Assignment::B
            }
        } else {
            Assignment::C
        };
        Ok(assignment)
    }
}

impl<GF> DataSet for Urbanization<DiskTreeMap, GF>
where
    GF: GeofenceValidator<u64>,
{
    const TYPE: DataSetType = DataSetType::Urbanization;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.urbanized = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn assign(&self, hex: u64) -> anyhow::Result<Assignment> {
        self.hex_assignment(hex)
    }
}
