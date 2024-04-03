use std::path::Path;

use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, DataSet, DataSetType, DiskTreeLike, HexAssignment};
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

impl<GF> DataSet for Urbanization<DiskTreeMap, GF>
where
    GF: GeofenceValidator<hextree::Cell> + Send + Sync + 'static,
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

    fn is_ready(&self) -> bool {
        self.urbanized.is_some()
    }
}

impl<Urban, Geo> HexAssignment for Urbanization<Urban, Geo>
where
    Urban: DiskTreeLike + Send + Sync + 'static,
    Geo: GeofenceValidator<hextree::Cell> + Send + Sync + 'static,
{
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref urbanized) = self.urbanized else {
            anyhow::bail!("No urbanization data set has been loaded");
        };

        if !self.usa_geofence.in_valid_region(&cell) {
            Ok(Assignment::C)
        } else if urbanized.get(cell)?.is_some() {
            Ok(Assignment::A)
        } else {
            Ok(Assignment::B)
        }
    }
}
