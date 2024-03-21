pub mod assignment;

use std::collections::HashMap;

use crate::{
    geofence::{Geofence, GeofenceValidator},
    Settings,
};
pub use assignment::Assignment;
use hextree::disktree::DiskTreeMap;

pub fn make_hex_boost_data(
    settings: &Settings,
    usa_geofence: Geofence,
) -> anyhow::Result<HexBoostData<impl HexAssignment, impl HexAssignment>> {
    let urban_disktree = DiskTreeMap::open(&settings.urbanization_data_set)?;
    let footfall_disktree = DiskTreeMap::open(&settings.footfall_data_set)?;

    let urbanization = UrbanizationData::new(urban_disktree, usa_geofence);
    let footfall_data = FootfallData::new(footfall_disktree);
    let hex_boost_data = HexBoostData::new(urbanization, footfall_data);

    Ok(hex_boost_data)
}

pub trait HexAssignment: Send + Sync {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
}

pub struct HexBoostData<Urban, Foot> {
    pub urbanization: Urban,
    pub footfall: Foot,
}

pub struct UrbanizationData<Urban, Geo> {
    urbanized: Urban,
    usa_geofence: Geo,
}

pub struct FootfallData<Foot> {
    footfall: Foot,
}

impl<Urban, Foot> HexBoostData<Urban, Foot> {
    pub fn new(urbanization: Urban, footfall: Foot) -> Self {
        Self {
            urbanization,
            footfall,
        }
    }
}

impl<Urban, Geo> UrbanizationData<Urban, Geo> {
    pub fn new(urbanized: Urban, usa_geofence: Geo) -> Self {
        Self {
            urbanized,
            usa_geofence,
        }
    }
}

impl<Foot> FootfallData<Foot> {
    pub fn new(footfall: Foot) -> Self {
        Self { footfall }
    }
}

trait DiskTreeLike: Send + Sync {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>>;
}

impl DiskTreeLike for DiskTreeMap {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        self.get(cell)
    }
}

impl DiskTreeLike for std::collections::HashSet<hextree::Cell> {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        match self.contains(&cell) {
            true => Ok(Some((cell, &[]))),
            false => Ok(None),
        }
    }
}

impl<Urban, Geo> HexAssignment for UrbanizationData<Urban, Geo>
where
    Urban: DiskTreeLike,
    Geo: GeofenceValidator<hextree::Cell>,
{
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        if !self.usa_geofence.in_valid_region(&cell) {
            return Ok(Assignment::C);
        }

        match self.urbanized.get(cell)?.is_some() {
            true => Ok(Assignment::A),
            false => Ok(Assignment::B),
        }
    }
}

impl<Foot> HexAssignment for FootfallData<Foot>
where
    Foot: DiskTreeLike,
{
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some((_, vals)) = self.footfall.get(cell)? else {
            return Ok(Assignment::C);
        };

        match vals {
            &[x] if x >= 1 => Ok(Assignment::A),
            &[0] => Ok(Assignment::B),
            other => anyhow::bail!("unexpected disktree data: {cell:?} {other:?}"),
        }
    }
}

impl HexAssignment for Assignment {
    fn assignment(&self, _cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(*self)
    }
}

impl HexAssignment for HashMap<hextree::Cell, bool> {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let assignment = match self.get(&cell) {
            Some(true) => Assignment::A,
            Some(false) => Assignment::B,
            None => Assignment::C,
        };
        Ok(assignment)
    }
}
