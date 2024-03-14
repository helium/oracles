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
) -> anyhow::Result<HexBoostData<UrbanizationData<DiskTreeMap, Geofence>, FootfallData<DiskTreeMap>>>
{
    let urban_disktree = DiskTreeMap::open(&settings.urbanization_data_set)?;
    let footfall_disktree = DiskTreeMap::open(&settings.footfall_data_set)?;

    let urbanization = UrbanizationData::new(urban_disktree, usa_geofence);
    let footfall_data = FootfallData::new(footfall_disktree);
    let hex_boost_data = HexBoostData::new(urbanization, footfall_data);

    Ok(hex_boost_data)
}

pub enum FootfallCategory {
    /// More than 1 point of interest
    Poi,
    /// (ZERO) points of interest
    PoiNoData,
    /// Cell not found
    NoPoi,
}

pub enum UrbanizationCategory {
    Urban,
    NotUrban,
    OutsideUSA,
}

pub trait HexAssignment: Send + Sync {
    fn urban_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
    fn footfall_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
}

pub struct HexBoostData<Urban, Foot> {
    urbanization: Urban,
    footfall: Foot,
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

impl<Urban, Foot> HexAssignment for HexBoostData<Urban, Foot>
where
    Urban: TryAsCategory<UrbanizationCategory>,
    Foot: TryAsCategory<FootfallCategory>,
{
    fn urban_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        self.urbanization
            .try_as_category(cell)
            .map(|cat| match cat {
                UrbanizationCategory::Urban => Assignment::A,
                UrbanizationCategory::NotUrban => Assignment::B,
                UrbanizationCategory::OutsideUSA => Assignment::C,
            })
    }

    fn footfall_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        self.footfall.try_as_category(cell).map(|cat| match cat {
            FootfallCategory::Poi => Assignment::A,
            FootfallCategory::PoiNoData => Assignment::B,
            FootfallCategory::NoPoi => Assignment::C,
        })
    }
}

pub trait TryAsCategory<T>: Sync + Send {
    fn try_as_category(&self, cell: hextree::Cell) -> anyhow::Result<T>;
}

impl<Urban, Geo> TryAsCategory<UrbanizationCategory> for UrbanizationData<Urban, Geo>
where
    Urban: DiskTreeLike,
    Geo: GeofenceValidator<hextree::Cell>,
{
    fn try_as_category(&self, cell: hextree::Cell) -> anyhow::Result<UrbanizationCategory> {
        if !self.usa_geofence.in_valid_region(&cell) {
            return Ok(UrbanizationCategory::OutsideUSA);
        }

        match self.urbanized.get(cell)?.is_some() {
            true => Ok(UrbanizationCategory::Urban),
            false => Ok(UrbanizationCategory::NotUrban),
        }
    }
}

impl<Foot> TryAsCategory<FootfallCategory> for FootfallData<Foot>
where
    Foot: DiskTreeLike,
{
    fn try_as_category(&self, cell: hextree::Cell) -> anyhow::Result<FootfallCategory> {
        let Some((_, vals)) = self.footfall.get(cell)? else {
            return Ok(FootfallCategory::NoPoi);
        };

        match vals {
            &[x] if x >= 1 => Ok(FootfallCategory::Poi),
            &[0] => Ok(FootfallCategory::PoiNoData),
            other => anyhow::bail!("unexpected disktree data: {cell:?} {other:?}"),
        }
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

// ==============================
// ==== TEST IMPLEMENTATIONS ====
// ==============================

impl DiskTreeLike for std::collections::HashSet<hextree::Cell> {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        match self.contains(&cell) {
            true => Ok(Some((cell, &[]))),
            false => Ok(None),
        }
    }
}

impl TryAsCategory<FootfallCategory> for HashMap<hextree::Cell, bool> {
    fn try_as_category(&self, cell: hextree::Cell) -> anyhow::Result<FootfallCategory> {
        match self.get(&cell) {
            Some(true) => Ok(FootfallCategory::Poi),
            Some(false) => Ok(FootfallCategory::PoiNoData),
            None => Ok(FootfallCategory::NoPoi),
        }
    }
}
