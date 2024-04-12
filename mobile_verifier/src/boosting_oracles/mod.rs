pub mod assignment;

use std::collections::HashMap;

use crate::{
    geofence::{Geofence, GeofenceValidator},
    Settings,
};
pub use assignment::{Assignment, HexAssignments};
use hextree::disktree::DiskTreeMap;

pub fn make_hex_boost_data(
    settings: &Settings,
    usa_geofence: Geofence,
) -> anyhow::Result<impl BoostedHexAssignments> {
    let urban_disktree = DiskTreeMap::open(&settings.urbanization_data_set)?;
    let footfall_disktree = DiskTreeMap::open(&settings.footfall_data_set)?;
    let landtype_disktree = DiskTreeMap::open(&settings.landtype_data_set)?;

    let urbanization = UrbanizationData::new(urban_disktree, usa_geofence);
    let footfall_data = FootfallData::new(footfall_disktree);
    let landtype_data = LandTypeData::new(landtype_disktree);

    let hex_boost_data = HexBoostData::new(urbanization, footfall_data, landtype_data);

    Ok(hex_boost_data)
}

pub trait BoostedHexAssignments: Send + Sync {
    fn assignments(&self, cell: hextree::Cell) -> anyhow::Result<HexAssignments>;
}

trait HexAssignment: Send + Sync {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
}

impl<U, F, L> BoostedHexAssignments for HexBoostData<U, F, L>
where
    U: HexAssignment,
    F: HexAssignment,
    L: HexAssignment,
{
    fn assignments(&self, cell: hextree::Cell) -> anyhow::Result<HexAssignments> {
        let footfall = self.footfall.assignment(cell)?;
        let urbanized = self.urbanization.assignment(cell)?;
        let landtype = self.landtype.assignment(cell)?;

        Ok(HexAssignments {
            footfall,
            urbanized,
            landtype,
        })
    }
}

pub struct HexBoostData<Urban, Foot, Land> {
    pub urbanization: Urban,
    pub footfall: Foot,
    pub landtype: Land,
}

pub struct UrbanizationData<Urban, Geo> {
    urbanized: Urban,
    usa_geofence: Geo,
}

pub struct FootfallData<Foot> {
    footfall: Foot,
}

pub struct LandTypeData<Land> {
    landtype: Land,
}

impl<Urban, Foot, Land> HexBoostData<Urban, Foot, Land> {
    pub fn new(urbanization: Urban, footfall: Foot, landtype: Land) -> Self {
        Self {
            urbanization,
            footfall,
            landtype,
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

impl<Land> LandTypeData<Land> {
    pub fn new(landtype: Land) -> Self {
        Self { landtype }
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
            other => anyhow::bail!("unexpected footfall disktree data: {cell:?} {other:?}"),
        }
    }
}

impl<Land> HexAssignment for LandTypeData<Land>
where
    Land: DiskTreeLike,
{
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some((_, vals)) = self.landtype.get(cell)? else {
            return Ok(Assignment::C);
        };

        let cover = match vals {
            &[x] => WorldCover::try_from(x).map_err(|_| {
                anyhow::anyhow!("unexpected landtype disktree value: {cell:?} {x:?}")
            })?,
            other => anyhow::bail!("unexpected landtype disktree data: {cell:?} {other:?}"),
        };

        Ok(Assignment::from(cover))
    }
}

impl HexAssignment for LandTypeData<Assignment> {
    fn assignment(&self, _cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(self.landtype)
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

impl HexAssignment for HashMap<hextree::Cell, Assignment> {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        match self.get(&cell) {
            Some(val) => Ok(*val),
            None => anyhow::bail!("{cell:?} not found"),
        }
    }
}

impl From<WorldCover> for Assignment {
    fn from(value: WorldCover) -> Self {
        match value {
            WorldCover::Built => Assignment::A,
            //
            WorldCover::Tree => Assignment::B,
            WorldCover::Shrub => Assignment::B,
            WorldCover::Grass => Assignment::B,
            //
            WorldCover::Bare => Assignment::C,
            WorldCover::Crop => Assignment::C,
            WorldCover::Frozen => Assignment::C,
            WorldCover::Water => Assignment::C,
            WorldCover::Wet => Assignment::C,
            WorldCover::Mangrove => Assignment::C,
            WorldCover::Moss => Assignment::C,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorldCover {
    Tree = 10,
    Shrub = 20,
    Grass = 30,
    Crop = 40,
    Built = 50,
    Bare = 60,
    Frozen = 70,
    Water = 80,
    Wet = 90,
    Mangrove = 95,
    Moss = 100,
}

impl std::fmt::Display for WorldCover {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl WorldCover {
    #[allow(dead_code)]
    pub(crate) fn to_str(self) -> &'static str {
        match self {
            WorldCover::Tree => "TreeCover",
            WorldCover::Shrub => "Shrubland",
            WorldCover::Grass => "Grassland",
            WorldCover::Crop => "Cropland",
            WorldCover::Built => "BuiltUp",
            WorldCover::Bare => "BareOrSparseVeg",
            WorldCover::Frozen => "SnowAndIce",
            WorldCover::Water => "Water",
            WorldCover::Wet => "HerbaceousWetland",
            WorldCover::Mangrove => "Mangroves",
            WorldCover::Moss => "MossAndLichen",
        }
    }
}

impl TryFrom<u8> for WorldCover {
    type Error = ();
    fn try_from(other: u8) -> Result<WorldCover, ()> {
        let val = match other {
            10 => WorldCover::Tree,
            20 => WorldCover::Shrub,
            30 => WorldCover::Grass,
            40 => WorldCover::Crop,
            50 => WorldCover::Built,
            60 => WorldCover::Bare,
            70 => WorldCover::Frozen,
            80 => WorldCover::Water,
            90 => WorldCover::Wet,
            95 => WorldCover::Mangrove,
            100 => WorldCover::Moss,
            _ => return Err(()),
        };
        Ok(val)
    }
}
