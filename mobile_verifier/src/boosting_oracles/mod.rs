pub mod assignment;

use crate::{
    geofence::{Geofence, GeofenceValidator},
    Settings,
};
pub use assignment::{Assignment, HexAssignments};
use hextree::disktree::DiskTreeMap;

pub trait BoostedHexAssignments: Send + Sync {
    fn assignments(&self, cell: hextree::Cell) -> anyhow::Result<HexAssignments>;
}

pub struct HexBoostData {
    urbanized: DiskTreeMap,
    usa_geofence: Geofence,
    footfall: DiskTreeMap,
    landtype: DiskTreeMap,
}

pub fn make_hex_boost_data(
    settings: &Settings,
    usa_geofence: Geofence,
) -> anyhow::Result<HexBoostData> {
    let urban_disktree = DiskTreeMap::open(&settings.urbanization_data_set)?;
    let footfall_disktree = DiskTreeMap::open(&settings.footfall_data_set)?;
    let landtype_disktree = DiskTreeMap::open(&settings.landtype_data_set)?;

    let hex_boost_data = HexBoostData {
        urbanized: urban_disktree,
        usa_geofence,
        footfall: footfall_disktree,
        landtype: landtype_disktree,
    };

    Ok(hex_boost_data)
}
impl BoostedHexAssignments for HexBoostData {
    fn assignments(&self, cell: hextree::Cell) -> anyhow::Result<HexAssignments> {
        let footfall = self.footfall_assignment(cell)?;
        let urbanized = self.urbanized_assignment(cell)?;
        let landtype = self.landtype_assignment(cell)?;

        Ok(HexAssignments {
            footfall,
            urbanized,
            landtype,
        })
    }
}

impl HexBoostData {
    fn urbanized_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        if !self.usa_geofence.in_valid_region(&cell) {
            return Ok(Assignment::C);
        }

        match self.urbanized.get(cell)?.is_some() {
            true => Ok(Assignment::A),
            false => Ok(Assignment::B),
        }
    }

    fn footfall_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some((_, vals)) = self.footfall.get(cell)? else {
            return Ok(Assignment::C);
        };

        match vals {
            &[x] if x >= 1 => Ok(Assignment::A),
            &[0] => Ok(Assignment::B),
            other => anyhow::bail!("unexpected footfall disktree data: {cell:?} {other:?}"),
        }
    }

    fn landtype_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some((_, vals)) = self.landtype.get(cell)? else {
            return Ok(Assignment::C);
        };

        anyhow::ensure!(
            vals.len() == 1,
            "unexpected landtype disktree data: {cell:?} {vals:?}"
        );

        let cover = WorldCover::try_from(vals[0])?;
        Ok(cover.into())
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
    type Error = anyhow::Error;
    fn try_from(other: u8) -> anyhow::Result<WorldCover, Self::Error> {
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
            other => anyhow::bail!("unexpected landtype disktree value: {other:?}"),
        };
        Ok(val)
    }
}
