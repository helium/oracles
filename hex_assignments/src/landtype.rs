use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, HexAssignment};

pub struct Landtype {
    pub landtype: Option<DiskTreeMap>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl Landtype {
    pub fn new(landtype: Option<DiskTreeMap>) -> Self {
        Self {
            landtype,
            timestamp: None,
        }
    }
}

impl Default for Landtype {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandtypeValue {
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

impl std::fmt::Display for LandtypeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl LandtypeValue {
    pub(crate) fn to_str(self) -> &'static str {
        match self {
            LandtypeValue::Tree => "TreeCover",
            LandtypeValue::Shrub => "Shrubland",
            LandtypeValue::Grass => "Grassland",
            LandtypeValue::Crop => "Cropland",
            LandtypeValue::Built => "BuiltUp",
            LandtypeValue::Bare => "BareOrSparseVeg",
            LandtypeValue::Frozen => "SnowAndIce",
            LandtypeValue::Water => "Water",
            LandtypeValue::Wet => "HerbaceousWetland",
            LandtypeValue::Mangrove => "Mangroves",
            LandtypeValue::Moss => "MossAndLichen",
        }
    }
}

impl TryFrom<u8> for LandtypeValue {
    type Error = anyhow::Error;
    fn try_from(other: u8) -> anyhow::Result<LandtypeValue, Self::Error> {
        let val = match other {
            10 => LandtypeValue::Tree,
            20 => LandtypeValue::Shrub,
            30 => LandtypeValue::Grass,
            40 => LandtypeValue::Crop,
            50 => LandtypeValue::Built,
            60 => LandtypeValue::Bare,
            70 => LandtypeValue::Frozen,
            80 => LandtypeValue::Water,
            90 => LandtypeValue::Wet,
            95 => LandtypeValue::Mangrove,
            100 => LandtypeValue::Moss,
            other => anyhow::bail!("unexpected landtype disktree value: {other:?}"),
        };
        Ok(val)
    }
}

impl From<LandtypeValue> for Assignment {
    fn from(value: LandtypeValue) -> Self {
        match value {
            LandtypeValue::Built => Assignment::A,
            //
            LandtypeValue::Tree => Assignment::B,
            LandtypeValue::Shrub => Assignment::B,
            LandtypeValue::Grass => Assignment::B,
            //
            LandtypeValue::Bare => Assignment::C,
            LandtypeValue::Crop => Assignment::C,
            LandtypeValue::Frozen => Assignment::C,
            LandtypeValue::Water => Assignment::C,
            LandtypeValue::Wet => Assignment::C,
            LandtypeValue::Mangrove => Assignment::C,
            LandtypeValue::Moss => Assignment::C,
        }
    }
}

impl HexAssignment for Landtype {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref landtype) = self.landtype else {
            anyhow::bail!("No landtype data set has been loaded");
        };

        let Some((_, vals)) = landtype.get(cell)? else {
            return Ok(Assignment::C);
        };

        anyhow::ensure!(
            vals.len() == 1,
            "unexpected landtype disktree data: {cell:?} {vals:?}"
        );

        let cover = LandtypeValue::try_from(vals[0])?;
        Ok(cover.into())
    }
}
