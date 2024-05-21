use std::path::Path;

use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, DataSet, DataSetType, HexAssignment};

pub struct Landtype {
    landtype: Option<DiskTreeMap>,
    timestamp: Option<DateTime<Utc>>,
}

impl Landtype {
    pub fn new() -> Self {
        Self {
            landtype: None,
            timestamp: None,
        }
    }

    pub fn new_mock(landtype: DiskTreeMap) -> Self {
        Self {
            landtype: Some(landtype),
            timestamp: None,
        }
    }
}

impl Default for Landtype {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl DataSet for Landtype {
    const TYPE: DataSetType = DataSetType::Landtype;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.landtype = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.landtype.is_some()
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
