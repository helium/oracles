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

        let cover = Landtype::try_from(vals[0])?;
        Ok(cover.into())
    }
}

impl From<Landtype> for Assignment {
    fn from(value: Landtype) -> Self {
        match value {
            Landtype::Built => Assignment::A,
            //
            Landtype::Tree => Assignment::B,
            Landtype::Shrub => Assignment::B,
            Landtype::Grass => Assignment::B,
            //
            Landtype::Bare => Assignment::C,
            Landtype::Crop => Assignment::C,
            Landtype::Frozen => Assignment::C,
            Landtype::Water => Assignment::C,
            Landtype::Wet => Assignment::C,
            Landtype::Mangrove => Assignment::C,
            Landtype::Moss => Assignment::C,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Landtype {
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

impl std::fmt::Display for Landtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl Landtype {
    pub(crate) fn to_str(self) -> &'static str {
        match self {
            Landtype::Tree => "TreeCover",
            Landtype::Shrub => "Shrubland",
            Landtype::Grass => "Grassland",
            Landtype::Crop => "Cropland",
            Landtype::Built => "BuiltUp",
            Landtype::Bare => "BareOrSparseVeg",
            Landtype::Frozen => "SnowAndIce",
            Landtype::Water => "Water",
            Landtype::Wet => "HerbaceousWetland",
            Landtype::Mangrove => "Mangroves",
            Landtype::Moss => "MossAndLichen",
        }
    }
}

impl TryFrom<u8> for Landtype {
    type Error = anyhow::Error;
    fn try_from(other: u8) -> anyhow::Result<Landtype, Self::Error> {
        let val = match other {
            10 => Landtype::Tree,
            20 => Landtype::Shrub,
            30 => Landtype::Grass,
            40 => Landtype::Crop,
            50 => Landtype::Built,
            60 => Landtype::Bare,
            70 => Landtype::Frozen,
            80 => Landtype::Water,
            90 => Landtype::Wet,
            95 => Landtype::Mangrove,
            100 => Landtype::Moss,
            other => anyhow::bail!("unexpected landtype disktree value: {other:?}"),
        };
        Ok(val)
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use hextree::{HexTreeMap, HexTreeSet};

    use super::*;

    #[test]
    fn test_hex_boost_data() -> anyhow::Result<()> {
        // This test will break if any of the logic deriving Assignments from
        // the underlying DiskTreeMap's changes.

        let unknown_cell = hextree::Cell::from_raw(0x8c2681a3064d9ff)?;

        // Types of Cells
        // yellow - POI ≥ 1 Urbanized
        let poi_built_urbanized = hextree::Cell::from_raw(0x8c2681a3064dbff)?;
        let poi_grass_urbanized = hextree::Cell::from_raw(0x8c2681a3064ddff)?;
        let poi_water_urbanized = hextree::Cell::from_raw(0x8c2681a3064e1ff)?;
        // orange - POI ≥ 1 Not Urbanized
        let poi_built_not_urbanized = hextree::Cell::from_raw(0x8c2681a3064e3ff)?;
        let poi_grass_not_urbanized = hextree::Cell::from_raw(0x8c2681a3064e5ff)?;
        let poi_water_not_urbanized = hextree::Cell::from_raw(0x8c2681a3064e7ff)?;
        // light green - Point of Interest Urbanized
        let poi_no_data_built_urbanized = hextree::Cell::from_raw(0x8c2681a3064e9ff)?;
        let poi_no_data_grass_urbanized = hextree::Cell::from_raw(0x8c2681a3064ebff)?;
        let poi_no_data_water_urbanized = hextree::Cell::from_raw(0x8c2681a3064edff)?;
        // dark green - Point of Interest Not Urbanized
        let poi_no_data_built_not_urbanized = hextree::Cell::from_raw(0x8c2681a306501ff)?;
        let poi_no_data_grass_not_urbanized = hextree::Cell::from_raw(0x8c2681a306503ff)?;
        let poi_no_data_water_not_urbanized = hextree::Cell::from_raw(0x8c2681a306505ff)?;
        // light blue - No POI Urbanized
        let no_poi_built_urbanized = hextree::Cell::from_raw(0x8c2681a306507ff)?;
        let no_poi_grass_urbanized = hextree::Cell::from_raw(0x8c2681a306509ff)?;
        let no_poi_water_urbanized = hextree::Cell::from_raw(0x8c2681a30650bff)?;
        // dark blue - No POI Not Urbanized
        let no_poi_built_not_urbanized = hextree::Cell::from_raw(0x8c2681a30650dff)?;
        let no_poi_grass_not_urbanized = hextree::Cell::from_raw(0x8c2681a306511ff)?;
        let no_poi_water_not_urbanized = hextree::Cell::from_raw(0x8c2681a306513ff)?;
        // gray - Outside of USA
        let poi_built_outside_us = hextree::Cell::from_raw(0x8c2681a306515ff)?;
        let poi_grass_outside_us = hextree::Cell::from_raw(0x8c2681a306517ff)?;
        let poi_water_outside_us = hextree::Cell::from_raw(0x8c2681a306519ff)?;
        let poi_no_data_built_outside_us = hextree::Cell::from_raw(0x8c2681a30651bff)?;
        let poi_no_data_grass_outside_us = hextree::Cell::from_raw(0x8c2681a30651dff)?;
        let poi_no_data_water_outside_us = hextree::Cell::from_raw(0x8c2681a306521ff)?;
        let no_poi_built_outside_us = hextree::Cell::from_raw(0x8c2681a306523ff)?;
        let no_poi_grass_outside_us = hextree::Cell::from_raw(0x8c2681a306525ff)?;
        let no_poi_water_outside_us = hextree::Cell::from_raw(0x8c2681a306527ff)?;

        let all_cells = vec![
            poi_built_urbanized,
            poi_grass_urbanized,
            poi_water_urbanized,
            poi_built_not_urbanized,
            poi_grass_not_urbanized,
            poi_water_not_urbanized,
            poi_no_data_built_urbanized,
            poi_no_data_grass_urbanized,
            poi_no_data_water_urbanized,
            poi_no_data_built_not_urbanized,
            poi_no_data_grass_not_urbanized,
            poi_no_data_water_not_urbanized,
            no_poi_built_urbanized,
            no_poi_grass_urbanized,
            no_poi_water_urbanized,
            no_poi_built_not_urbanized,
            no_poi_grass_not_urbanized,
            no_poi_water_not_urbanized,
            poi_built_outside_us,
            poi_grass_outside_us,
            poi_water_outside_us,
            poi_no_data_built_outside_us,
            poi_no_data_grass_outside_us,
            poi_no_data_water_outside_us,
            no_poi_built_outside_us,
            no_poi_grass_outside_us,
            no_poi_water_outside_us,
        ];
        for cell in all_cells {
            println!("Cell: {cell:?}");
        }

        // Footfall Data
        // POI         - footfalls > 1 for a POI across hexes
        // POI No Data - No footfalls for a POI across any hexes
        // NO POI      - Does not exist
        let mut footfall = HexTreeMap::<u8>::new();
        footfall.insert(poi_built_urbanized, 42);
        footfall.insert(poi_grass_urbanized, 42);
        footfall.insert(poi_water_urbanized, 42);
        footfall.insert(poi_built_not_urbanized, 42);
        footfall.insert(poi_grass_not_urbanized, 42);
        footfall.insert(poi_water_not_urbanized, 42);
        footfall.insert(poi_no_data_built_urbanized, 0);
        footfall.insert(poi_no_data_grass_urbanized, 0);
        footfall.insert(poi_no_data_water_urbanized, 0);
        footfall.insert(poi_no_data_built_not_urbanized, 0);
        footfall.insert(poi_no_data_grass_not_urbanized, 0);
        footfall.insert(poi_no_data_water_not_urbanized, 0);
        footfall.insert(poi_built_outside_us, 42);
        footfall.insert(poi_grass_outside_us, 42);
        footfall.insert(poi_water_outside_us, 42);
        footfall.insert(poi_no_data_built_outside_us, 0);
        footfall.insert(poi_no_data_grass_outside_us, 0);
        footfall.insert(poi_no_data_water_outside_us, 0);

        // Landtype Data
        // Map to enum values for Landtype
        // An unknown cell is considered Assignment::C
        let mut landtype = HexTreeMap::<u8>::new();
        landtype.insert(poi_built_urbanized, 50);
        landtype.insert(poi_grass_urbanized, 30);
        landtype.insert(poi_water_urbanized, 80);
        landtype.insert(poi_built_not_urbanized, 50);
        landtype.insert(poi_grass_not_urbanized, 30);
        landtype.insert(poi_water_not_urbanized, 80);
        landtype.insert(poi_no_data_built_urbanized, 50);
        landtype.insert(poi_no_data_grass_urbanized, 30);
        landtype.insert(poi_no_data_water_urbanized, 80);
        landtype.insert(poi_no_data_built_not_urbanized, 50);
        landtype.insert(poi_no_data_grass_not_urbanized, 30);
        landtype.insert(poi_no_data_water_not_urbanized, 80);
        landtype.insert(no_poi_built_urbanized, 50);
        landtype.insert(no_poi_grass_urbanized, 30);
        landtype.insert(no_poi_water_urbanized, 80);
        landtype.insert(no_poi_built_not_urbanized, 50);
        landtype.insert(no_poi_grass_not_urbanized, 30);
        landtype.insert(no_poi_water_not_urbanized, 80);
        landtype.insert(poi_built_outside_us, 50);
        landtype.insert(poi_grass_outside_us, 30);
        landtype.insert(poi_water_outside_us, 80);
        landtype.insert(poi_no_data_built_outside_us, 50);
        landtype.insert(poi_no_data_grass_outside_us, 30);
        landtype.insert(poi_no_data_water_outside_us, 80);
        landtype.insert(no_poi_built_outside_us, 50);
        landtype.insert(no_poi_grass_outside_us, 30);
        landtype.insert(no_poi_water_outside_us, 80);

        // Urbanized data
        // Urban     - something in the map, and in the geofence
        // Not Urban - nothing in the map, but in the geofence
        // Outside   - not in the geofence, urbanized hex never considered
        let mut urbanized = HexTreeMap::<u8>::new();
        urbanized.insert(poi_built_urbanized, 0);
        urbanized.insert(poi_grass_urbanized, 0);
        urbanized.insert(poi_water_urbanized, 0);
        urbanized.insert(poi_no_data_built_urbanized, 0);
        urbanized.insert(poi_no_data_grass_urbanized, 0);
        urbanized.insert(poi_no_data_water_urbanized, 0);
        urbanized.insert(no_poi_built_urbanized, 0);
        urbanized.insert(no_poi_grass_urbanized, 0);
        urbanized.insert(no_poi_water_urbanized, 0);

        let inside_usa = [
            poi_built_urbanized,
            poi_grass_urbanized,
            poi_water_urbanized,
            poi_built_not_urbanized,
            poi_grass_not_urbanized,
            poi_water_not_urbanized,
            poi_no_data_built_urbanized,
            poi_no_data_grass_urbanized,
            poi_no_data_water_urbanized,
            poi_no_data_built_not_urbanized,
            poi_no_data_grass_not_urbanized,
            poi_no_data_water_not_urbanized,
            no_poi_built_urbanized,
            no_poi_grass_urbanized,
            no_poi_water_urbanized,
            no_poi_built_not_urbanized,
            no_poi_grass_not_urbanized,
            no_poi_water_not_urbanized,
        ];
        let geofence_set: HexTreeSet = inside_usa.iter().collect();
        let usa_geofence = Geofence::new(geofence_set, h3o::Resolution::Twelve);

        // These vectors are a standin for the file system
        let mut urbanized_buf = vec![];
        let mut footfall_buff = vec![];
        let mut landtype_buf = vec![];

        // Turn the HexTrees into DiskTrees
        urbanized.to_disktree(Cursor::new(&mut urbanized_buf), |w, v| w.write_all(&[*v]))?;
        footfall.to_disktree(Cursor::new(&mut footfall_buff), |w, v| w.write_all(&[*v]))?;
        landtype.to_disktree(Cursor::new(&mut landtype_buf), |w, v| w.write_all(&[*v]))?;

        let urbanized = DiskTreeMap::with_buf(urbanized_buf)?;
        let footfall = DiskTreeMap::with_buf(footfall_buff)?;
        let landtype = DiskTreeMap::with_buf(landtype_buf)?;

        // Let the testing commence
        let data = HexBoostData {
            urbanized,
            usa_geofence,
            footfall,
            landtype,
        };

        // NOTE(mj): formatting ignored to make it easier to see the expected change in assignments.
        // NOTE(mj): The semicolon at the end of the block is there to keep rust from
        // complaining about attributes on expression being experimental.
        #[rustfmt::skip]
        {
            use Assignment::*;
            // yellow
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: A }, data.assignments(poi_built_urbanized)?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: A }, data.assignments(poi_grass_urbanized)?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: A }, data.assignments(poi_water_urbanized)?);
            // orange
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: B }, data.assignments(poi_built_not_urbanized)?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: B }, data.assignments(poi_grass_not_urbanized)?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: B }, data.assignments(poi_water_not_urbanized)?);
            // light green
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: A }, data.assignments(poi_no_data_built_urbanized)?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: A }, data.assignments(poi_no_data_grass_urbanized)?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: A }, data.assignments(poi_no_data_water_urbanized)?);
            // green
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: B }, data.assignments(poi_no_data_built_not_urbanized)?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: B }, data.assignments(poi_no_data_grass_not_urbanized)?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: B }, data.assignments(poi_no_data_water_not_urbanized)?);
            // light blue
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: A }, data.assignments(no_poi_built_urbanized)?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: A }, data.assignments(no_poi_grass_urbanized)?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: A }, data.assignments(no_poi_water_urbanized)?);
            // dark blue
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: B }, data.assignments(no_poi_built_not_urbanized)?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: B }, data.assignments(no_poi_grass_not_urbanized)?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: B }, data.assignments(no_poi_water_not_urbanized)?);
            // gray
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: C }, data.assignments(poi_built_outside_us)?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: C }, data.assignments(poi_grass_outside_us)?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: C }, data.assignments(poi_water_outside_us)?);
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: C }, data.assignments(poi_no_data_built_outside_us)?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: C }, data.assignments(poi_no_data_grass_outside_us)?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: C }, data.assignments(poi_no_data_water_outside_us)?);
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: C }, data.assignments(no_poi_built_outside_us)?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: C }, data.assignments(no_poi_grass_outside_us)?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: C }, data.assignments(no_poi_water_outside_us)?);
            // never inserted
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: C }, data.assignments(unknown_cell)?);
        };

        Ok(())
    }
}
