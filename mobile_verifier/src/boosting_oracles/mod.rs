pub mod assignment;
pub mod data_sets;
pub mod footfall;
pub mod landtype;
pub mod urbanization;

use std::collections::HashMap;
use std::sync::Arc;

use crate::boosting_oracles::assignment::HexAssignments;
pub use assignment::Assignment;
pub use data_sets::*;

use hextree::disktree::DiskTreeMap;
use tokio::sync::Mutex;
pub use urbanization::Urbanization;

pub trait HexAssignment: Send + Sync + 'static {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
}

impl HexAssignment for HashMap<hextree::Cell, Assignment> {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(*self.get(&cell).unwrap())
    }
}

impl HexAssignment for Assignment {
    fn assignment(&self, _cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(*self)
    }
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct HexBoostData<Foot, Land, Urban> {
    #[builder(setter(custom))]
    pub footfall: Arc<Mutex<Foot>>,
    #[builder(setter(custom))]
    pub landtype: Arc<Mutex<Land>>,
    #[builder(setter(custom))]
    pub urbanization: Arc<Mutex<Urban>>,
}
impl<F, L, U> HexBoostData<F, L, U> {
    pub fn builder() -> HexBoostDataBuilder<F, L, U> {
        HexBoostDataBuilder::default()
    }
}

impl<F, L, U> Clone for HexBoostData<F, L, U> {
    fn clone(&self) -> Self {
        Self {
            footfall: self.footfall.clone(),
            landtype: self.landtype.clone(),
            urbanization: self.urbanization.clone(),
        }
    }
}

impl<Foot, Land, Urban> HexBoostDataBuilder<Foot, Land, Urban> {
    pub fn footfall(mut self, foot: Foot) -> Self {
        self.footfall = Some(Arc::new(Mutex::new(foot)));
        self
    }

    pub fn landtype(mut self, land: Land) -> Self {
        self.landtype = Some(Arc::new(Mutex::new(land)));
        self
    }

    pub fn urbanization(mut self, urban: Urban) -> Self {
        self.urbanization = Some(Arc::new(Mutex::new(urban)));
        self
    }
}

impl<Foot, Land, Urban> HexBoostData<Foot, Land, Urban>
where
    Foot: DataSet,
    Land: DataSet,
    Urban: DataSet,
{
    pub async fn is_ready(&self) -> bool {
        self.urbanization.lock().await.is_ready()
            && self.footfall.lock().await.is_ready()
            && self.landtype.lock().await.is_ready()
    }
}

impl<Foot, Land, Urban> HexBoostData<Foot, Land, Urban>
where
    Foot: HexAssignment,
    Land: HexAssignment,
    Urban: HexAssignment,
{
    pub async fn assignments(&self, cell: hextree::Cell) -> anyhow::Result<HexAssignments> {
        let footfall = self.footfall.lock().await;
        let landtype = self.landtype.lock().await;
        let urbanization = self.urbanization.lock().await;
        HexAssignments::from_data_sets(cell, &*footfall, &*landtype, &*urbanization)
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
        Ok(self.contains(&cell).then_some((cell, &[])))
    }
}

pub struct MockDiskTree;

impl DiskTreeLike for MockDiskTree {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(Some((cell, &[])))
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use hextree::{HexTreeMap, HexTreeSet};

    use crate::geofence::Geofence;

    use self::{footfall::Footfall, landtype::Landtype};

    use super::*;

    #[tokio::test]
    async fn test_hex_boost_data() -> anyhow::Result<()> {
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

        let footfall = Footfall::new_mock(DiskTreeMap::with_buf(footfall_buff)?);
        let landtype = Landtype::new_mock(DiskTreeMap::with_buf(landtype_buf)?);
        let urbanization =
            Urbanization::new_mock(DiskTreeMap::with_buf(urbanized_buf)?, usa_geofence);

        // Let the testing commence
        let data = HexBoostData::builder()
            .footfall(footfall)
            .landtype(landtype)
            .urbanization(urbanization)
            .build()?;

        // NOTE(mj): formatting ignored to make it easier to see the expected change in assignments.
        // NOTE(mj): The semicolon at the end of the block is there to keep rust from
        // complaining about attributes on expression being experimental.
        #[rustfmt::skip]
        {
            use Assignment::*;
            // yellow
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: A }, data.assignments(poi_built_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: A }, data.assignments(poi_grass_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: A }, data.assignments(poi_water_urbanized).await?);
            // orange
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: B }, data.assignments(poi_built_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: B }, data.assignments(poi_grass_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: B }, data.assignments(poi_water_not_urbanized).await?);
            // light green
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: A }, data.assignments(poi_no_data_built_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: A }, data.assignments(poi_no_data_grass_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: A }, data.assignments(poi_no_data_water_urbanized).await?);
            // green
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: B }, data.assignments(poi_no_data_built_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: B }, data.assignments(poi_no_data_grass_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: B }, data.assignments(poi_no_data_water_not_urbanized).await?);
            // light blue
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: A }, data.assignments(no_poi_built_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: A }, data.assignments(no_poi_grass_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: A }, data.assignments(no_poi_water_urbanized).await?);
            // dark blue
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: B }, data.assignments(no_poi_built_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: B }, data.assignments(no_poi_grass_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: B }, data.assignments(no_poi_water_not_urbanized).await?);
            // gray
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: C }, data.assignments(poi_built_outside_us).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: C }, data.assignments(poi_grass_outside_us).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: C }, data.assignments(poi_water_outside_us).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: C }, data.assignments(poi_no_data_built_outside_us).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: C }, data.assignments(poi_no_data_grass_outside_us).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: C }, data.assignments(poi_no_data_water_outside_us).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: C }, data.assignments(no_poi_built_outside_us).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: C }, data.assignments(no_poi_grass_outside_us).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: C }, data.assignments(no_poi_water_outside_us).await?);
            // never inserted
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: C }, data.assignments(unknown_cell).await?);
        };

        Ok(())
    }
}
