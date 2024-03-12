pub mod assignment;

use std::collections::HashMap;

use crate::geofence::GeofenceValidator;
pub use assignment::Assignment;
use hextree::disktree::DiskTreeMap;

pub trait DiskTreeLike: Send + Sync + 'static {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>>;
}

impl DiskTreeLike for DiskTreeMap {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        self.get(cell)
    }
}

impl DiskTreeLike for HashMap<hextree::Cell, Vec<u8>> {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(self.get(&cell).map(|x| (cell, x.as_slice())))
    }
}

pub struct MockDiskTree;

impl DiskTreeLike for MockDiskTree {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(Some((cell, &[])))
    }
}

pub struct UrbanizationData<DT, GF> {
    urbanized: DT,
    usa_geofence: GF,
}

impl<DT, GF> UrbanizationData<DT, GF> {
    pub fn new(urbanized: DT, usa_geofence: GF) -> Self {
        Self {
            urbanized,
            usa_geofence,
        }
    }
}

impl<DT, GF> UrbanizationData<DT, GF>
where
    DT: DiskTreeLike,
    GF: GeofenceValidator<hextree::Cell>,
{
    fn is_urbanized(&self, cell: hextree::Cell) -> anyhow::Result<bool> {
        let result = self.urbanized.get(cell)?;
        Ok(result.is_some())
    }

    pub fn hex_assignment(&self, hex: hextree::Cell) -> anyhow::Result<Assignment> {
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

pub trait FootfallLike: Send + Sync + 'static {
    fn get(&self, cell: hextree::Cell) -> Option<bool>;
}

#[derive(Default)]
pub struct FootfallData<FL> {
    values: FL,
}

impl<FL> FootfallData<FL>
where
    FL: FootfallLike,
{
    pub fn new(values: FL) -> Self {
        Self { values }
    }

    pub fn hex_assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        match self.values.get(cell) {
            Some(true) => Ok(Assignment::A),
            Some(false) => Ok(Assignment::B),
            None => Ok(Assignment::C),
        }
    }
}

impl FootfallLike for HashMap<hextree::Cell, bool> {
    fn get(&self, cell: hextree::Cell) -> Option<bool> {
        self.get(&cell).cloned()
    }
}

pub struct MockFootfallData;

impl FootfallLike for MockFootfallData {
    fn get(&self, _cell: hextree::Cell) -> Option<bool> {
        Some(true)
    }
}
