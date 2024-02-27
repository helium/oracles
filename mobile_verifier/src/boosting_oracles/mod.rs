pub mod assignment;

use crate::geofence::GeofenceValidator;
pub use assignment::Assignment;
use hextree::disktree::DiskTreeMap;

pub struct Urbanization<T> {
    urbanized: DiskTreeMap,
    usa_geofence: T,
}

impl<T> Urbanization<T> {
    pub fn new(urbanized: DiskTreeMap, usa_geofence: T) -> Self {
        Self {
            urbanized,
            usa_geofence,
        }
    }

    fn is_urbanized(&self, location: u64) -> anyhow::Result<bool> {
        let cell = hextree::Cell::from_raw(location)?;
        let result = self.urbanized.get(cell)?;
        Ok(result.is_some())
    }
}

impl<T> Urbanization<T>
where
    T: GeofenceValidator<u64>,
{
    pub fn hex_assignment(&self, hex: u64) -> anyhow::Result<Assignment> {
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
