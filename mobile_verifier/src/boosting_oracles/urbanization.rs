use super::Assignment;
use crate::geofence::GeofenceValidator;

pub struct Urbanization<DT, GF> {
    urbanized: DT,
    usa_geofence: GF,
}

impl<DT, GF> Urbanization<DT, GF> {
    pub fn new(urbanized: DT, usa_geofence: GF) -> Self {
        Self {
            urbanized,
            usa_geofence,
        }
    }
}

impl<DT, GF> Urbanization<DT, GF>
where
    DT: DiskTreeLike,
    GF: GeofenceValidator<u64>,
{
    fn is_urbanized(&self, location: u64) -> anyhow::Result<bool> {
        let cell = hextree::Cell::from_raw(location)?;
        let result = self.urbanized.get(cell)?;
        Ok(result.is_some())
    }

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
