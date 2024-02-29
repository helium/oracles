use base64::{engine::general_purpose, Engine as _};
use h3o::{LatLng, Resolution};
use hextree::{Cell, HexTreeSet};
use std::{fs, io::Read, path, sync::Arc};

use crate::heartbeats::Heartbeat;

pub trait GeofenceValidator: Clone + Send + Sync + 'static {
    fn in_valid_region(&self, heartbeat: &Heartbeat) -> bool;
}

#[derive(Clone)]
pub struct Geofence {
    regions: Arc<HexTreeSet>,
    resolution: Resolution,
}

impl Geofence {
    pub fn new(paths: Vec<std::path::PathBuf>, resolution: Resolution) -> anyhow::Result<Self> {
        Ok(Self {
            regions: Arc::new(valid_mapping_regions(paths)?),
            resolution,
        })
    }
}

impl GeofenceValidator for Geofence {
    fn in_valid_region(&self, heartbeat: &Heartbeat) -> bool {
        let Ok(lat_lon) = LatLng::new(heartbeat.lat, heartbeat.lon) else {
            return false;
        };
        let Ok(cell) = Cell::try_from(u64::from(lat_lon.to_cell(self.resolution))) else {
            return false;
        };
        self.regions.contains(cell)
    }
}

pub fn valid_mapping_regions(encoded_files: Vec<std::path::PathBuf>) -> anyhow::Result<HexTreeSet> {
    let mut combined_regions: Vec<Cell> = Vec::new();
    for file in encoded_files {
        let indexes = from_base64_file(file)?;
        combined_regions.extend(indexes);
    }
    let region_set: HexTreeSet = combined_regions.iter().collect();
    Ok(region_set)
}

fn from_base64_file<P: AsRef<path::Path>>(file: P) -> anyhow::Result<Vec<Cell>> {
    let mut file = fs::File::open(file.as_ref())?;
    let mut encoded_string = String::new();
    file.read_to_string(&mut encoded_string)?;

    let compressed_bytes = general_purpose::STANDARD.decode(&encoded_string)?;
    let mut decoder = flate2::read::GzDecoder::new(&compressed_bytes[..]);

    let mut uncompressed_bytes = Vec::new();
    decoder.read_to_end(&mut uncompressed_bytes)?;

    let mut indexes: Vec<Cell> = Vec::new();
    let mut h3_idx_buf = [0_u8; 8];
    for chunk in uncompressed_bytes.chunks(8) {
        h3_idx_buf.as_mut_slice().copy_from_slice(chunk);
        let index = u64::from_le_bytes(h3_idx_buf);
        indexes.push(Cell::try_from(index)?);
    }
    Ok(indexes)
}
