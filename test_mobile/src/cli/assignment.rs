use std::{
    fs::{self, File},
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use flate2::{write::GzEncoder, Compression};
use hextree::{Cell, HexTreeMap};

/// Generate footfall, landtype and urbanization
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self) -> Result<()> {
        let center_cell = Cell::from_raw(0x8a5d107916dffff)?;

        let inner_1_cell = Cell::from_raw(0x8a5d107916c7fff)?;
        let inner_2_cell = Cell::from_raw(0x8a5d107916cffff)?;
        let inner_3_cell = Cell::from_raw(0x8a5d1079ed67fff)?;
        let inner_4_cell = Cell::from_raw(0x8a5d1079ed77fff)?;
        let inner_5_cell = Cell::from_raw(0x8a5d1079ed2ffff)?;
        let inner_6_cell = Cell::from_raw(0x8a5d107916d7fff)?;

        let outer_1_cell = Cell::from_raw(0x8a5d107916e7fff)?;
        let outer_2_cell = Cell::from_raw(0x8a5d107916effff)?;
        let outer_3_cell = Cell::from_raw(0x8a5d1079e9b7fff)?;
        let outer_4_cell = Cell::from_raw(0x8a5d1079e997fff)?;
        let outer_5_cell = Cell::from_raw(0x8a5d1079ed6ffff)?;
        let outer_6_cell = Cell::from_raw(0x8a5d1079ed47fff)?;
        let outer_7_cell = Cell::from_raw(0x8a5d1079ed57fff)?;
        let outer_8_cell = Cell::from_raw(0x8a5d1079ed0ffff)?;
        let outer_9_cell = Cell::from_raw(0x8a5d1079ed07fff)?;
        let outer_10_cell = Cell::from_raw(0x8a5d1079ed27fff)?;
        let outer_11_cell = Cell::from_raw(0x8a5d1079168ffff)?;
        let outer_12_cell = Cell::from_raw(0x8a5d107916f7fff)?;

        // Footfall Data
        // POI         - footfalls > 1 for a POI across hexes
        // POI No Data - No footfalls for a POI across any hexes
        // NO POI      - Does not exist
        let mut footfall = HexTreeMap::<u8>::new();
        footfall.insert(center_cell, 42);
        footfall.insert(inner_1_cell, 42);
        footfall.insert(inner_2_cell, 42);
        footfall.insert(inner_3_cell, 42);
        footfall.insert(inner_4_cell, 42);
        footfall.insert(inner_5_cell, 42);
        footfall.insert(inner_6_cell, 42);
        footfall.insert(outer_1_cell, 0);
        footfall.insert(outer_2_cell, 0);
        footfall.insert(outer_3_cell, 0);
        footfall.insert(outer_4_cell, 0);
        footfall.insert(outer_5_cell, 0);
        footfall.insert(outer_6_cell, 0);
        // outer_7 to 12 =  NO POI

        // Landtype Data
        // Map to enum values for Landtype
        // An unknown cell is considered Assignment::C
        let mut landtype = HexTreeMap::<u8>::new();
        landtype.insert(center_cell, 50);
        landtype.insert(inner_1_cell, 10);
        landtype.insert(inner_2_cell, 10);
        landtype.insert(inner_3_cell, 10);
        landtype.insert(inner_4_cell, 10);
        landtype.insert(inner_5_cell, 10);
        landtype.insert(inner_6_cell, 10);
        landtype.insert(outer_1_cell, 60);
        landtype.insert(outer_2_cell, 60);
        landtype.insert(outer_3_cell, 60);
        landtype.insert(outer_4_cell, 60);
        landtype.insert(outer_5_cell, 60);
        landtype.insert(outer_6_cell, 60);
        // outer_7 to 12 = unknown

        // Urbanized data
        // Urban     - something in the map, and in the geofence
        // Not Urban - nothing in the map, but in the geofence
        // Outside   - not in the geofence, urbanized hex never considered
        let mut urbanized = HexTreeMap::<u8>::new();
        urbanized.insert(center_cell, 1);
        urbanized.insert(inner_1_cell, 1);
        urbanized.insert(inner_2_cell, 1);
        urbanized.insert(inner_3_cell, 1);
        urbanized.insert(inner_4_cell, 1);
        urbanized.insert(inner_5_cell, 1);
        urbanized.insert(inner_6_cell, 1);
        urbanized.insert(outer_1_cell, 1);
        urbanized.insert(outer_2_cell, 1);
        urbanized.insert(outer_3_cell, 1);
        urbanized.insert(outer_4_cell, 1);
        urbanized.insert(outer_5_cell, 1);
        urbanized.insert(outer_6_cell, 1);
        urbanized.insert(outer_7_cell, 0);
        urbanized.insert(outer_8_cell, 0);
        urbanized.insert(outer_9_cell, 0);
        urbanized.insert(outer_10_cell, 0);
        urbanized.insert(outer_11_cell, 0);
        urbanized.insert(outer_12_cell, 0);

        hex_tree_map_to_file(urbanized, "urbanized")?;
        hex_tree_map_to_file(footfall, "footfall")?;
        hex_tree_map_to_file(landtype, "landtype")?;

        Ok(())
    }
}

fn hex_tree_map_to_file(map: HexTreeMap<u8>, name: &str) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let file_name = format!("{name}.{now}");
    let data_file = File::create(file_name.clone())?;

    map.to_disktree(data_file, |w, v| w.write_all(&[*v]))?;

    let mut input = io::BufReader::new(File::open(file_name.clone())?);
    let output = File::create(format!("{}.gz", file_name.clone()))?;

    let mut encoder = GzEncoder::new(output, Compression::default());
    io::copy(&mut input, &mut encoder)?;
    encoder.finish()?;

    fs::remove_file(file_name.clone())?;

    Ok(())
}
