use std::{
    io::Cursor,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use hextree::{Cell, HexTreeMap};
use tokio::{fs::File, io::AsyncWriteExt};

pub const CENTER_CELL: u64 = 0x8a5d107916dffff;

pub const INNER_1_CELL: u64 = 0x8a5d107916c7fff;
pub const INNER_2_CELL: u64 = 0x8a5d107916cffff;
pub const INNER_3_CELL: u64 = 0x8a5d1079ed67fff;
pub const INNER_4_CELL: u64 = 0x8a5d1079ed77fff;
pub const INNER_5_CELL: u64 = 0x8a5d1079ed2ffff;
pub const INNER_6_CELL: u64 = 0x8a5d107916d7fff;

pub const OUTER_1_CELL: u64 = 0x8a5d107916e7fff;
pub const OUTER_2_CELL: u64 = 0x8a5d107916effff;
pub const OUTER_3_CELL: u64 = 0x8a5d1079e9b7fff;
pub const OUTER_4_CELL: u64 = 0x8a5d1079e997fff;
pub const OUTER_5_CELL: u64 = 0x8a5d1079ed6ffff;
pub const OUTER_6_CELL: u64 = 0x8a5d1079ed47fff;
pub const OUTER_7_CELL: u64 = 0x8a5d1079ed57fff;
pub const OUTER_8_CELL: u64 = 0x8a5d1079ed0ffff;
pub const OUTER_9_CELL: u64 = 0x8a5d1079ed07fff;
pub const OUTER_10_CELL: u64 = 0x8a5d1079ed27fff;
pub const OUTER_11_CELL: u64 = 0x8a5d1079168ffff;
pub const OUTER_12_CELL: u64 = 0x8a5d107916f7fff;

/// Generate footfall, landtype and urbanization
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self) -> Result<()> {
        let center_cell = Cell::from_raw(CENTER_CELL)?;

        let inner_1_cell = Cell::from_raw(INNER_1_CELL)?;
        let inner_2_cell = Cell::from_raw(INNER_2_CELL)?;
        let inner_3_cell = Cell::from_raw(INNER_3_CELL)?;
        let inner_4_cell = Cell::from_raw(INNER_4_CELL)?;
        let inner_5_cell = Cell::from_raw(INNER_5_CELL)?;
        let inner_6_cell = Cell::from_raw(INNER_6_CELL)?;

        let outer_1_cell = Cell::from_raw(OUTER_1_CELL)?;
        let outer_2_cell = Cell::from_raw(OUTER_2_CELL)?;
        let outer_3_cell = Cell::from_raw(OUTER_3_CELL)?;
        let outer_4_cell = Cell::from_raw(OUTER_4_CELL)?;
        let outer_5_cell = Cell::from_raw(OUTER_5_CELL)?;
        let outer_6_cell = Cell::from_raw(OUTER_6_CELL)?;
        let outer_7_cell = Cell::from_raw(OUTER_7_CELL)?;
        let outer_8_cell = Cell::from_raw(OUTER_8_CELL)?;
        let outer_9_cell = Cell::from_raw(OUTER_9_CELL)?;
        let outer_10_cell = Cell::from_raw(OUTER_10_CELL)?;
        let outer_11_cell = Cell::from_raw(OUTER_11_CELL)?;
        let outer_12_cell = Cell::from_raw(OUTER_12_CELL)?;

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

        hex_tree_map_to_file(urbanized, "urbanized").await?;
        hex_tree_map_to_file(footfall, "footfall").await?;
        hex_tree_map_to_file(landtype, "landtype").await?;

        Ok(())
    }
}

async fn hex_tree_map_to_file(map: HexTreeMap<u8>, name: &str) -> anyhow::Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let file_name = format!("{name}.{now}.gz");
    let disktree_file = File::create(file_name.clone()).await?;

    let mut data = vec![];
    map.to_disktree(Cursor::new(&mut data), |w, v| w.write_all(&[*v]))?;

    let mut writer = async_compression::tokio::write::GzipEncoder::new(disktree_file);
    writer.write_all(&data).await?;
    writer.shutdown().await?;

    Ok(())
}
