use h3ron::{FromH3Index, H3Cell, Index};
use itertools::Itertools;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{cmp, collections::HashMap, ops::Range, sync::Arc};
use tokio::sync::RwLock;

pub struct HexResConfig {
    pub neighbors: u64,
    pub target: u64,
    pub max: u64,
}

impl HexResConfig {
    pub const fn new(neighbors: u64, target: u64, max: u64) -> Self {
        Self {
            neighbors,
            target,
            max,
        }
    }
}

type HexMap = HashMap<H3Cell, u64>;

const DENSITY_TGT_RES: u8 = 4;
const MAX_RES: u8 = 11;
const USED_RES: Range<u8> = DENSITY_TGT_RES..MAX_RES;
const SCALING_RES: Range<u8> = DENSITY_TGT_RES..MAX_RES + 2;
pub const SCALING_PRECISION: u32 = 4;

static HIP17_RES_CONFIG: [Option<HexResConfig>; 11] = [
    // Hex resolutions 0 - 3 and 11 and 12 are currently ignored when calculating density;
    // For completeness sake their on-chain settings are N=2, TGT=100_000, MAX=100_000
    None,                                 // 0
    None,                                 // 1
    None,                                 // 2
    None,                                 // 3
    Some(HexResConfig::new(1, 250, 800)), // 4
    Some(HexResConfig::new(1, 100, 400)), // 5
    Some(HexResConfig::new(1, 25, 100)),  // 6
    Some(HexResConfig::new(2, 5, 20)),    // 7
    Some(HexResConfig::new(2, 1, 4)),     // 8
    Some(HexResConfig::new(2, 1, 2)),     // 9
    Some(HexResConfig::new(2, 1, 1)),     // 10
];

#[async_trait::async_trait]
pub trait HexDensityMap: Clone {
    async fn get(&self, hex: &str) -> Option<Decimal>;
    async fn swap(&self, new_map: HashMap<String, Decimal>);
}

#[derive(Debug, Clone)]
pub struct SharedHexDensityMap(Arc<RwLock<HashMap<String, Decimal>>>);

impl SharedHexDensityMap {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

#[async_trait::async_trait]
impl HexDensityMap for SharedHexDensityMap {
    async fn get(&self, hex: &str) -> Option<Decimal> {
        self.0.read().await.get(hex).cloned()
    }

    async fn swap(&self, new_map: HashMap<String, Decimal>) {
        *self.0.write().await = new_map;
    }
}

#[derive(Debug)]
pub struct GlobalHexMap {
    clipped_hexes: HexMap,
    unclipped_hexes: HexMap,
    asserted_hexes: Vec<H3Cell>,
}

impl GlobalHexMap {
    pub fn new() -> Self {
        Self {
            clipped_hexes: HashMap::new(),
            unclipped_hexes: HashMap::new(),
            asserted_hexes: Vec::new(),
        }
    }

    pub fn increment_unclipped(&mut self, index: u64) {
        let cell = H3Cell::from_h3index(index);
        if let Ok(parent) = cell.get_parent(MAX_RES) {
            self.unclipped_hexes
                .entry(parent)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            self.clipped_hexes
                .entry(parent)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            self.asserted_hexes.push(cell);
        }
    }

    pub fn reduce_global(&mut self) {
        // At the point this reduce is triggered the only keys present in the unclipped
        // hexmap are the res 11 parent keys
        let starting_hexes: Vec<H3Cell> =
            self.unclipped_hexes.clone().into_keys().unique().collect();
        reduce_hex_res(
            &mut self.unclipped_hexes,
            &mut self.clipped_hexes,
            starting_hexes,
        )
    }
}

fn rollup_child_count(
    unclipped_map: &mut HexMap,
    clipped_map: &mut HexMap,
    cell: H3Cell,
    parent: H3Cell,
) {
    let cell_count = clipped_map.get(&cell).map_or(0, |count| *count);

    unclipped_map
        .entry(parent)
        .and_modify(|parent_count| *parent_count += cell_count)
        .or_insert(cell_count);

    // This block is marked in the original code as "not sure if required" but removing it
    // results in dynamically unpredictable result scaling maps.
    clipped_map
        .entry(parent)
        .and_modify(|parent_count| *parent_count += cell_count)
        .or_insert(cell_count);
}

fn reduce_hex_res(unclipped: &mut HexMap, clipped: &mut HexMap, hex_list: Vec<H3Cell>) {
    let mut hexes_at_res: Vec<H3Cell> = hex_list;
    for res in USED_RES.rev() {
        std::mem::take(&mut hexes_at_res)
            .into_iter()
            .for_each(|cell| {
                if let Ok(parent) = cell.get_parent(res) {
                    rollup_child_count(unclipped, clipped, cell, parent);
                    hexes_at_res.push(parent);
                }
            });
        let density_tgt = get_res_tgt(res);
        hexes_at_res = hexes_at_res
            .into_iter()
            .unique()
            .map(|parent_cell| {
                let occupied_count = occupied_count(clipped, &parent_cell, density_tgt);
                let limit = limit(res, occupied_count);
                if let Some(count) = unclipped.get(&parent_cell) {
                    let actual = cmp::min(limit, *count);
                    clipped.insert(parent_cell, actual);
                }
                parent_cell
            })
            .collect()
    }
}

fn occupied_count(cell_map: &HexMap, hex: &H3Cell, density_tgt: u64) -> u64 {
    match hex.grid_disk(1) {
        Ok(k_ring) => k_ring.into_iter().fold(0, |count, cell| {
            cell_map.get(&cell).map_or(count, |population| {
                if *population >= density_tgt {
                    count + 1
                } else {
                    count
                }
            })
        }),
        Err(_) => 0,
    }
}

fn limit(res: u8, occupied_count: u64) -> u64 {
    let res_config = HIP17_RES_CONFIG
        .get(res as usize)
        .unwrap()
        .as_ref()
        .unwrap();
    let occupied_neighbor_diff = occupied_count.saturating_sub(res_config.neighbors);
    let max = cmp::max((occupied_neighbor_diff) + 1, 1);
    cmp::min(res_config.max, res_config.target * max)
}

pub fn compute_hex_density_map(global_map: &GlobalHexMap) -> HashMap<String, Decimal> {
    let mut map = HashMap::new();
    for hex in &global_map.asserted_hexes {
        let scale: Decimal = SCALING_RES.rev().into_iter().fold(dec!(1.0), |scale, res| {
            hex.get_parent(res).map_or(scale, |parent| {
                match (
                    global_map.unclipped_hexes.get(&parent),
                    global_map.clipped_hexes.get(&parent),
                ) {
                    (Some(unclipped), Some(clipped)) => {
                        scale
                            * (Decimal::new(*clipped as i64, SCALING_PRECISION)
                                / Decimal::new(*unclipped as i64, SCALING_PRECISION))
                    }
                    _ => scale,
                }
            })
        });
        let trunc_scale = scale.round_dp(SCALING_PRECISION);
        map.insert(hex.h3index().to_string(), trunc_scale);
    }

    map
}

fn get_res_tgt(res: u8) -> u64 {
    HIP17_RES_CONFIG
        .get(res as usize)
        .unwrap()
        .as_ref()
        .map(|config| config.target)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_scale_check() {
        let indexes: Vec<u64> = vec![
            // Hex region generated from live ledger as the 2022-10-14
            // population of from the res 8 882830D341FFFFF / 613196592008134655
            631210990515536895, // note: 3
            631210990515536895, // for this
            631210990515536895, // res 12
            631210990515537919, // 3
            631210990515537919, // for this
            631210990515537919, // res 12 as well
            631210990515538431,
            631210990515564031,
            631210990515589631,
            631210990515600383,
            631210990515601919,
            631210990515606527,
            631210990515722239, // 2 hotspots
            631210990515722239, // in this hex
            631210990515727359,
            631210990515728895,
            631210990515739647,
            631210990515874303,
            631210990515924479,
            631210990516363775,
            631210990515987455,
            631210990516590079,
            631210990516612607,
            631210990516613631,
            631210990516640767,
            631210990516876799,
            631210990516885503,
            631210990516888063,
            631210990516907007,
            631210990516912639,
            631210990516955647,
            631210990516996607,
            631210990517011455,
            631210990517016575,
            631210990517144063,
            631210990517264895,
        ];
        let mut gw_map = GlobalHexMap::new();
        for index in indexes {
            gw_map.increment_unclipped(index);
        }
        gw_map.reduce_global();
        let hex_density_map = compute_hex_density_map(&gw_map);

        let expected_map = HashMap::from([
            ("631210990515538431".to_string(), dec!(0.0065)),
            ("631210990515727359".to_string(), dec!(0.0091)),
            ("631210990515924479".to_string(), dec!(0.0606)),
            ("631210990515537919".to_string(), dec!(0.0065)),
            ("631210990517011455".to_string(), dec!(0.0152)),
            ("631210990516885503".to_string(), dec!(0.0152)),
            ("631210990516888063".to_string(), dec!(0.0152)),
            ("631210990516955647".to_string(), dec!(0.0455)),
            ("631210990515728895".to_string(), dec!(0.0091)),
            ("631210990517144063".to_string(), dec!(0.0909)),
            ("631210990515739647".to_string(), dec!(0.0091)),
            ("631210990516640767".to_string(), dec!(0.0606)),
            ("631210990515601919".to_string(), dec!(0.0114)),
            ("631210990516590079".to_string(), dec!(0.0606)),
            ("631210990517264895".to_string(), dec!(0.0909)),
            ("631210990515606527".to_string(), dec!(0.0114)),
            ("631210990515874303".to_string(), dec!(0.0606)),
            ("631210990516613631".to_string(), dec!(0.0303)),
            ("631210990515589631".to_string(), dec!(0.0114)),
            ("631210990515564031".to_string(), dec!(0.0455)),
            ("631210990516612607".to_string(), dec!(0.0303)),
            ("631210990516876799".to_string(), dec!(0.0152)),
            ("631210990516363775".to_string(), dec!(0.0909)),
            ("631210990516907007".to_string(), dec!(0.0227)),
            ("631210990516912639".to_string(), dec!(0.0227)),
            ("631210990515722239".to_string(), dec!(0.0091)),
            ("631210990516996607".to_string(), dec!(0.0152)),
            ("631210990515987455".to_string(), dec!(0.0606)),
            ("631210990515600383".to_string(), dec!(0.0114)),
            ("631210990517016575".to_string(), dec!(0.0152)),
            ("631210990515536895".to_string(), dec!(0.0065)),
        ]);
        assert_eq!(hex_density_map, expected_map);
    }
}
