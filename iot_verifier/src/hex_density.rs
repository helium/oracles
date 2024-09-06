use file_store::SCALING_PRECISION;
use h3o::{CellIndex, Resolution};
use itertools::Itertools;
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{cmp, collections::HashMap, sync::Arc};
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

type HexMap = HashMap<CellIndex, u64>;

const MAX_RES: Resolution = Resolution::Eleven;
const USED_RES: [Resolution; 7] = [
    Resolution::Ten,
    Resolution::Nine,
    Resolution::Eight,
    Resolution::Seven,
    Resolution::Six,
    Resolution::Five,
    Resolution::Four,
];
const SCALING_RES: [Resolution; 10] = [
    Resolution::Thirteen,
    Resolution::Twelve,
    Resolution::Eleven,
    Resolution::Ten,
    Resolution::Nine,
    Resolution::Eight,
    Resolution::Seven,
    Resolution::Six,
    Resolution::Five,
    Resolution::Four,
];

lazy_static! {
    static ref HIP104_RES_CONFIG: HashMap<Resolution, HexResConfig> = {
        // Hex resolutions 0 - 3 and 11 and 12 are currently ignored when calculating density;
        // For completeness sake their on-chain settings are N=2, TGT=100_000, MAX=100_000
        let mut configs = HashMap::new();
        configs.insert(Resolution::Four, HexResConfig::new(2, 500, 1000));
        configs.insert(Resolution::Five, HexResConfig::new(4, 100, 200));
        configs.insert(Resolution::Six, HexResConfig::new(4, 25, 50));
        configs.insert(Resolution::Seven, HexResConfig::new(4, 5, 10));
        configs.insert(Resolution::Eight, HexResConfig::new(2, 1, 1));
        configs.insert(Resolution::Nine, HexResConfig::new(2, 1, 1));
        configs.insert(Resolution::Ten, HexResConfig::new(2, 1, 1));
        configs
    };
}

#[derive(Debug, Clone)]
pub struct HexDensityMap(Arc<RwLock<HashMap<u64, Decimal>>>);

impl Default for HexDensityMap {
    fn default() -> Self {
        Self::new()
    }
}

impl HexDensityMap {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub async fn get(&self, hex: u64) -> Option<Decimal> {
        self.0.read().await.get(&hex).cloned()
    }

    pub async fn swap(&self, new_map: HashMap<u64, Decimal>) {
        *self.0.write().await = new_map;
    }
}

#[derive(Debug)]
pub struct GlobalHexMap {
    clipped_hexes: HexMap,
    unclipped_hexes: HexMap,
    asserted_hexes: Vec<CellIndex>,
}

impl Default for GlobalHexMap {
    fn default() -> Self {
        Self::new()
    }
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
        if let Ok(cell) = CellIndex::try_from(index) {
            if let Some(parent) = cell.parent(MAX_RES) {
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
    }

    pub fn reduce_global(&mut self) {
        // At the point this reduce is triggered the only keys present in the unclipped
        // hexmap are the res 11 parent keys
        let starting_hexes: Vec<CellIndex> =
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
    cell: CellIndex,
    parent: CellIndex,
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

fn reduce_hex_res(unclipped: &mut HexMap, clipped: &mut HexMap, hex_list: Vec<CellIndex>) {
    let mut hexes_at_res: Vec<CellIndex> = hex_list;
    for res in USED_RES {
        std::mem::take(&mut hexes_at_res)
            .into_iter()
            .for_each(|cell| {
                if let Some(parent) = cell.parent(res) {
                    rollup_child_count(unclipped, clipped, cell, parent);
                    hexes_at_res.push(parent);
                }
            });
        let density_tgt = get_res_tgt(&res);
        hexes_at_res = hexes_at_res
            .into_iter()
            .unique()
            .inspect(|parent_cell| {
                let occupied_count = occupied_count(clipped, parent_cell, density_tgt);
                let limit = limit(&res, occupied_count);
                if let Some(count) = unclipped.get(parent_cell) {
                    let actual = cmp::min(limit, *count);
                    clipped.insert(*parent_cell, actual);
                }
            })
            .collect()
    }
}

fn occupied_count(cell_map: &HexMap, hex: &CellIndex, density_tgt: u64) -> u64 {
    let k_ring = hex.grid_disk::<Vec<_>>(1);
    k_ring.into_iter().fold(0, |count, cell| {
        cell_map.get(&cell).map_or(count, |population| {
            if *population >= density_tgt {
                count + 1
            } else {
                count
            }
        })
    })
}

fn limit(res: &h3o::Resolution, occupied_count: u64) -> u64 {
    let res_config = HIP104_RES_CONFIG.get(res).unwrap();
    let occupied_neighbor_diff = occupied_count.saturating_sub(res_config.neighbors);
    let max = cmp::max((occupied_neighbor_diff) + 1, 1);
    cmp::min(res_config.max, res_config.target * max)
}

pub fn compute_hex_density_map(global_map: &GlobalHexMap) -> HashMap<u64, Decimal> {
    let mut map: HashMap<u64, Decimal> = HashMap::new();
    for hex in &global_map.asserted_hexes {
        let scale: Decimal = SCALING_RES.iter().fold(dec!(1.0), |scale, res| {
            hex.parent(*res).map_or(scale, |parent| {
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
        map.insert(u64::from(*hex), trunc_scale);
    }
    map
}

fn get_res_tgt(res: &Resolution) -> u64 {
    HIP104_RES_CONFIG
        .get(res)
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

        let expected_map = HashMap::<u64, Decimal>::from([
            (631210990515537919, dec!(0.0060)),
            (631210990516955647, dec!(0.0417)),
            (631210990516885503, dec!(0.0139)),
            (631210990515601919, dec!(0.0104)),
            (631210990516590079, dec!(0.0556)),
            (631210990516888063, dec!(0.0139)),
            (631210990516640767, dec!(0.0556)),
            (631210990516907007, dec!(0.0208)),
            (631210990515874303, dec!(0.0556)),
            (631210990517011455, dec!(0.0139)),
            (631210990516363775, dec!(0.1667)),
            (631210990515589631, dec!(0.0104)),
            (631210990516996607, dec!(0.0139)),
            (631210990515600383, dec!(0.0104)),
            (631210990517016575, dec!(0.0139)),
            (631210990515606527, dec!(0.0104)),
            (631210990515739647, dec!(0.0083)),
            (631210990516613631, dec!(0.0278)),
            (631210990516876799, dec!(0.0139)),
            (631210990515924479, dec!(0.0556)),
            (631210990515538431, dec!(0.0060)),
            (631210990515536895, dec!(0.0060)),
            (631210990517144063, dec!(0.0833)),
            (631210990517264895, dec!(0.0833)),
            (631210990516612607, dec!(0.0278)),
            (631210990515722239, dec!(0.0083)),
            (631210990516912639, dec!(0.0208)),
            (631210990515727359, dec!(0.0083)),
            (631210990515564031, dec!(0.0417)),
            (631210990515728895, dec!(0.0083)),
            (631210990515987455, dec!(0.0556)),
        ]);
        assert_eq!(hex_density_map, expected_map);
    }
}
