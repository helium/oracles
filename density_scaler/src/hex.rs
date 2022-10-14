use h3ron::{FromH3Index, H3Cell, Index};
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::Serialize;
use std::{cmp, collections::HashMap, ops::Range};

pub struct HexResConfig {
    pub neighbors: u64,
    pub target: u64,
    pub max: u64,
}

impl HexResConfig {
    pub fn new(neighbors: u64, target: u64, max: u64) -> Self {
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

lazy_static! {
    static ref HIP17_RES_CONFIG: HashMap<u8, HexResConfig> = {
        let mut conf_map = HashMap::new();
        // Hex resolutions 0 - 3 and 11 and 12 are currently ignored by the
        // global hex population constructor, filtered by their target value.
        // For completeness sake their on-chain settings are N=2, TGT=100_000, MAX=100_000
        conf_map.insert(4, HexResConfig::new(1, 250, 800));
        conf_map.insert(5, HexResConfig::new(1, 100, 400));
        conf_map.insert(6, HexResConfig::new(1, 25, 100));
        conf_map.insert(7, HexResConfig::new(2, 5, 20));
        conf_map.insert(8, HexResConfig::new(2, 1, 4));
        conf_map.insert(9, HexResConfig::new(2, 1, 2));
        conf_map.insert(10, HexResConfig::new(2, 1, 1));
        conf_map
    };
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ScalingMap(HashMap<String, f64>);

impl ScalingMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, hex: &str, scale_factor: f64) -> Option<f64> {
        self.0.insert(hex.to_string(), scale_factor)
    }

    pub fn get(&self, hex: &str) -> Option<&f64> {
        self.0.get(hex)
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
        if let Some(density_tgt) = get_res_tgt(res) {
            hexes_at_res = hexes_at_res
                .into_iter()
                .unique()
                .map(|parent_cell| {
                    let occupied_count = occupied_count(clipped, &parent_cell, density_tgt);
                    if let (Some(limit), Some(count)) =
                        (limit(res, occupied_count), unclipped.get(&parent_cell))
                    {
                        let actual = cmp::min(limit, *count);
                        clipped.insert(parent_cell, actual);
                    }
                    parent_cell
                })
                .collect()
        }
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

fn limit(res: u8, occupied_count: u64) -> Option<u64> {
    if let Some(res_config) = HIP17_RES_CONFIG.get(&res) {
        let occupied_neighbor_diff = occupied_count.saturating_sub(res_config.neighbors);
        let max = cmp::max((occupied_neighbor_diff) + 1, 1);
        return Some(cmp::min(res_config.max, res_config.target * max));
    }
    None
}

pub fn compute_scaling_map(global_map: &GlobalHexMap, scaling_map: &mut ScalingMap) {
    for hex in &global_map.asserted_hexes {
        let scale: f64 = SCALING_RES.rev().into_iter().fold(1.0, |scale, res| {
            hex.get_parent(res).map_or(scale, |parent| {
                match (
                    global_map.unclipped_hexes.get(&parent),
                    global_map.clipped_hexes.get(&parent),
                ) {
                    (Some(unclipped), Some(clipped)) => {
                        scale * (*clipped as f64 / *unclipped as f64)
                    }
                    _ => scale,
                }
            })
        });
        let trunc_scale = round_scale_factor(scale);
        scaling_map.insert(&hex.h3index().to_string(), trunc_scale);
    }
}

fn round_scale_factor(scale: f64) -> f64 {
    (scale * 10000.0).round() / 10000.0
}

fn get_res_tgt(res: u8) -> Option<u64> {
    HIP17_RES_CONFIG.get(&res).map(|config| config.target)
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
            631210990515987455,
            631210990516363775,
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
        let mut scale_map = ScalingMap::new();
        compute_scaling_map(&gw_map, &mut scale_map);

        let expected_map = HashMap::from([
            ("631210990515538431".to_string(), 0.0065),
            ("631210990515727359".to_string(), 0.0091),
            ("631210990515924479".to_string(), 0.0606),
            ("631210990515537919".to_string(), 0.0065),
            ("631210990517011455".to_string(), 0.0152),
            ("631210990516885503".to_string(), 0.0152),
            ("631210990516888063".to_string(), 0.0152),
            ("631210990516955647".to_string(), 0.0455),
            ("631210990515728895".to_string(), 0.0091),
            ("631210990517144063".to_string(), 0.0909),
            ("631210990515739647".to_string(), 0.0091),
            ("631210990516640767".to_string(), 0.0606),
            ("631210990515601919".to_string(), 0.0114),
            ("631210990516590079".to_string(), 0.0606),
            ("631210990517264895".to_string(), 0.0909),
            ("631210990515606527".to_string(), 0.0114),
            ("631210990515874303".to_string(), 0.0606),
            ("631210990516613631".to_string(), 0.0303),
            ("631210990515589631".to_string(), 0.0114),
            ("631210990515564031".to_string(), 0.0455),
            ("631210990516612607".to_string(), 0.0303),
            ("631210990516876799".to_string(), 0.0152),
            ("631210990516363775".to_string(), 0.0909),
            ("631210990516907007".to_string(), 0.0227),
            ("631210990516912639".to_string(), 0.0227),
            ("631210990515722239".to_string(), 0.0091),
            ("631210990516996607".to_string(), 0.0152),
            ("631210990515987455".to_string(), 0.0606),
            ("631210990515600383".to_string(), 0.0114),
            ("631210990517016575".to_string(), 0.0152),
            ("631210990515536895".to_string(), 0.0065),
        ]);
        assert_eq!(scale_map, ScalingMap(expected_map));
    }
}
