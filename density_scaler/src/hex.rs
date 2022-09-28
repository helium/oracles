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

const MAX_RES: u8 = 11;
const DENSITY_TGT_RES: u8 = 4;
const USED_RES: Range<u8> = DENSITY_TGT_RES..MAX_RES;

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

#[derive(Debug, Serialize)]
pub struct ScalingMap(HashMap<String, f64>);

impl ScalingMap {
    pub fn new() -> ScalingMap {
        let map: HashMap<String, f64> = HashMap::new();
        ScalingMap(map)
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
        let starting_hexes: Vec<H3Cell> = self.unclipped_hexes.clone().into_keys().collect();
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
            .unique()
            .for_each(|cell| {
                if let Ok(parent) = cell.get_parent(res) {
                    rollup_child_count(unclipped, clipped, cell, parent);
                    hexes_at_res.push(parent);
                }
            });
        let density_tgt = get_res_tgt(res);
        for parent_hex in &hexes_at_res {
            let occupied_count = occupied_count(clipped, parent_hex, density_tgt);
            let limit = limit(res, occupied_count);
            if let Some(count) = unclipped.get(parent_hex) {
                let actual = cmp::min(limit, *count);
                clipped.insert(*parent_hex, actual);
            }
        }
    }
}

fn occupied_count(cell_map: &HexMap, hex: &H3Cell, density_tgt: u64) -> u64 {
    match hex.grid_disk(1) {
        Ok(k_ring) => {
            let mut count = 0;
            for cell in k_ring.into_iter() {
                if let Some(population) = cell_map.get(&cell) {
                    if *population >= density_tgt {
                        count += 1
                    }
                }
            }
            count
        }
        Err(_) => 0,
    }
}

fn limit(res: u8, occupied_count: u64) -> u64 {
    match HIP17_RES_CONFIG.get(&res) {
        Some(res_config) => {
            let occupied_neighbor_diff = occupied_count.saturating_sub(res_config.neighbors);
            let max = cmp::max((occupied_neighbor_diff) + 1, 1);
            cmp::min(res_config.max, res_config.target * max)
        }
        None => 1,
    }
}

pub fn compute_scaling_map(global_map: &GlobalHexMap, scaling_map: &mut ScalingMap) {
    for hex in &global_map.asserted_hexes {
        let mut scale: f64 = 1.0;
        for res in USED_RES.rev() {
            if let Ok(parent) = hex.get_parent(res) {
                if let Some(unclipped) = global_map.unclipped_hexes.get(&parent) {
                    if let Some(clipped) = global_map.clipped_hexes.get(&parent) {
                        scale *= *clipped as f64 / *unclipped as f64;
                    }
                }
            }
        }
        scaling_map.insert(&hex.h3index().to_string(), scale);
    }
}

fn get_res_tgt(res: u8) -> u64 {
    HIP17_RES_CONFIG
        .get(&res)
        .map(|config| config.target)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_scale_check() {
        let indexes: Vec<u64> = vec![
            631210990515645439,
            631210990515609087,
            631210990516667903,
            631210990528935935,
            631210990528385535,
            631210990528546815,
            631210990529462783,
            631210990529337343,
            631210990524024831,
            631210990524753919,
            631210990525267455,
        ];
        let mut gw_map = GlobalHexMap::new();
        for index in indexes {
            gw_map.increment_unclipped(index);
        }
        gw_map.reduce_global();
        let mut scale_map = ScalingMap::new();
        compute_scaling_map(&gw_map, &mut scale_map);
        println!("Result {scale_map:?}");
    }
}
