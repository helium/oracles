pub mod entropy;
pub mod entropy_loader;
pub mod gateway_cache;
pub mod gateway_updater;
pub mod hex_density;
pub mod last_beacon;
pub mod last_beacon_reciprocity;
pub mod last_witness;
pub mod loader;
pub mod meta;
pub mod packet_loader;
pub mod poc;
pub mod poc_report;
pub mod purger;
pub mod region_cache;
pub mod reward_share;
pub mod rewarder;
pub mod runner;
mod settings;
pub mod telemetry;
pub mod tx_scaler;
pub mod witness_updater;

use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
pub use settings::Settings;

pub const IOT_SUB_DAO_ONCHAIN_ADDRESS: &str = "Gm9xDCJawDEKDrrQW6haw94gABaYzQwCq4ZQU8h8bd22";

pub struct PriceConverter;

impl PriceConverter {
    pub fn hnt_bones_to_pricer_format(hnt_bone_price: Decimal) -> u64 {
        (hnt_bone_price * dec!(1_0000_0000) * dec!(1_0000_0000))
            .to_u64()
            .unwrap_or_default()
    }

    // Hnt prices are supplied from pricer in 10^8
    pub fn pricer_format_to_hnt_bones(hnt_price: u64) -> Decimal {
        Decimal::from(hnt_price) / dec!(1_0000_0000) / dec!(1_0000_0000)
    }
}
