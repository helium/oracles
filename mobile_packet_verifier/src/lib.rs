extern crate tls_init;

pub mod accumulate;
pub mod banning;
pub mod burner;
pub mod daemon;
pub mod event_ids;
pub mod gateway;
pub mod iceberg;
pub mod pending_burns;
pub mod pending_txns;
pub mod routing;
pub mod settings;

const BYTES_PER_DC: u64 = 100_000;

pub fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    bytes.div_ceil(BYTES_PER_DC)
}

pub fn dc_to_bytes(dcs: u64) -> u64 {
    dcs * BYTES_PER_DC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_dc() {
        assert_eq!(1, bytes_to_dc(1));
        assert_eq!(1, bytes_to_dc(100_000));
        assert_eq!(2, bytes_to_dc(100_001));
    }
}

#[cfg(test)]
tls_init::include_tls_tests!();
