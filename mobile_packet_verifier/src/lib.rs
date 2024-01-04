pub mod accumulate;
pub mod burner;
pub mod daemon;
pub mod event_ids;
pub mod settings;

const BYTES_PER_DC: u64 = 20_000;

pub fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    // Integer div/ceil from: https://stackoverflow.com/a/2745086
    (bytes + BYTES_PER_DC - 1) / BYTES_PER_DC
}
