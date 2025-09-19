extern crate tls_init;

pub mod server_iot;
pub mod server_mobile;
pub mod settings;

pub use settings::{Mode, Settings};

#[cfg(test)]
tls_init::include_tls_tests!();
