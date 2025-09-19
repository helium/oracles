//! ### TLS
//! Updates to `rustls` now support multiple CryptoProvider backends. Namely
//! `aws-lc-rs` (default) and `ring` (legacy).
//!
//! Until more crates have updated to default to `aws-lc-rs` we need to ensure
//! that a single CryptoProvider backend is installed. Because of our heavy
//! reliance on aws crates, we've chosen `aws-lc-rs`. Also because `rustls` has
//! recently chosen that as it's default.
//!
//! If you're crate needs to use tls, add the following.
//! // Cargo.toml
//! tls-init = { path = "../tls_init" }
//!
//! // lib.rs
//! `extern crate tls_init;`
//!
//! `tls_init` uses `ctor` to run a constructor when the code is loaded a single
//! time. We need to `extern` the crate to prevent the linker from optimizing the
//! codepath away because there are no direct calls into the workspace.

pub use rustls;

#[ctor::ctor]
fn _install_tls_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs crypto provider");
}

/// Include the following macro in your src/lib.rs to hopefully prevent
/// transient dependencies from installing another CryptoProvider backend.
///
/// #[cfg(test)]
/// tls_init::include_tls_tests!();
#[macro_export]
macro_rules! include_tls_tests {
    () => {
        mod tls_init_tests {
            #[test]
            fn rustls_provider_is_set() {
                let provider = $crate::rustls::crypto::CryptoProvider::get_default();

                assert!(
                    provider.is_some(),
                    "No default Crypto Provider set (likely no or multiple providers)"
                );
                if let Some(p) = provider {
                    assert_eq!(format!("{:?}", p.key_provider), "AwsLcRs");
                }
            }
        }
    };
}
