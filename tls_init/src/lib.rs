pub use rustls;

#[ctor::ctor]
fn _install_tls_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs crypto provider");
}

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
