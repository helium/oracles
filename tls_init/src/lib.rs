pub use rustls;

#[ctor::ctor]
fn _install_tls_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs crypto provider");
}
