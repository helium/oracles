use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("socket address decode error {0}")]
    DecodeError(#[from] std::net::AddrParseError),
    #[error("metrics build error")]
    Metrics(#[from] metrics_exporter_prometheus::BuildError),
}
