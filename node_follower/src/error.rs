use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("grpc {}", .0.message())]
    Grpc(#[from] tonic::Status),
    #[error("unsupported staking mode {0}")]
    StakingMode(String),
    #[error("unsupported region {0}")]
    Region(String),
}
