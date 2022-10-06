use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("uri error")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("grpc {}", .0.message())]
    Grpc(#[from] tonic::Status),
    #[error("custom error")]
    Custom(String),
}

impl Error {
    pub fn custom<E: ToString>(msg: E) -> Self {
        Self::Custom(msg.to_string())
    }
}
