use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("environment error")]
    DotEnv(#[from] dotenv::Error),
}
