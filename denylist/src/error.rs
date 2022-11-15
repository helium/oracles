use thiserror::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("request error")]
    Request(#[from] reqwest::Error),
    #[error("unexpected binary")]
    InvalidBinary(String),
    #[error("unexpected value")]
    Value(serde_json::Value),
    #[error("invalid decimals in {0}, only 8 allowed")]
    Decimals(String),
    #[error("unexpected or invalid number {0}")]
    Number(String),
    #[error("crypto error")]
    Crypto(#[from] helium_crypto::Error),
    #[error("bincode error")]
    BinCode(#[from] bincode::Error),
    #[error("unexpected status {0}")]
    UnexpectedStatus(String),
    #[error("unable to parse metadata")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("config error")]
    Config(#[from] config::ConfigError),
}

impl Error {
    pub fn invalid_binary<E: ToString>(msg: E) -> Self {
        Self::InvalidBinary(msg.to_string())
    }

    pub fn value(value: serde_json::Value) -> Self {
        Self::Value(value)
    }

    pub fn decimals(value: &str) -> Self {
        Self::Decimals(value.to_string())
    }

    pub fn number(value: &str) -> Self {
        Self::Number(value.to_string())
    }
}
