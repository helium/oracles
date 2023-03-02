#[derive(thiserror::Error, Debug)]
pub enum PriceError {
    #[error("unsupported token type: {0}")]
    UnsupportedTokenType(i32),
    #[error("unable to fetch price")]
    UnableToFetch,
    #[error("unknown price account key, token_type: {0}")]
    UnknownKey(String),
}
