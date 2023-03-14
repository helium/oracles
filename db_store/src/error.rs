use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Sql error")]
    SqlError(#[from] sqlx::Error),
    #[error("Failed to decode value")]
    DecodeError,
    #[error("meta key not found {0}")]
    NotFound(String),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Aws Assume Role Error")]
    AwsStsError(#[from] aws_sdk_sts::types::SdkError<aws_sdk_sts::error::AssumeRoleError>),
    #[error("Assumed Credentials were invalid: {0}")]
    InvalidAssumedCredentials(String),
    #[error("Aws Signing Error")]
    SigningError(#[from] aws_sig_auth::signer::SigningError),
    #[error("tokio join error")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("invalid auth token, does not start with http")]
    InvalidAuthToken(),
}

pub fn invalid_configuration(str: impl Into<String>) -> Error {
    Error::InvalidConfiguration(str.into())
}
