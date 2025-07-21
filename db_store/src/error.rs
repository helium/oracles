use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

pub mod aws_sts {
    pub use aws_sdk_sts::error::SdkError;
    pub use aws_sdk_sts::operation::assume_role::AssumeRoleError;
}

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
    AwsStsError(Box<aws_sts::SdkError<aws_sts::AssumeRoleError>>),
    #[error("Aws DateTime conversion error: {0}")]
    AwsDateTimeConversionError(Box<dyn std::error::Error + Send + Sync>),
    #[error("Assumed Credentials were invalid: {0}")]
    InvalidAssumedCredentials(String),
    #[error("Aws Signing Error")]
    SigningError(String),
    #[error("tokio join error")]
    JoinError(#[from] tokio::task::JoinError),
}

pub fn invalid_configuration(str: impl Into<String>) -> Error {
    Error::InvalidConfiguration(str.into())
}

impl From<aws_sts::SdkError<aws_sts::AssumeRoleError>> for Error {
    fn from(value: aws_sts::SdkError<aws_sts::AssumeRoleError>) -> Self {
        Self::AwsStsError(Box::new(value))
    }
}
