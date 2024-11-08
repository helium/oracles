use thiserror::Error;

#[derive(thiserror::Error, Debug)]
pub enum SolanaOrgError {
    #[error("Serialization error")]
    SerializationError,
    #[error("Program Derived Address (PDA) error")]
    PDAError,
}
