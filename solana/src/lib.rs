use re::{
    solana_client::client_error::ClientError,
    solana_sdk::{program_error, pubkey::ParsePubkeyError, signature, transaction::Transaction},
};
use std::time::SystemTimeError;

pub use re::solana_sdk::signature::Signature;

pub mod re {

    pub use helium_lib;
    pub use helium_lib::anchor_client;
    pub use helium_lib::anchor_lang;
    pub use helium_lib::solana_client;
    pub use helium_lib::solana_program;
    pub use helium_lib::solana_sdk;
}

pub mod burn;
pub mod carrier;
pub mod start_boost;

macro_rules! send_with_retry {
    ($rpc:expr) => {{
        let mut attempt = 1;
        loop {
            match $rpc.await {
                Ok(resp) => break Ok(resp),
                Err(err) => {
                    if attempt < 5 {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_secs(attempt)).await;
                        continue;
                    } else {
                        break Err(err);
                    }
                }
            }
        }
    }};
}
pub(crate) use send_with_retry;

#[derive(thiserror::Error, Debug)]
pub enum SolanaRpcError {
    #[error("Solana rpc error: {0}")]
    RpcClientError(Box<ClientError>),
    #[error("Anchor error: {0}")]
    AnchorError(Box<re::anchor_lang::error::Error>),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] program_error::ProgramError),
    #[error("Parse pubkey error: {0}")]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Parse signature error: {0}")]
    ParseSignatureError(#[from] signature::ParseSignatureError),
    #[error("DC burn authority does not match keypair")]
    InvalidKeypair,
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("Failed to read keypair file: {0}")]
    FailedToReadKeypairError(String),
    #[error("crypto error: {0}")]
    Crypto(#[from] helium_crypto::Error),
    // TODO: Remove when fully integrated with helium-lib
    #[error("Test Error")]
    Test(String),
}

impl From<re::anchor_lang::error::Error> for SolanaRpcError {
    fn from(err: re::anchor_lang::error::Error) -> Self {
        Self::AnchorError(Box::new(err))
    }
}

impl From<ClientError> for SolanaRpcError {
    fn from(err: ClientError) -> Self {
        Self::RpcClientError(Box::new(err))
    }
}

pub trait GetSignature {
    fn get_signature(&self) -> &Signature;
}

impl GetSignature for Transaction {
    fn get_signature(&self) -> &Signature {
        &self.signatures[0]
    }
}

impl GetSignature for Signature {
    fn get_signature(&self) -> &Signature {
        self
    }
}
