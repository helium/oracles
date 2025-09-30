extern crate tls_init;

use helium_lib::{
    solana_client::client_error::ClientError,
    solana_sdk::{
        program_error,
        pubkey::ParsePubkeyError,
        signature::{self, read_keypair},
        signer::keypair::Keypair,
        transaction::Transaction,
    },
};
use serde::Deserialize;
use std::time::SystemTimeError;

pub use helium_lib::{
    self,
    dao::SubDao,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{pubkey::Pubkey as SolPubkey, signature::Signature},
    token::{self, Token},
};

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
    AnchorError(Box<helium_lib::anchor_lang::error::Error>),
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

impl From<helium_lib::anchor_lang::error::Error> for SolanaRpcError {
    fn from(err: helium_lib::anchor_lang::error::Error) -> Self {
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

pub fn deserialize_solana_keypair<'a, D>(deserializer: D) -> Result<Keypair, D::Error>
where
    D: serde::Deserializer<'a>,
{
    let s = String::deserialize(deserializer)?;
    let keypair = read_keypair(&mut s.as_bytes()).map_err(serde::de::Error::custom)?;
    Ok(keypair)
}

#[cfg(test)]
tls_init::include_tls_tests!();
