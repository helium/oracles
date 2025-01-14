use solana_client::client_error::ClientError;
use solana_sdk::pubkey::ParsePubkeyError;
use solana_sdk::transaction::Transaction;
use std::{fs::File, io::Read, path::Path, time::SystemTimeError};

pub use helium_lib::{
    dao::SubDao,
    error,
    keypair::{Keypair, Pubkey},
    send_txn, Signature, TransactionWithBlockhash,
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

pub fn read_keypair_from_file<F: AsRef<Path>>(path: F) -> anyhow::Result<Keypair> {
    let mut file = File::open(path.as_ref())?;
    let mut sk_buf = [0u8; 64];
    file.read_exact(&mut sk_buf)?;
    Ok(Keypair::try_from(&sk_buf)?)
}

#[derive(thiserror::Error, Debug)]
pub enum SolanaRpcError {
    #[error("Solana rpc error: {0}")]
    RpcClientError(Box<ClientError>),
    #[error("Anchor error: {0}")]
    AnchorError(Box<helium_anchor_gen::anchor_lang::error::Error>),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
    #[error("Parse pubkey error: {0}")]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Parse signature error: {0}")]
    ParseSignatureError(#[from] solana_sdk::signature::ParseSignatureError),
    #[error("DC burn authority does not match keypair")]
    InvalidKeypair,
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("Failed to read keypair file: {0}")]
    FailedToReadKeypairError(String),
    #[error("crypto error: {0}")]
    Crypto(#[from] helium_crypto::Error),
    #[error("helium-lib error: {0}")]
    HeliumLib(#[from] helium_lib::error::Error),
    #[error("helium-lib txn send error: {0}")]
    HeliumLibTxnSend(#[from] helium_lib::send_txn::TxnSenderError),
    #[error("Parse Solana Pubkey from slice error: {0}")]
    ParsePubkeyFromSliceError(#[from] std::array::TryFromSliceError),
}

impl From<helium_anchor_gen::anchor_lang::error::Error> for SolanaRpcError {
    fn from(err: helium_anchor_gen::anchor_lang::error::Error) -> Self {
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

impl GetSignature for TransactionWithBlockhash {
    fn get_signature(&self) -> &Signature {
        self.get_signature()
    }
}
