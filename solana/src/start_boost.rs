use anchor_client::{RequestBuilder, RequestNamespace};
use anchor_lang::{InstructionData, ToAccountMetas};
use async_trait::async_trait;
use file_store::hex_boost::BoostedHexActivation;
use helium_anchor_gen::hexboosting::{self, accounts, instruction};
use serde::Deserialize;
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_program::instruction::Instruction;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::{ParsePubkeyError, Pubkey},
    signature::{read_keypair_file, Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTimeError},
};

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error>;

    async fn submit_transaction(&self, transaction: &Self::Transaction) -> Result<(), Self::Error>;

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, Self::Error>;
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

macro_rules! send_with_retry {
    ($rpc:expr) => {{
        let mut attempt = 1;
        loop {
            match $rpc.await {
                Ok(resp) => break Ok(resp),
                Err(err) => {
                    if attempt < 5 {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_secs(attempt)).await;
                        continue;
                    } else {
                        break Err(err);
                    }
                }
            }
        }
    }};
}

#[derive(thiserror::Error, Debug)]
pub enum SolanaRpcError {
    #[error("Solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(Box<anchor_lang::error::Error>),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
    #[error("Parse pubkey error: {0}")]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Parse signature error: {0}")]
    ParseSignatureError(#[from] solana_sdk::signature::ParseSignatureError),
    #[error("Start authority does not match keypair")]
    InvalidKeypair,
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("Failed to read keypair file")]
    FailedToReadKeypairError,
    #[error("crypto error: {0}")]
    Crypto(#[from] helium_crypto::Error),
}

impl From<anchor_lang::error::Error> for SolanaRpcError {
    fn from(err: anchor_lang::error::Error) -> Self {
        Self::AnchorError(Box::new(err))
    }
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    rpc_url: String,
    cluster: String,
    start_authority_keypair: String,
}

pub struct SolanaRpc {
    provider: RpcClient,
    cluster: String,
    keypair: [u8; 64],
    start_authority: Pubkey,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>, SolanaRpcError> {
        let Ok(keypair) = read_keypair_file(&settings.start_authority_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError);
        };
        let provider =
            RpcClient::new_with_commitment(settings.rpc_url.clone(), CommitmentConfig::finalized());
        let start_authority = keypair.pubkey();
        Ok(Arc::new(Self {
            cluster: settings.cluster.clone(),
            provider,
            keypair: keypair.to_bytes(),
            start_authority,
        }))
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Error = SolanaRpcError;
    type Transaction = Transaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error> {
        let instructions = {
            let mut request = RequestBuilder::from(
                hexboosting::id(),
                &self.cluster,
                std::rc::Rc::new(Keypair::from_bytes(&self.keypair).unwrap()),
                Some(CommitmentConfig::confirmed()),
                RequestNamespace::Global,
            );
            for update in batch {
                let account = accounts::StartBoostV0 {
                    start_authority: self.start_authority,
                    boost_config: update.boost_config_pubkey.parse()?,
                    boosted_hex: update.boosted_hex_pubkey.parse()?,
                };
                let args = instruction::StartBoostV0 {
                    _args: hexboosting::StartBoostArgsV0 {
                        start_ts: update.activation_ts.timestamp(),
                    },
                };
                let instruction = Instruction {
                    program_id: hexboosting::id(),
                    accounts: account.to_account_metas(None),
                    data: args.data(),
                };
                request = request.instruction(instruction);
            }
            request.instructions().unwrap()
        };
        tracing::info!("instructions: {:?}", instructions);
        let blockhash = self.provider.get_latest_blockhash().await?;
        let signer = Keypair::from_bytes(&self.keypair).unwrap();

        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&signer.pubkey()),
            &[&signer],
            blockhash,
        ))
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), Self::Error> {
        match send_with_retry!(self.provider.send_and_confirm_transaction(tx)) {
            Ok(signature) => {
                tracing::info!(
                    transaction = %signature,
                    "hex start boost successful",
                );
                Ok(())
            }
            Err(err) => {
                let signature = tx.get_signature();
                tracing::error!(
                    transaction = %signature,
                    "hex start boost failed: {err:?}"
                );
                Err(SolanaRpcError::RpcClientError(err))
            }
        }
    }

    async fn confirm_transaction(&self, signature: &str) -> Result<bool, Self::Error> {
        let txn: Signature = signature.parse()?;
        Ok(matches!(
            self.provider
                .get_signature_status_with_commitment_and_history(
                    &txn,
                    CommitmentConfig::confirmed(),
                    true,
                )
                .await?,
            Some(Ok(()))
        ))
    }
}
pub enum PossibleTransaction {
    NoTransaction(Signature),
    Transaction(Transaction),
}

impl GetSignature for PossibleTransaction {
    fn get_signature(&self) -> &Signature {
        match self {
            Self::NoTransaction(ref sig) => sig,
            Self::Transaction(ref txn) => txn.get_signature(),
        }
    }
}

#[async_trait]
impl SolanaNetwork for Option<Arc<SolanaRpc>> {
    type Error = SolanaRpcError;
    type Transaction = PossibleTransaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error> {
        if let Some(ref rpc) = self {
            Ok(PossibleTransaction::Transaction(
                rpc.make_start_boost_transaction(batch).await?,
            ))
        } else {
            Ok(PossibleTransaction::NoTransaction(Signature::new_unique()))
        }
    }

    async fn submit_transaction(&self, transaction: &Self::Transaction) -> Result<(), Self::Error> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(ref txn)) => {
                rpc.submit_transaction(txn).await?
            }
            (None, PossibleTransaction::NoTransaction(_)) => (),
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.confirm_transaction(txn).await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }
}

// TODO: reinstate mock transaction
