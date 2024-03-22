use crate::{send_with_retry, GetSignature, SolanaRpcError};
use anchor_client::RequestBuilder;
use async_trait::async_trait;
use file_store::hex_boost::BoostedHexActivation;
use helium_anchor_gen::{
    anchor_lang::{InstructionData, ToAccountMetas},
    hexboosting::{self, accounts, instruction},
};
use serde::Deserialize;
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_program::instruction::Instruction;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::sync::Arc;

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error>;

    async fn sign_transaction(
        &self,
        transaction: &Self::Transaction,
    ) -> Result<Self::Transaction, Self::Error>;

    async fn submit_transaction(&self, transaction: &Self::Transaction) -> Result<(), Self::Error>;

    async fn confirm_transaction(&self, txn: &str) -> Result<bool, Self::Error>;

    async fn check_for_blockhash_not_found_error(&self, err: &Self::Error) -> bool;
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
            let rt_handle = tokio::runtime::Handle::current();
            let mut request = RequestBuilder::from(
                hexboosting::id(),
                &self.cluster,
                std::rc::Rc::new(Keypair::from_bytes(&self.keypair).unwrap()),
                Some(CommitmentConfig::confirmed()),
                &rt_handle,
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
        tracing::debug!("instructions: {:?}", instructions);
        let signer = Keypair::from_bytes(&self.keypair).unwrap();
        Ok(Transaction::new_with_payer(
            &instructions,
            Some(&signer.pubkey()),
        ))
    }

    async fn sign_transaction(
        &self,
        txn: &Self::Transaction,
    ) -> Result<Self::Transaction, Self::Error> {
        let (blockhash, _) = self
            .provider
            .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
            .await?;
        let signer = Keypair::from_bytes(&self.keypair).unwrap();
        let mut signed_txn = txn.clone();
        signed_txn.sign(&[&signer], blockhash);
        Ok(signed_txn)
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), Self::Error> {
        match self.provider.send_and_confirm_transaction(tx).await {
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

    async fn check_for_blockhash_not_found_error(&self, err: &Self::Error) -> bool {
        matches!(
            err,
            SolanaRpcError::RpcClientError(ClientError {
                kind: solana_client::client_error::ClientErrorKind::TransactionError(
                    solana_sdk::transaction::TransactionError::BlockhashNotFound,
                ),
                ..
            })
        )
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

    async fn sign_transaction(
        &self,
        transaction: &Self::Transaction,
    ) -> Result<Self::Transaction, Self::Error> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(txn)) => {
                let signed_txn = rpc.sign_transaction(txn).await?;
                Ok(PossibleTransaction::Transaction(signed_txn))
            }
            (None, PossibleTransaction::NoTransaction(_)) => {
                panic!("We will not confirm transactions when Solana is disabled")
            }
            _ => unreachable!(),
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

    async fn check_for_blockhash_not_found_error(&self, err: &Self::Error) -> bool {
        if let Some(ref rpc) = self {
            rpc.check_for_blockhash_not_found_error(err).await
        } else {
            false
        }
    }
}
