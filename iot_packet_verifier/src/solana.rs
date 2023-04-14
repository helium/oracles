use crate::pdas;
use anchor_client::{RequestBuilder, RequestNamespace};
use anchor_lang::AccountDeserialize;
use async_trait::async_trait;
use data_credits::DelegatedDataCreditsV0;
use data_credits::{accounts, instruction};
use helium_crypto::PublicKeyBinary;
use helium_sub_daos::{DaoV0, SubDaoV0};
use solana_client::{
    client_error::ClientError, nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    program_pack::Pack,
    pubkey::{ParsePubkeyError, Pubkey},
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::{
    sync::Arc,
    time::{SystemTime, SystemTimeError},
};
use tokio::sync::Mutex;

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error>;

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum SolanaRpcError {
    #[error("Solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
    #[error("Parse pubkey error: {0}")]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Burn transaction {0} failed")]
    TransactionFailed(solana_sdk::signature::Signature),
    #[error("DC burn authority does not match keypair")]
    InvalidKeypair,
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
}

pub struct SolanaRpc {
    provider: RpcClient,
    program_cache: BurnProgramCache,
    cluster: String,
    keypair: [u8; 64],
}

impl SolanaRpc {
    pub async fn new(
        provider: RpcClient,
        cluster: String,
        keypair: Keypair,
        dc_mint: Pubkey,
        dnt_mint: Pubkey,
    ) -> Result<Arc<Self>, SolanaRpcError> {
        let program_cache = BurnProgramCache::new(&provider, dc_mint, dnt_mint).await?;
        if program_cache.dc_burn_authority != keypair.pubkey() {
            return Err(SolanaRpcError::InvalidKeypair);
        }
        Ok(Arc::new(Self {
            cluster,
            provider,
            program_cache,
            keypair: keypair.to_bytes(),
        }))
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Error = SolanaRpcError;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        let ddc_key = pdas::delegated_data_credits(&self.program_cache.sub_dao, payer);
        let account_data = self.provider.get_account_data(&ddc_key).await?;
        let mut account_data = account_data.as_ref();
        let ddc = DelegatedDataCreditsV0::try_deserialize(&mut account_data)?;
        let account_data = self.provider.get_account_data(&ddc.escrow_account).await?;
        let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;
        Ok(account_layout.amount)
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        tracing::info!("Burning {} DC from {}", amount, payer);

        // Fetch the sub dao epoch info:
        const EPOCH_LENGTH: u64 = 60 * 60 * 24;
        let epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs()
            / EPOCH_LENGTH;
        let (sub_dao_epoch_info, _) = Pubkey::find_program_address(
            &[
                "sub_dao_epoch_info".as_bytes(),
                self.program_cache.sub_dao.as_ref(),
                &epoch.to_le_bytes(),
            ],
            &helium_sub_daos::ID,
        );

        // Fetch escrow account
        let ddc_key = pdas::delegated_data_credits(&self.program_cache.sub_dao, payer);
        let account_data = self.provider.get_account_data(&ddc_key).await?;
        let mut account_data = account_data.as_ref();
        let escrow_account =
            DelegatedDataCreditsV0::try_deserialize(&mut account_data)?.escrow_account;

        let instructions = {
            let request = RequestBuilder::from(
                data_credits::id(),
                &self.cluster,
                std::rc::Rc::new(Keypair::from_bytes(&self.keypair).unwrap()),
                Some(CommitmentConfig::confirmed()),
                RequestNamespace::Global,
            );

            let accounts = accounts::BurnDelegatedDataCreditsV0 {
                sub_dao_epoch_info,
                dao: self.program_cache.dao,
                sub_dao: self.program_cache.sub_dao,
                account_payer: self.program_cache.account_payer,
                data_credits: self.program_cache.data_credits,
                delegated_data_credits: pdas::delegated_data_credits(
                    &self.program_cache.sub_dao,
                    payer,
                ),
                token_program: spl_token::id(),
                helium_sub_daos_program: helium_sub_daos::id(),
                system_program: solana_program::system_program::id(),
                dc_burn_authority: self.program_cache.dc_burn_authority,
                dc_mint: self.program_cache.dc_mint,
                escrow_account,
                registrar: self.program_cache.registrar,
            };
            let args = instruction::BurnDelegatedDataCreditsV0 {
                args: data_credits::BurnDelegatedDataCreditsArgsV0 { amount },
            };

            // As far as I can tell, the instructions function does not actually have any
            // error paths.
            request
                .accounts(accounts)
                .args(args)
                .instructions()
                .unwrap()
        };

        let blockhash = self.provider.get_latest_blockhash().await?;
        let signer = Keypair::from_bytes(&self.keypair).unwrap();

        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&signer.pubkey()),
            &[&signer],
            blockhash,
        );

        // Preflight can be flakey, so we skip it for now
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };
        let signature = self
            .provider
            .send_transaction_with_config(&tx, config)
            .await?;
        let result = self.provider.confirm_transaction(&signature).await?;

        if !result {
            return Err(SolanaRpcError::TransactionFailed(signature));
        }

        tracing::info!(
            "Successfully burned data credits. Transaction: {}",
            signature
        );

        Ok(())
    }
}

/// Cached pubkeys for the burn program
pub struct BurnProgramCache {
    pub account_payer: Pubkey,
    pub data_credits: Pubkey,
    pub sub_dao: Pubkey,
    pub dao: Pubkey,
    pub dc_mint: Pubkey,
    pub dc_burn_authority: Pubkey,
    pub registrar: Pubkey,
}

impl BurnProgramCache {
    pub async fn new(
        provider: &RpcClient,
        dc_mint: Pubkey,
        dnt_mint: Pubkey,
    ) -> Result<Self, SolanaRpcError> {
        let (account_payer, _) =
            Pubkey::find_program_address(&["account_payer".as_bytes()], &data_credits::ID);
        let (data_credits, _) =
            Pubkey::find_program_address(&["dc".as_bytes(), dc_mint.as_ref()], &data_credits::ID);
        let (sub_dao, _) = Pubkey::find_program_address(
            &["sub_dao".as_bytes(), dnt_mint.as_ref()],
            &helium_sub_daos::ID,
        );
        let (dao, dc_burn_authority) = {
            let account_data = provider.get_account_data(&sub_dao).await?;
            let mut account_data = account_data.as_ref();
            let sub_dao = SubDaoV0::try_deserialize(&mut account_data)?;
            (sub_dao.dao, sub_dao.dc_burn_authority)
        };
        let registrar = {
            let account_data = provider.get_account_data(&dao).await?;
            let mut account_data = account_data.as_ref();
            DaoV0::try_deserialize(&mut account_data)?.registrar
        };
        Ok(Self {
            account_payer,
            data_credits,
            sub_dao,
            dao,
            dc_mint,
            dc_burn_authority,
            registrar,
        })
    }
}

const FIXED_BALANCE: u64 = 1_000_000_000;

#[async_trait]
impl SolanaNetwork for Option<Arc<SolanaRpc>> {
    type Error = SolanaRpcError;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.payer_balance(payer).await
        } else {
            Ok(FIXED_BALANCE)
        }
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        if let Some(ref rpc) = self {
            rpc.burn_data_credits(payer, amount).await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl SolanaNetwork for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        Ok(*self.lock().await.get(payer).unwrap())
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        *self.lock().await.get_mut(payer).unwrap() -= amount;
        Ok(())
    }
}
