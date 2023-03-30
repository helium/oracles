use crate::{
    balances::{BalanceCache, BalanceStore},
    pdas,
    settings::Settings,
};
use anchor_client::{RequestBuilder, RequestNamespace};
use anchor_lang::AccountDeserialize;
use chrono::Utc;
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
    pubkey::{ParsePubkeyError, Pubkey},
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use sqlx::{FromRow, Pool, Postgres};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, SystemTimeError},
};
use tokio::task;

pub struct Burner {
    pool: Pool<Postgres>,
    balances: BalanceStore,
    provider: Arc<RpcClient>,
    program_cache: BurnProgramCache,
    cluster: String,
    // We store the keypair as bytes since the type does not implement clone (for some reason).
    keypair: [u8; 64],
    burn_period: Duration,
    enable_dc_burn: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError {
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Solana client error: {0}")]
    SolanaClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("Parse pubkey error: {0}")]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Burn transaction {0} failed")]
    TransactionFailed(solana_sdk::signature::Signature),
    #[error("DC burn authority does not match keypair")]
    InvalidKeypair,
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
}

const BURN_THRESHOLD: i64 = 10_000;

impl Burner {
    pub async fn new(
        settings: &Settings,
        pool: &Pool<Postgres>,
        balances: &BalanceCache,
        provider: Arc<RpcClient>,
        keypair: Keypair,
    ) -> Result<Self, BurnError> {
        let program_cache = BurnProgramCache::new(settings, provider.as_ref()).await?;
        if program_cache.dc_burn_authority != keypair.pubkey() {
            return Err(BurnError::InvalidKeypair);
        }
        Ok(Self {
            pool: pool.clone(),
            balances: balances.balances(),
            program_cache,
            cluster: settings.cluster.clone(),
            provider,
            keypair: keypair.to_bytes(),
            burn_period: Duration::from_secs(60 * settings.burn_period),
            enable_dc_burn: settings.enable_dc_burn,
        })
    }

    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<(), BurnError> {
        let burn_service = task::spawn(async move {
            loop {
                self.burn().await?;
                tokio::time::sleep(self.burn_period).await;
            }
        });

        tokio::select! {
            _ = shutdown.clone() => Ok(()),
            service_result = burn_service => service_result?,
        }
    }

    pub async fn burn(&mut self) -> Result<(), BurnError> {
        // Create burn transaction and execute it:

        let Some(Burn { payer, amount, id }): Option<Burn> =
            sqlx::query_as("SELECT * FROM pending_burns WHERE amount >= $1 ORDER BY last_burn ASC")
                .bind(BURN_THRESHOLD)
                .fetch_optional(&self.pool)
            .await? else {
                return Ok(());
            };

        if self.enable_dc_burn {
            self.burn_data_credits(&payer, amount).await?;
        }

        // Now that we have successfully executed the burn and are no long in
        // sync land, we can remove the amount burned.
        sqlx::query(
            r#"
            UPDATE pending_burns SET
              amount = amount - $1,
              last_burn = $2
            WHERE id = $3
            "#,
        )
        .bind(amount)
        .bind(Utc::now().naive_utc())
        .bind(id)
        .execute(&self.pool)
        .await?;

        let mut balance_lock = self.balances.lock().await;
        let balances = balance_lock.get_mut(&payer).unwrap();
        balances.burned -= amount as u64;
        // Zero the balance in order to force a reset:
        balances.balance = 0;

        Ok(())
    }

    async fn burn_data_credits(
        &mut self,
        payer: &PublicKeyBinary,
        amount: i64,
    ) -> Result<(), BurnError> {
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
                args: data_credits::BurnDelegatedDataCreditsArgsV0 {
                    amount: amount as u64,
                },
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
            return Err(BurnError::TransactionFailed(signature));
        }

        tracing::info!(
            "Successfully burned data credits. Transaction: {}",
            signature
        );

        Ok(())
    }
}

#[derive(FromRow, Debug)]
pub struct Burn {
    pub id: i32,
    pub payer: PublicKeyBinary,
    pub amount: i64,
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
    pub async fn new(settings: &Settings, provider: &RpcClient) -> Result<Self, BurnError> {
        let (account_payer, _) =
            Pubkey::find_program_address(&["account_payer".as_bytes()], &data_credits::ID);
        let (data_credits, _) = Pubkey::find_program_address(
            &["dc".as_bytes(), settings.dc_mint()?.as_ref()],
            &data_credits::ID,
        );
        let (sub_dao, _) = Pubkey::find_program_address(
            &["sub_dao".as_bytes(), settings.dnt_mint()?.as_ref()],
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
            dc_mint: settings.dc_mint()?,
            dc_burn_authority,
            registrar,
        })
    }
}
