use crate::{
    balances::{Balance, Balances},
    pdas,
};
use anchor_client::{RequestBuilder, RequestNamespace};
use data_credits::{accounts, instruction};
use helium_crypto::{PublicKey, PublicKeyBinary};
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Keypair, signer::Signer,
    transaction::Transaction,
};
use sqlx::{FromRow, Pool, Postgres};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::task;

pub struct Burner {
    pool: Pool<Postgres>,
    balances: Arc<Mutex<HashMap<PublicKey, Balance>>>,
    provider: Arc<RpcClient>,
    program_cache: BurnProgramCache,
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError {
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Solana client error: {0}")]
    SolanaClientError(#[from] ClientError),
}

impl Burner {
    pub fn new(
        pool: &Pool<Postgres>,
        provider: Arc<RpcClient>,
        balances: &Balances,
    ) -> Self {
        Self {
            pool: pool.clone(),
            balances: balances.balances(),
            program_cache: BurnProgramCache::new(),
            provider,
        }
    }

    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<(), BurnError> {
        let burn_service = task::spawn(async move {
            loop {
                self.burn().await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        tokio::select! {
            _ = shutdown.clone() => Ok(()),
            service_result = burn_service => service_result?,
        }
    }

    pub async fn burn(&mut self) -> Result<(), BurnError> {
        // Do not update any balances until we have successfully burned
        // the accounts
        let balances_lock = self.balances.lock().await;

        // Create burn transaction and execute it:

        // Fetch the sub dao epoch info:
        let epoch = self.provider.get_epoch_info().await?.epoch;
        let (sub_dao_epoch_info, _) = Pubkey::find_program_address(
            &[
                "sub_dao_epoch_info".as_bytes(),
                crate::SUB_DAO.as_ref(),
                &epoch.to_le_bytes(),
            ],
            &helium_sub_daos::ID,
        );

        let Some(Burn { gateway, amount, id }): Option<Burn> =
            sqlx::query_as("SELECT * FROM pending_burns ORDER BY RAND ()")
                .fetch_optional(&self.pool)
            .await? else {
                return Ok(());
            };

        let instructions = {
            let payer: std::rc::Rc<dyn Signer> = todo!();
            let request = RequestBuilder::from(
                data_credits::id(),
                "devnet",
                payer,
                Some(CommitmentConfig::confirmed()),
                RequestNamespace::Global,
            );

            let gateway = PublicKey::try_from(gateway.clone()).unwrap();
            let accounts = accounts::BurnDelegatedDataCreditsV0 {
                sub_dao_epoch_info,
                dao: self.program_cache.dao.clone(),
                sub_dao: self.program_cache.sub_dao.clone(),
                account_payer: self.program_cache.account_payer.clone(),
                data_credits: self.program_cache.data_credits.clone(),
                delegated_data_credits: pdas::delegated_data_credits(&gateway),
                token_program: spl_token::id(),
                helium_sub_daos_program: helium_sub_daos::id(),
                system_program: solana_program::system_program::id(),

                // Fields that I do not know how to populate:
                dc_burn_authority: todo!(),
                dc_mint: todo!(),
                escrow_account: todo!(),
                registrar: todo!(),
            };
            let args = instruction::BurnDelegatedDataCreditsV0 {
                args: data_credits::BurnDelegatedDataCreditsArgsV0 {
                    amount: amount as u64,
                },
            };

            // Remove the entry from the balance sheet
            balances_lock.remove(&gateway);

            // As far as I can tell, the instructions does not actually have any
            // error paths.
            request
                .accounts(accounts)
                .args(args)
                .instructions()
                .unwrap()
        };

        let blockhash = self.provider.get_latest_blockhash().await?;
        let signer: Keypair = todo!();

        let tx =
            Transaction::new_signed_with_payer(&instructions, Some(todo!()), &[&signer], blockhash);

        let signature = self.provider.send_and_confirm_transaction(&tx).await?;

        // Now that we have successfully executed the burn and are no long in
        // sync land, we can delete the entries
        sqlx::query("DELETE FROM pending_burns WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[derive(FromRow, Debug)]
pub struct Burn {
    pub id: i32,
    pub gateway: PublicKeyBinary,
    pub amount: i64,
}

/// Cached pubkeys for the burn program
pub struct BurnProgramCache {
    pub account_payer: Pubkey,
    pub data_credits: Pubkey,
    pub sub_dao: Pubkey,
    pub dao: Pubkey,
}

impl BurnProgramCache {
    pub fn new() -> Self {
        let (account_payer, _) =
            Pubkey::find_program_address(&["account_payer".as_bytes()], &data_credits::ID);
        let (data_credits, _) = Pubkey::find_program_address(
            &["dc".as_bytes(), crate::DC_MINT.as_ref()],
            &data_credits::ID,
        );
        let (sub_dao, _) = Pubkey::find_program_address(
            &["sub_dao".as_bytes(), crate::DNT_MINT.as_ref()],
            &helium_sub_daos::ID,
        );
        let (dao, _) = Pubkey::find_program_address(
            &["dao".as_bytes(), crate::HNT_MINT.as_ref()],
            &helium_sub_daos::ID,
        );
        Self {
            account_payer,
            data_credits,
            sub_dao,
            dao,
        }
    }
}
