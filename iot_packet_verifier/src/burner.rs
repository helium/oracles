use crate::{
    balances::{Balance, Balances},
    pdas,
};
use anchor_client::RequestBuilder;
use data_credits::{accounts, instruction};
use helium_crypto::{PublicKey, PublicKeyBinary};
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, transaction::Transaction};
use sqlx::{FromRow, Pool, Postgres};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex,
};
use tokio::task;

pub struct Burner {
    pool: Pool<Postgres>,
    balances: Arc<Mutex<HashMap<PublicKey, Balance>>>,
    pending_burns: i64,
    provider: Arc<RpcClient>,
    burns: UnboundedReceiver<Burn>,
    program_cache: Option<BurnProgramCache>,
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError {
    #[error("Database error: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Solana client error: {0}")]
    SolanaClientError(#[from] ClientError),
}

const MAX_NUM_PENDING_BURNS: i64 = 10;

#[derive(FromRow)]
struct NewBurn {
    inserted: bool,
}

impl Burner {
    pub async fn new(
        pool: &Pool<Postgres>,
        provider: Arc<RpcClient>,
        balances: &Balances,
    ) -> Result<(Self, UnboundedSender<Burn>), BurnError> {
        let (sender, receiver) = unbounded_channel();

        // Fetch the current pending burns
        let pending_burns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_burns")
            .fetch_one(pool)
            .await?;

        Ok((
            Self {
                pool: pool.clone(),
                balances: balances.balances(),
                burns: receiver,
                program_cache: None,
                provider,
                pending_burns,
            },
            sender,
        ))
    }

    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<(), BurnError> {
        let burn_service = task::spawn(async move {
            loop {
                if self.pending_burns >= MAX_NUM_PENDING_BURNS {
                    self.burn().await?;
                }

                let Some(burn) = self.burns.recv().await else {
                    return Ok(());
                };

                // Add the pending burn to the burn table
                let NewBurn { inserted } = sqlx::query_as::<_, NewBurn>(
                    r#"
                    INSERT INTO pending_burns (gateway, amount)
                    VALUES ($1, $2)
                    ON CONFLICT (gateway) DO UPDATE SET
                    amount = pending_burns.amount + $2
                    RETURNING (xmax = 0) as inserted
                    "#,
                )
                .bind(burn.gateway)
                .bind(burn.amount)
                .fetch_one(&self.pool)
                .await?;

                if inserted {
                    self.pending_burns += 1;
                }
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

        if self.program_cache.is_none() {
            // Populate the program cache:
            self.program_cache = Some(BurnProgramCache::new());
        }

        // We would _like_ to have this streamed, but RequestBuilder does not implement Send
        // Everything past this point must be synchronous
        let pending_burns: Vec<Burn> = sqlx::query_as("SELECT * FROM pending_burns")
            .fetch_all(&self.pool)
            .await?;

        let instructions = {
            let program_cache = self.program_cache.as_ref().unwrap();
            let mut request = RequestBuilder::from(todo!(), todo!(), todo!(), todo!(), todo!());

            for Burn { gateway, amount } in pending_burns {
                let accounts = accounts::BurnDelegatedDataCreditsV0 {
                    sub_dao_epoch_info,
                    dao: program_cache.dao.clone(),
                    sub_dao: program_cache.sub_dao.clone(),
                    account_payer: program_cache.account_payer.clone(),
                    data_credits: program_cache.data_credits.clone(),
                    delegated_data_credits: pdas::delegated_data_credits(
                        &PublicKey::try_from(gateway).unwrap(),
                    ),
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

                request = request.accounts(accounts).args(args);
            }

            // As far as I can tell, the instructions does not actually have any
            // error paths.
            request.instructions().unwrap()
        };

        let blockhash = self.provider.get_latest_blockhash().await?;
        let signer: Keypair = todo!();

        let tx =
            Transaction::new_signed_with_payer(&instructions, Some(todo!()), &[&signer], blockhash);

        let signature = self.provider.send_and_confirm_transaction(&tx).await?;

        // TODO: We probably need to keep track of the last verified burn timestamp
        // and only truncate verified packets that come before that timestamp
        sqlx::query(
            r#"
            UPDATE meta SET value = (
                SELECT MAX(timestamp) FROM verified_packets
            ) WHERE key = 'last_burned_packet';
            TRUNCATE TABLE pending_burns;
            TRUNCATE TABLE verified_packets;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Clear all of the balances, requiring a refresh
        balances_lock.clear();

        Ok(())
    }
}

#[derive(FromRow, Debug)]
pub struct Burn {
    pub gateway: PublicKeyBinary,
    pub amount: i64,
}

impl Burn {
    pub fn new(gateway: PublicKey, amount: i64) -> Self {
        Self {
            gateway: PublicKeyBinary::from(gateway),
            amount,
        }
    }
}

/// Cached accounts and pubkeys for the burn program
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
