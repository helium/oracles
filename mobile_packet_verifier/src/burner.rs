use chrono::{DateTime, Utc};
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::{ParsePubkeyError, Pubkey},
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use sqlx::{FromRow, Postgres, Transaction};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(FromRow)]
pub struct DataTransferSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: u64,
    downloaded_bytes: u64,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

impl From<DataTransferSession> for ValidDataTransferSession {
    fn from(ds: DataTransferSession) -> Self {
        Self {
            pub_key: ds.pub_key.into(),
            payer: ds.payer.into(),
            uploaded_bytes: ds.uploaded_bytes,
            downloaded_bytes: ds.downloaded_bytes,
            first_timestamp: ds.first_timestamp.encode_timestamp_millis(),
            last_timestamp: ds.last_timestamp.encode_timestamp_millis(),
        }
    }
}

pub struct PayerTotals {
    total_bytes: u64,
    sessions: Vec<DataTransferSession>,
}

impl PayerTotals {
    fn push_sess(&mut self, sess: DataTransferSession) {
        self.total_bytes += sess.downloaded_bytes + sess.uploaded_bytes;
        self.sessions.push(sess);
    }
}

impl Default for Payer {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            sessions: Vec::new(),
        }
    }
}

pub struct Burner {
    pool: Postgres<Pool>,
    valid_sessions: FileSinkClient,
    provider: RpcClient,
    program_cache: BurnProgramCache,
    keypair: [u8; 64],
    db_lock: Arc<Mutex<()>>,
    burn_period: Duration,
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError {
    #[error("Solana client error: {0}")]
    SolanaClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("sql error: {0}")]
    SqlError(#[from] sqlx::Error),
}

impl Burner {
    pub async fn new(
        settings: &Settings,
        pool: Pool<Postgres>,
        valid_sessions: FileSinkClient,
        db_lock: Arc<Mutex<()>>,
        provider: RpcClient,
        keypair: Keypair,
    ) -> Result<Self, BurnError> {
        Ok(Self {
            pool,
            db_lock,
            burn_period: Duration::from_secs(60 * 60 * settings.burn_period),
            program_cache: BurnProgramCache::new(settings, &provider).await?,
            provider,
            valid_sessions,
            db_lock,
            keypair: keypair.to_bytes(),
        })
    }
    
    async fn run(self, shutdown: &tiggered::Listener) -> Result<(), BurnError> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => return Ok(()),
                _ = tokio::sleep(self.burn_period) => self.burn().await?,
            }
        }
    }

    async fn burn(&self) -> Result<(), BurnError> {
        // Prevent any use of the database by the verifier until after we've finished
        let _db_lock = self.db_lock.lock().await;

        // Fetch all of the sessions
        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * FROM data_transfer_sessions").fetch_all(&mut transaction);

        // Fetch all of the sessions and group by the payer
        let mut payers = HashMap::<PublicKeyBinary, PayerTotals>::new();
        for session in sessions.into_iter() {
            payer_totals
                .entry(session.payer.clone())
                .or_default()
                .push_sess(session);
        }

        for (
            payer,
            PayerTotals {
                total_bytes,
                sessions,
            },
        ) in payers.into_iter()
        {
            let amount = bytes_to_dc(total_bytes);

            // Burn the DC for the payer 
            let ddc_key = pdas::delegated_data_credits(&self.program_cache.sub_dao, &payer);
            let account_data = self.provider.get_account_data(&ddc_key).await?;
            let mut account_data = account_data.as_ref();
            let escrow_account =
                DelegatedDataCreditsV0::try_deserialize(&mut account_data)?.escrow_account;

            tracing::info!("Burning {} DC from {}", amount, payer);

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
                        &payer,
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

            let signature = self.provider.send_and_confirm_transaction(&tx).await?;
            tracing::info!(
                "Successfully burned data credits. Transaction: {}",
                signature
            );

            // Delete from the data transfer session and write out to S3

            sqlx::query("DELETE FROM data_tranfer_sessions WHERE payer = $1")
                .bind(payer)
                .execute(&self.pool)
                .await?;

            for session in sessions {
                self.valid_sessions.write(session.into(), &[]);
            }
        }

        Ok(())
    }
}

const BYTES_PER_DC: u64 = 20_000;

fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    (bytes + BYTES_PER_DC - 1) / BYTES_PER_DC
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
