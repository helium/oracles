use std::{future::Future, time::Duration};

use file_store::file_sink::FileSinkClient;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::{burn::SolanaNetwork, GetSignature};
use sqlx::{Pool, Postgres};
use tracing::Instrument;

use crate::pending_burns;

pub struct Burner<S> {
    valid_sessions: FileSinkClient<ValidDataTransferSession>,
    solana: S,
    retry_attempts: usize,
    failed_check_interval: Duration,
}

impl<S> Burner<S> {
    pub fn new(
        valid_sessions: FileSinkClient<ValidDataTransferSession>,
        solana: S,
        retry_attempts: usize,
        failed_check_interval: Duration,
    ) -> Self {
        Self {
            valid_sessions,
            solana,
            retry_attempts,
            failed_check_interval,
        }
    }
}

impl<S> Burner<S>
where
    S: SolanaNetwork,
{
    pub async fn burn(&self, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        for payer_pending_burn in pending_burns::get_all_payer_burns(pool).await? {
            let payer = payer_pending_burn.payer;
            let total_dcs = payer_pending_burn.total_dcs;
            let sessions = payer_pending_burn.sessions;

            let payer_balance = self.solana.payer_balance(&payer).await?;

            if payer_balance < total_dcs {
                tracing::warn!(%payer, %payer_balance, %total_dcs, "Payer does not have enough balance to burn dcs");
                continue;
            }

            tracing::info!(%total_dcs, %payer, "Burning DC");
            if self.burn_data_credits(&payer, total_dcs).await.is_err() {
                // We have failed to burn data credits:
                metrics::counter!("burned", "payer" => payer.to_string(), "success" => "false")
                    .increment(total_dcs);
                continue;
            }

            // We succesfully managed to burn data credits:

            metrics::counter!("burned", "payer" => payer.to_string(), "success" => "true")
                .increment(total_dcs);

            // Delete from the data transfer session and write out to S3
            pending_burns::delete_for_payer(pool, &payer, total_dcs).await?;

            for session in sessions {
                self.valid_sessions
                    .write(ValidDataTransferSession::from(session), &[])
                    .await?;
            }
        }

        Ok(())
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), S::Error> {
        let txn = self.solana.make_burn_transaction(payer, amount).await?;
        self.solana.submit_transaction(&txn).await?;
        Ok(())
    }
}
