use std::time::Duration;

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
    failed_retry_attempts: usize,
    failed_check_interval: Duration,
}

impl<S> Burner<S> {
    pub fn new(
        valid_sessions: FileSinkClient<ValidDataTransferSession>,
        solana: S,
        failed_retry_attempts: usize,
        failed_check_interval: Duration,
    ) -> Self {
        Self {
            valid_sessions,
            solana,
            failed_retry_attempts,
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

            let payer_balance = self.solana.payer_balance(&payer.to_string()).await?;

            if payer_balance < total_dcs {
                tracing::warn!(
                    %payer,
                    %payer_balance,
                    %total_dcs,
                    "Payer does not have enough balance to burn dcs"
                );
                continue;
            }

            tracing::info!(%total_dcs, %payer, "Burning DC");
            let txn = self
                .solana
                .make_burn_transaction(&payer.to_string(), total_dcs)
                .await?;
            match self.solana.submit_transaction(&txn).await {
                Ok(()) => {
                    handle_transaction_success(
                        pool,
                        payer,
                        total_dcs,
                        sessions,
                        &self.valid_sessions,
                    )
                    .await?;
                }
                Err(err) => {
                    let span = tracing::info_span!(
                        "txn_confirmation",
                        signature = %txn.get_signature(),
                        %payer,
                        total_dcs,
                        max_attempts = self.failed_retry_attempts
                    );

                    // block on confirmation
                    self.transaction_confirmation_check(pool, err, txn, payer, total_dcs, sessions)
                        .instrument(span)
                        .await;
                }
            }
        }

        Ok(())
    }

    async fn transaction_confirmation_check(
        &self,
        pool: &Pool<Postgres>,
        err: S::Error,
        txn: S::Transaction,
        payer: PublicKeyBinary,
        total_dcs: u64,
        sessions: Vec<pending_burns::DataTransferSession>,
    ) {
        tracing::warn!(?err, "starting txn confirmation check");
        // We don't know if the txn actually made it, maybe it did

        let signature = txn.get_signature();

        let mut attempt = 0;
        while attempt <= self.failed_retry_attempts {
            tokio::time::sleep(self.failed_check_interval).await;
            match self.solana.confirm_transaction(signature).await {
                Ok(true) => {
                    tracing::debug!("txn confirmed on chain");
                    let txn_success = handle_transaction_success(
                        pool,
                        payer,
                        total_dcs,
                        sessions,
                        &self.valid_sessions,
                    )
                    .await;
                    if let Err(err) = txn_success {
                        tracing::error!(?err, "txn succeeded, something else failed");
                    }

                    return;
                }
                Ok(false) => {
                    tracing::info!(attempt, "txn not confirmed, yet...");
                    attempt += 1;
                    continue;
                }
                Err(err) => {
                    // Client errors do not count against retry attempts
                    tracing::error!(?err, attempt, "failed to confirm txn");
                    continue;
                }
            }
        }

        tracing::warn!("failed to confirm txn");

        // We have failed to burn data credits:
        metrics::counter!(
            "burned",
            "payer" => payer.to_string(),
            "success" => "false"
        )
        .increment(total_dcs);
    }
}

async fn handle_transaction_success(
    pool: &Pool<Postgres>,
    payer: PublicKeyBinary,
    total_dcs: u64,
    sessions: Vec<pending_burns::DataTransferSession>,
    valid_sessions: &FileSinkClient<ValidDataTransferSession>,
) -> Result<(), anyhow::Error> {
    // We succesfully managed to burn data credits:
    metrics::counter!(
        "burned",
        "payer" => payer.to_string(),
        "success" => "true"
    )
    .increment(total_dcs);

    // Delete from the data transfer session and write out to S3
    pending_burns::delete_for_payer(pool, &payer, total_dcs).await?;

    for session in sessions {
        valid_sessions
            .write(ValidDataTransferSession::from(session), &[])
            .await?;
    }

    Ok(())
}
