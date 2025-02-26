use anyhow::Context;
use chrono::{Duration, Utc};
use file_store::file_sink::FileSinkClient;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::{burn::SolanaNetwork, GetSignature, Signature, SolanaRpcError};
use sqlx::PgPool;
use tracing::Instrument;

use crate::{pending_burns, pending_txns};

pub struct Burner<S> {
    valid_sessions: FileSinkClient<ValidDataTransferSession>,
    solana: S,
    failed_retry_attempts: usize,
    failed_check_interval: std::time::Duration,
}

impl<S> Burner<S> {
    pub fn new(
        valid_sessions: FileSinkClient<ValidDataTransferSession>,
        solana: S,
        failed_retry_attempts: usize,
        failed_check_interval: std::time::Duration,
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
    pub async fn confirm_and_burn(&self, pool: &PgPool) -> anyhow::Result<()> {
        self.confirm_pending_txns(pool)
            .await
            .context("confirming pending txns")?;
        self.burn(pool).await.context("burning")?;
        Ok(())
    }

    pub async fn confirm_pending_txns(&self, pool: &PgPool) -> anyhow::Result<()> {
        let pending_txns = pending_txns::fetch_all_pending_txns(pool).await?;
        tracing::info!(count = pending_txns.len(), "confirming pending txns");

        for pending in pending_txns {
            // Sleep for at least a minute since the time of submission to
            // give the transaction plenty of time to be finalized
            let time_since_submission = Utc::now() - pending.time_of_submission;
            if Duration::minutes(1) > time_since_submission {
                let delay = Duration::minutes(1) - time_since_submission;
                tracing::info!(?pending, %delay, "waiting to confirm pending txn");
                tokio::time::sleep(delay.to_std()?).await;
            }

            let signature = pending.signature;
            let confirmed = self.solana.confirm_transaction(&signature).await?;
            tracing::info!(?pending, confirmed, "confirming pending txn");

            if confirmed {
                let sessions =
                    pending_txns::get_pending_data_sessions_for_signature(pool, &signature).await?;
                for session in sessions {
                    let _write = self
                        .valid_sessions
                        .write(ValidDataTransferSession::from(session), &[])
                        .await?;
                }

                pending_txns::remove_pending_txn_success(pool, &signature).await?;
            } else {
                pending_txns::remove_pending_txn_failure(pool, &signature).await?;
            }
        }

        Ok(())
    }

    pub async fn burn(&self, pool: &PgPool) -> anyhow::Result<()> {
        let pending_txns = pending_txns::pending_txn_count(pool).await?;
        if pending_txns > 0 {
            tracing::error!(pending_txns, "ignoring burn");
            return Ok(());
        }

        for payer_pending_burn in pending_burns::get_all_payer_burns(pool).await? {
            let payer = payer_pending_burn.payer;
            let total_dcs = payer_pending_burn.total_dcs;
            let sessions = payer_pending_burn.sessions;

            let payer_balance = self.solana.payer_balance(&payer).await?;

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
            let txn = self.solana.make_burn_transaction(&payer, total_dcs).await?;
            pending_txns::add_pending_txn(pool, &payer, total_dcs, txn.get_signature())
                .await
                .context("adding pending txns and moving sessions")?;
            match self.solana.submit_transaction(&txn).await {
                Ok(()) => {
                    handle_transaction_success(
                        pool,
                        txn.get_signature(),
                        payer,
                        total_dcs,
                        sessions,
                        &self.valid_sessions,
                    )
                    .await?;
                }
                Err(err) => {
                    // NOTE: The next time we burn, pending txns will be
                    // checked. If this txn is not finalized by that time, th
                    // txn and pending data sessions will be moved back to the
                    // pending_burns table in `confirm_pending_txns()`
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
        pool: &PgPool,
        err: SolanaRpcError,
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
                        signature,
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
    pool: &PgPool,
    signature: &Signature,
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
    pending_txns::remove_pending_txn_success(pool, signature).await?;

    for session in sessions {
        valid_sessions
            .write(ValidDataTransferSession::from(session), &[])
            .await?;
    }

    Ok(())
}
