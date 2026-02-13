use anyhow::Context;
use chrono::{Duration, Utc};
use file_store::file_sink::FileSinkClient;
use file_store_oracles::mobile_transfer::ValidDataTransferSession;
use helium_crypto::PublicKeyBinary;
use helium_iceberg::BoxedDataWriter;
use solana::{burn::SolanaNetwork, GetSignature, Signature, SolanaRpcError};
use sqlx::PgPool;
use tracing::Instrument;

mod proto {
    pub use helium_proto::services::packet_verifier::ValidDataTransferSession;
}

use crate::{
    iceberg::{self, burned_data_transfer::TrinoBurnedDataTransferSession},
    pending_burns, pending_txns,
};

pub struct Burner<S> {
    valid_sessions: FileSinkClient<proto::ValidDataTransferSession>,
    solana: S,
    failed_retry_attempts: usize,
    failed_check_interval: std::time::Duration,
    data_writer: Option<BoxedDataWriter<TrinoBurnedDataTransferSession>>,
}

impl<S> Burner<S> {
    pub fn new(
        valid_sessions: FileSinkClient<proto::ValidDataTransferSession>,
        solana: S,
        failed_retry_attempts: usize,
        failed_check_interval: std::time::Duration,
        data_writer: Option<BoxedDataWriter<TrinoBurnedDataTransferSession>>,
    ) -> Self {
        Self {
            valid_sessions,
            solana,
            failed_retry_attempts,
            failed_check_interval,
            data_writer,
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

                write_burned_data_transfer_sessions(
                    sessions,
                    &self.valid_sessions,
                    self.data_writer.as_ref(),
                )
                .await?;

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
            let total_dcs = payer_pending_burn.total_dcs;
            let payer = payer_pending_burn.payer;
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
                        self.data_writer.as_ref(),
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
                        self.data_writer.as_ref(),
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
    valid_sessions: &FileSinkClient<proto::ValidDataTransferSession>,
    data_writer: Option<&BoxedDataWriter<TrinoBurnedDataTransferSession>>,
) -> Result<(), anyhow::Error> {
    // We successfully managed to burn data credits:
    metrics::counter!(
        "burned",
        "payer" => payer.to_string(),
        "success" => "true"
    )
    .increment(total_dcs);

    // Delete from the data transfer session and write out to S3
    pending_burns::delete_for_payer(pool, &payer).await?;
    pending_burns::decrement_metric(&payer, total_dcs);
    pending_txns::remove_pending_txn_success(pool, signature).await?;

    write_burned_data_transfer_sessions(sessions, valid_sessions, data_writer).await?;

    Ok(())
}

async fn write_burned_data_transfer_sessions(
    sessions: Vec<pending_burns::DataTransferSession>,
    file_sink: &FileSinkClient<proto::ValidDataTransferSession>,
    iceberg_sink: Option<&BoxedDataWriter<TrinoBurnedDataTransferSession>>,
) -> anyhow::Result<()> {
    let sessions = sessions
        .into_iter()
        .map(ValidDataTransferSession::from)
        .collect::<Vec<_>>();

    file_sink.write_all(sessions.clone()).await?;

    if let Some(writer) = iceberg_sink {
        let sessions = sessions
            .into_iter()
            .map(TrinoBurnedDataTransferSession::from)
            .collect::<Vec<_>>();

        iceberg::write(writer, sessions).await?;
    }

    Ok(())
}
