use anyhow::Context;
use chrono::{Duration, Utc};
use file_store::file_sink::FileSinkClient;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::{burn::SolanaNetwork, sender};
use sqlx::PgPool;
use tracing::Instrument;

use crate::{pending_burns, pending_txns};

pub struct Burner<S> {
    valid_sessions: FileSinkClient<ValidDataTransferSession>,
    solana: S,
}

impl<S> Burner<S> {
    pub fn new(valid_sessions: FileSinkClient<ValidDataTransferSession>, solana: S) -> Self {
        Self {
            valid_sessions,
            solana,
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
        let pending = pending_txns::fetch_all_pending_txns(pool).await?;
        tracing::info!(count = pending.len(), "confirming pending txns");

        for pending in pending {
            // Sleep for at least a minute since the time of submission to
            // give the transaction plenty of time to be confirmed:
            let time_since_submission = Utc::now() - pending.time_of_submission;
            if Duration::minutes(1) > time_since_submission {
                let delay = Duration::minutes(1) - time_since_submission;
                tracing::info!(?pending, %delay, "waiting to confirm pending txn");
                tokio::time::sleep(delay.to_std()?).await;
            }

            let signature = pending.signature;
            let confirmed = self.solana.confirm_transaction(&signature).await?;
            tracing::info!(?pending, confirmed, "confirming pending transaction");
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
            let txn = self.solana.make_burn_transaction(&payer, total_dcs).await?;
            let store = BurnTxnStore::new(
                pool.clone(),
                payer.clone(),
                total_dcs,
                sessions,
                self.valid_sessions.clone(),
            );

            let burn_span = tracing::info_span!("burn_txn", %payer, amount = total_dcs);
            self.solana
                .submit_transaction(&txn, &store)
                .instrument(burn_span)
                .await?
        }

        Ok(())
    }
}

struct BurnTxnStore {
    pool: PgPool,
    payer: PublicKeyBinary,
    amount: u64,
    sessions: Vec<pending_burns::DataTransferSession>,
    valid_sessions: FileSinkClient<ValidDataTransferSession>,
}

impl BurnTxnStore {
    fn new(
        pool: PgPool,
        payer: PublicKeyBinary,
        amount: u64,
        sessions: Vec<pending_burns::DataTransferSession>,
        valid_sessions: FileSinkClient<ValidDataTransferSession>,
    ) -> Self {
        Self {
            pool,
            payer,
            amount,
            sessions,
            valid_sessions,
        }
    }
}

#[async_trait::async_trait]
impl sender::TxnStore for BurnTxnStore {
    async fn on_prepared(&self, txn: &solana::Transaction) -> sender::SenderResult<()> {
        tracing::info!("txn prepared");

        let signature = txn.get_signature();
        let add_pending =
            pending_txns::add_pending_txn(&self.pool, &self.payer, self.amount, signature);

        match add_pending.await {
            Ok(()) => {}
            Err(err) => {
                tracing::error!("failed to add pending transaction");
                return Err(sender::SenderError::preparation(&format!(
                    "could not add pending transaction: {err:?}"
                )));
            }
        }

        Ok(())
    }

    async fn on_finalized(&self, txn: &solana::Transaction) {
        tracing::info!("txn finalized");
        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "true"
        )
        .increment(self.amount);

        // Delete from the data transfer session and write out to S3
        let remove_burn = pending_burns::delete_for_payer(&self.pool, &self.payer, self.amount);
        if let Err(err) = remove_burn.await {
            tracing::error!(?err, "failed to deduct finalized burn");
        }

        let signature = txn.get_signature();
        let remove_pending = pending_txns::remove_pending_txn_success(&self.pool, signature);
        if let Err(err) = remove_pending.await {
            tracing::error!(?err, "failed to remove successful pending txn");
        }

        for session in self.sessions.iter() {
            let session = session.to_owned();
            let write = self
                .valid_sessions
                .write(ValidDataTransferSession::from(session), &[]);
            if let Err(err) = write.await {
                tracing::error!(?err, "failed to write data session for finalized burn");
            }
        }
    }

    async fn on_error(&self, txn: &solana::Transaction, err: sender::SenderError) {
        tracing::warn!(?err, "txn failed");

        let signature = txn.get_signature();
        let remove_pending = pending_txns::remove_pending_txn_failure(&self.pool, signature);
        if let Err(err) = remove_pending.await {
            tracing::error!(?err, "failed to remove failed pending txn");
        }

        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "false"
        )
        .increment(self.amount);
    }
}
