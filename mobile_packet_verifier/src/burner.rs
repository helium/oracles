use file_store::file_sink::FileSinkClient;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::{burn::SolanaNetwork, sender};
use sqlx::{PgPool, Pool, Postgres};
use tracing::Instrument;

use crate::pending_burns;

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
    pub async fn burn(&self, pool: &Pool<Postgres>) -> anyhow::Result<()> {
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
            let store = BurnerTxnStore::new(
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

struct BurnerTxnStore {
    pool: PgPool,
    payer: PublicKeyBinary,
    amount: u64,
    sessions: Vec<pending_burns::DataTransferSession>,
    valid_sessions: FileSinkClient<ValidDataTransferSession>,
}

impl BurnerTxnStore {
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

    fn on_error(&self) {
        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "false"
        )
        .increment(self.amount);
    }
}

#[async_trait::async_trait]
impl sender::TxnStore for BurnerTxnStore {
    // fn make_span(&self) -> tracing::Span {
    //     tracing::info_span!(
    //         "burn_txn",
    //         payer = %self.payer,
    //         amount = self.amount
    //     )
    // }

    async fn on_prepared(
        &self,
        _txn: &solana::TransactionWithBlockhash,
    ) -> sender::SenderResult<()> {
        tracing::info!("txn prepared");
        Ok(())
    }

    async fn on_sent(&self, _txn: &solana::TransactionWithBlockhash) {
        tracing::info!("txn sent");
    }

    async fn on_sent_retry(&self, _txn: &solana::TransactionWithBlockhash, attempt: usize) {
        tracing::warn!(attempt, "retrying");
    }

    async fn on_finalized(&self, _txn: &solana::TransactionWithBlockhash) {
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

    async fn on_error_sending(
        &self,
        _txn: &solana::TransactionWithBlockhash,
        err: &sender::SolanaClientError,
    ) {
        tracing::warn!(?err, "failed to send");
        self.on_error();
    }

    async fn on_error_finalizing(
        &self,
        _txn: &solana::TransactionWithBlockhash,
        err: &sender::SolanaClientError,
    ) {
        tracing::warn!(?err, "failed to finalize");
        self.on_error();
    }
}
