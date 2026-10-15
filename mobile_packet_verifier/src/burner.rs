use anyhow::Context;
use file_store_oracles::mobile_transfer::ValidDataTransferSession;
use helium_crypto::PublicKeyBinary;
use solana::{
    burn::{BurnOutcome, SolanaNetwork, BLOCKHASH_EXPIRY_MARGIN},
    GetLastValidBlockHeight, GetSignature, Signature, SolanaRpcError,
};
use sqlx::PgPool;
use tracing::Instrument;

use crate::{
    iceberg::{burned_session::IcebergBurnedDataTransferSession, BurnedDataTransferWriter},
    pending_burns, pending_txns,
};

pub struct Burner<S> {
    solana: S,
    failed_retry_attempts: usize,
    failed_check_interval: std::time::Duration,
    iceberg_writer: BurnedDataTransferWriter,
}

impl<S> Burner<S> {
    pub fn new(
        solana: S,
        failed_retry_attempts: usize,
        failed_check_interval: std::time::Duration,
        iceberg_writer: BurnedDataTransferWriter,
    ) -> Self {
        Self {
            solana,
            failed_retry_attempts,
            failed_check_interval,
            iceberg_writer,
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
            let signature = pending.signature;
            let burned = self
                .settle(&signature, pending.last_valid_block_height)
                .await?;
            tracing::info!(?pending, burned, "settled pending txn");

            if burned {
                let sessions =
                    pending_txns::get_pending_data_sessions_for_signature(pool, &signature).await?;

                write_burned_data_transfer_sessions(sessions, &self.iceberg_writer).await?;

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
            let payer = payer_pending_burn.payer.clone();
            // Groups for the record, the per-file rows behind them for the move.
            let sessions = payer_pending_burn.sessions();
            let rows = payer_pending_burn.rows;

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
            pending_txns::add_pending_txn(
                pool,
                &payer,
                total_dcs,
                txn.get_signature(),
                txn.last_valid_block_height(),
                &rows,
            )
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
                        &self.iceberg_writer,
                    )
                    .await?;
                }
                Err(err) => {
                    // The send failed, but the txn may still land. Wait for
                    // the chain to settle it. If the chain can't be reached,
                    // the txn stays pending and `confirm_pending_txns()`
                    // settles it on the next burn.
                    let span = tracing::info_span!(
                        "txn_confirmation",
                        signature = %txn.get_signature(),
                        %payer,
                        total_dcs,
                        max_rpc_errors = self.failed_retry_attempts
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
        let burned = match self.settle(signature, txn.last_valid_block_height()).await {
            Ok(burned) => burned,
            Err(err) => {
                tracing::error!(?err, "failed to confirm txn, leaving it pending");
                return;
            }
        };

        if burned {
            tracing::debug!("txn confirmed on chain");
            let txn_success = handle_transaction_success(
                pool,
                signature,
                payer,
                total_dcs,
                sessions,
                &self.iceberg_writer,
            )
            .await;
            if let Err(err) = txn_success {
                tracing::error!(?err, "txn succeeded, something else failed");
            }

            return;
        }

        tracing::warn!("txn can no longer land, moving its sessions back");
        if let Err(err) = pending_txns::remove_pending_txn_failure(pool, signature).await {
            // Still pending, so `confirm_pending_txns()` releases it next burn.
            tracing::error!(?err, "failed to move sessions back");
        }

        // We have failed to burn data credits:
        metrics::counter!(
            "burned",
            "payer" => payer.to_string(),
            "success" => "false"
        )
        .increment(total_dcs);
    }

    /// Wait for the chain to settle a burn, and return whether it spent the DC.
    ///
    /// Loops while the burn is [`BurnOutcome::Pending`]. That ends once the
    /// finalized block height passes the burn's expiry, about two minutes
    /// after it was signed. RPC errors are retried `failed_retry_attempts`
    /// times in a row before giving up.
    async fn settle(
        &self,
        signature: &Signature,
        last_valid_block_height: u64,
    ) -> anyhow::Result<bool> {
        let expiry = last_valid_block_height.saturating_add(BLOCKHASH_EXPIRY_MARGIN);
        let started = std::time::Instant::now();
        let mut checks = 0;
        let mut rpc_errors = 0;

        tracing::info!(%signature, last_valid_block_height, expiry, "settling txn");

        loop {
            checks += 1;
            match self
                .solana
                .burn_outcome(signature, last_valid_block_height)
                .await
            {
                Ok(BurnOutcome::Pending {
                    finalized_block_height,
                }) => {
                    rpc_errors = 0;
                    tracing::info!(
                        %signature,
                        checks,
                        elapsed = ?started.elapsed(),
                        finalized_block_height,
                        blocks_until_expiry = expiry.saturating_sub(finalized_block_height),
                        next_check_in = ?self.failed_check_interval,
                        "txn not settled yet, waiting"
                    );
                }
                Ok(outcome) => {
                    tracing::info!(
                        %signature,
                        ?outcome,
                        checks,
                        elapsed = ?started.elapsed(),
                        "txn settled"
                    );
                    return Ok(outcome == BurnOutcome::Burned);
                }
                Err(err) if rpc_errors < self.failed_retry_attempts => {
                    rpc_errors += 1;
                    tracing::warn!(
                        ?err,
                        %signature,
                        checks,
                        rpc_errors,
                        max_rpc_errors = self.failed_retry_attempts,
                        "failed to check txn, retrying"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        %signature,
                        checks,
                        rpc_errors,
                        elapsed = ?started.elapsed(),
                        "too many rpc errors, giving up settling txn"
                    );
                    return Err(err).context("checking burn outcome");
                }
            }
            tokio::time::sleep(self.failed_check_interval).await;
        }
    }
}

async fn handle_transaction_success(
    pool: &PgPool,
    signature: &Signature,
    payer: PublicKeyBinary,
    total_dcs: u64,
    sessions: Vec<pending_burns::DataTransferSession>,
    iceberg_writer: &BurnedDataTransferWriter,
) -> Result<(), anyhow::Error> {
    // We successfully managed to burn data credits:
    metrics::counter!(
        "burned",
        "payer" => payer.to_string(),
        "success" => "true"
    )
    .increment(total_dcs);

    // Delete from the data transfer session and write out to Iceberg
    pending_burns::delete_for_payer(pool, &payer).await?;
    // The payer has no rows left, so 0 is exact. Setting rather than
    // subtracting the burned amount because that only lands on 0 while the two
    // figures agree, and `accumulate` counts differently from the burn.
    pending_burns::set_metric(&payer, 0);
    pending_txns::remove_pending_txn_success(pool, signature).await?;

    write_burned_data_transfer_sessions(sessions, iceberg_writer).await?;

    Ok(())
}

async fn write_burned_data_transfer_sessions(
    sessions: Vec<pending_burns::DataTransferSession>,
    iceberg_sink: &BurnedDataTransferWriter,
) -> anyhow::Result<()> {
    // Fallible now: a session whose multiplier cannot be applied has no price,
    // and a burned record without one would be a lie about what was charged.
    // This runs after the burn has settled, so it cannot prevent the spend — it
    // surfaces as a write failure, which is the pre-existing behaviour for any
    // failure on this path.
    let sessions = sessions
        .into_iter()
        .map(ValidDataTransferSession::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;

    // DC burned, split by the multiplier that produced it.
    //
    // `burned` counts what the transaction charged, which is the right number
    // but says nothing about where it came from. This splits the same total, so
    // summing it back up gives `burned` again, and grouping by multiplier shows
    // how much of the burn is coming from ticketed hotspots.
    //
    // Both burn paths pass through here, so this counts a burn once, after it
    // has settled. The multiplier is normalized, so `1.5` and `1.50` are one
    // series rather than two.
    for session in &sessions {
        metrics::counter!(
            "burned_dc_by_multiplier",
            "payer" => session.payer.to_string(),
            "multiplier" => session.multiplier.to_string(),
        )
        .increment(session.num_dcs);
    }

    let sessions = sessions
        .into_iter()
        .map(IcebergBurnedDataTransferSession::from)
        .collect::<Vec<_>>();

    // NOTE: Burned Sessions are not tied to a file like regular data
    // transfer sessions. Do we want to use transaction semantics here?
    iceberg_sink.write(sessions).await?;

    Ok(())
}
