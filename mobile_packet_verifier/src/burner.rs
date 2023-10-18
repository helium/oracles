use chrono::{DateTime, Utc};
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::SolanaNetwork;
use sqlx::{FromRow, Pool, Postgres};
use std::collections::HashMap;

#[derive(FromRow)]
pub struct DataTransferSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

#[derive(Default)]
pub struct PayerTotals {
    total_dcs: u64,
    sessions: Vec<DataTransferSession>,
}

impl PayerTotals {
    fn push_sess(&mut self, sess: DataTransferSession) {
        self.total_dcs += bytes_to_dc(sess.downloaded_bytes as u64 + sess.uploaded_bytes as u64);
        self.sessions.push(sess);
    }
}

pub struct Burner<S> {
    valid_sessions: FileSinkClient,
    solana: S,
}

impl<S> Burner<S> {
    pub fn new(valid_sessions: FileSinkClient, solana: S) -> Self {
        Self {
            valid_sessions,
            solana,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError<E> {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("solana error: {0}")]
    SolanaError(E),
}

impl<S> Burner<S>
where
    S: SolanaNetwork,
{
    pub async fn burn(&self, pool: &Pool<Postgres>) -> Result<(), BurnError<S::Error>> {
        // Fetch all of the sessions
        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * FROM data_transfer_sessions")
                .fetch_all(pool)
                .await?;

        // Fetch all of the sessions and group by the payer
        let mut payer_totals = HashMap::<PublicKeyBinary, PayerTotals>::new();
        for session in sessions.into_iter() {
            payer_totals
                .entry(session.payer.clone())
                .or_default()
                .push_sess(session);
        }

        for (
            payer,
            PayerTotals {
                total_dcs,
                sessions,
            },
        ) in payer_totals.into_iter()
        {
            let payer_balance = self
                .solana
                .payer_balance(&payer)
                .await
                .map_err(BurnError::SolanaError)?;

            if payer_balance < total_dcs {
                tracing::warn!(%payer, %payer_balance, %total_dcs, "Payer does not have enough balance to burn dcs");
                continue;
            }

            tracing::info!(%total_dcs, %payer, "Burning DC");
            if self.burn_data_credits(&payer, total_dcs).await.is_err() {
                // We have failed to burn data credits:
                metrics::counter!("burned", total_dcs, "payer" => payer.to_string(), "success" => "false");
                continue;
            }

            // We succesfully managed to burn data credits:

            metrics::counter!("burned", total_dcs, "payer" => payer.to_string(), "success" => "true");

            // Delete from the data transfer session and write out to S3

            sqlx::query("DELETE FROM data_transfer_sessions WHERE payer = $1")
                .bind(&payer)
                .execute(pool)
                .await?;

            for session in sessions {
                let num_dcs =
                    bytes_to_dc(session.uploaded_bytes as u64 + session.downloaded_bytes as u64);
                self.valid_sessions
                    .write(
                        ValidDataTransferSession {
                            pub_key: session.pub_key.into(),
                            payer: session.payer.into(),
                            upload_bytes: session.uploaded_bytes as u64,
                            download_bytes: session.downloaded_bytes as u64,
                            num_dcs,
                            first_timestamp: session.first_timestamp.encode_timestamp_millis(),
                            last_timestamp: session.last_timestamp.encode_timestamp_millis(),
                        },
                        &[],
                    )
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

const BYTES_PER_DC: u64 = 20_000;

fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    // Integer div/ceil from: https://stackoverflow.com/a/2745086
    (bytes + BYTES_PER_DC - 1) / BYTES_PER_DC
}
