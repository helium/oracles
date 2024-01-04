use chrono::{DateTime, Duration, Utc};
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::{GetSignature, SolanaNetwork};
use solana_sdk::signature::Signature;
use sqlx::{FromRow, Pool, Postgres};

#[derive(FromRow)]
pub struct DataTransferSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    rewardable_bytes: i64,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

#[derive(FromRow)]
pub struct PayerTotals {
    payer: PublicKeyBinary,
    total_dcs: i64,
    txn: Option<SolanaTransaction>,
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "solana_transaction")]
pub struct SolanaTransaction {
    signature: String,
    amount: i64,
    time_of_submission: DateTime<Utc>,
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
    #[error("Chrono error: {0}")]
    ChronoError(#[from] chrono::OutOfRangeError),
    #[error("solana error: {0}")]
    SolanaError(E),
}

impl<S> Burner<S>
where
    S: SolanaNetwork,
{
    pub async fn burn(&self, pool: &Pool<Postgres>) -> Result<(), BurnError<S::Error>> {
        // Fetch all of the payer totals:
        let totals: Vec<PayerTotals> = sqlx::query_as("SELECT * FROM payer_totals")
            .fetch_all(pool)
            .await?;

        for PayerTotals {
            payer,
            total_dcs,
            txn,
        } in totals
        {
            let mut total_dcs = total_dcs as u64;

            // Check if there is a pending transaction
            if let Some(SolanaTransaction {
                signature,
                amount,
                time_of_submission,
            }) = txn
            {
                // Sleep for at least a minute since the time of submission to
                // give the transaction plenty of time to be confirmed:
                let time_since_submission = Utc::now() - time_of_submission;
                if Duration::minutes(1) > time_since_submission {
                    tokio::time::sleep((Duration::minutes(1) - time_since_submission).to_std()?)
                        .await;
                }

                let signature: Signature = signature.parse().unwrap();
                if self
                    .solana
                    .confirm_transaction(&signature)
                    .await
                    .map_err(BurnError::SolanaError)?
                {
                    // This transaction has been confirmed. Subtract the amount confirmed from
                    // the total amount burned and remove the transaction.
                    total_dcs -= amount as u64;
                    sqlx::query(
                        "UPDATE payer_totals SET txn = NULL, total_dcs = $2 WHERE payer = $1",
                    )
                    .bind(&payer)
                    .bind(total_dcs as i64)
                    .execute(pool)
                    .await?;
                } else {
                    // Transaction is no longer valid. Remove it from the payer totals.
                    sqlx::query("UPDATE payer_totals SET txn = NULL WHERE payer = $1")
                        .bind(&payer)
                        .execute(pool)
                        .await?;
                }
            }

            // Get the current sessions we need to write, before creating any new transactions
            let sessions: Vec<DataTransferSession> =
                sqlx::query_as("SELECT * FROM data_transfer_session WHERE payer = $1")
                    .bind(&payer)
                    .fetch_all(pool)
                    .await?;

            // Create a new transaction for the given amount, if there is any left.
            // If total_dcs is zero, that means we need to clear out the current sessions as they are paid for.
            if total_dcs != 0 {
                let txn = self
                    .solana
                    .make_burn_transaction(&payer, total_dcs)
                    .await
                    .map_err(BurnError::SolanaError)?;
                sqlx::query("UPDATE payer_totals SET txn = $2 WHERE payer = $1")
                    .bind(&payer)
                    .bind(SolanaTransaction {
                        signature: txn.get_signature().to_string(),
                        amount: total_dcs as i64,
                        time_of_submission: Utc::now(),
                    })
                    .execute(pool)
                    .await?;
                // Attempt to execute the transaction
                if self.solana.submit_transaction(&txn).await.is_err() {
                    // We have failed to burn data credits:
                    metrics::counter!("burned", total_dcs, "payer" => payer.to_string(), "success" => "false");
                    continue;
                }
            }

            // Submit the sessions
            sqlx::query("DELETE FROM data_transfer_sessions WHERE payer = $1")
                .bind(&payer)
                .execute(pool)
                .await?;

            sqlx::query("DELETE FROM payer_totals WHERE payer = $1")
                .bind(&payer)
                .execute(pool)
                .await?;

            for session in sessions {
                let num_dcs = crate::bytes_to_dc(session.rewardable_bytes as u64);
                self.valid_sessions
                    .write(
                        ValidDataTransferSession {
                            pub_key: session.pub_key.into(),
                            payer: session.payer.into(),
                            upload_bytes: session.uploaded_bytes as u64,
                            download_bytes: session.downloaded_bytes as u64,
                            rewardable_bytes: session.rewardable_bytes as u64,
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
}
