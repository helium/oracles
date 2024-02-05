use chrono::{DateTime, Utc};
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use solana::{GetSignature, SolanaNetwork};
use solana_sdk::signature::{ParseSignatureError, Signature};
use sqlx::{FromRow, PgPool};
use task_manager::ManagedTask;
use tokio::time::{sleep_until, Duration, Instant};

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
    #[error("parse signature error: {0}")]
    ParseSignatureError(#[from] ParseSignatureError),
}

pub struct BurnInitiator<S> {
    solana: S,
    pool: PgPool,
    burn_period: Duration,
}

impl<S> BurnInitiator<S> {
    pub fn new(solana: S, pool: PgPool, burn_period: Duration) -> Self {
        Self {
            solana,
            pool,
            burn_period,
        }
    }
}

impl<S> ManagedTask for BurnInitiator<S>
where
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl<S> BurnInitiator<S>
where
    S: SolanaNetwork,
{
    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        // Initial burn period is one minute
        let mut burn_time = Instant::now() + Duration::from_secs(60);
        loop {
            #[rustfmt::skip]
            tokio::select! {
		_ = sleep_until(burn_time) => {
                    // Initiate new burns
                    self.initiate_burns().await?;
                    burn_time = Instant::now() + self.burn_period;
		}
		_ = shutdown.clone() => return Ok(()),
            }
        }
    }

    pub async fn initiate_burns(&self) -> anyhow::Result<()> {
        let sessions: Vec<DataTransferSession> = sqlx::query_as(
	    r#"
	    SELECT pub_key, payer, SUM(uploaded_bytes) as uploaded_bytes, SUM(downloaded_bytes) as downloaded_bytes,
	    SUM(rewardable_bytes) as rewardable_bytes, MIN(session_timestamp) as first_timestamp, MAX(session_timestamp) as last_timestamp
	    FROM data_transfer_sessions GROUP BY pub_key, payer
	    WHERE session_timestamp < $1
	    "#
	).bind(Utc::now())
	    .fetch_all(&self.pool)
	    .await?;

        for session in sessions.into_iter() {
            let num_dcs = bytes_to_dc(session.rewardable_bytes as u64);
            let txn = self
                .solana
                .make_burn_transaction(&session.payer, num_dcs)
                .await?;
            let mut transaction = self.pool.begin().await?;
            sqlx::query(
		r#"
		INSERT INTO pending_txns (signature, pub_key, payer, num_dcs, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timesetamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		"#
	    )
		.bind(txn.get_signature().to_string())
		.bind(&session.pub_key)
		.bind(&session.payer)
		.bind(session.uploaded_bytes)
		.bind(session.downloaded_bytes)
		.bind(session.rewardable_bytes)
		.bind(session.first_timestamp)
		.bind(session.last_timestamp)
		.execute(&mut transaction)
		.await?;
            sqlx::query(
		r#"
		DELETE FROM pending_txns WHERE pub_key = $1 AND payer = $2 AND session_timestamp >= $3 and session_timestamp <= $4 
		"#
	    )
		.bind(&session.pub_key)
		.bind(&session.payer)
		.bind(session.first_timestamp)
		.bind(session.last_timestamp)
		.execute(&mut transaction)
		.await?;
            transaction.commit().await?;
            // We should make this a quick submission that doesn't check for confirmation
            let _ = self.solana.submit_transaction(&txn).await;
        }
        Ok(())
    }
}

#[derive(FromRow)]
pub struct PendingTxn {
    signature: String,
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    rewardable_bytes: i64,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

pub struct BurnConfirmer<S> {
    valid_sessions: FileSinkClient,
    solana: S,
    pool: PgPool,
    confirmation_period: Duration,
}

impl<S> BurnConfirmer<S> {
    pub fn new(
        valid_sessions: FileSinkClient,
        solana: S,
        pool: PgPool,
        confirmation_period: Duration,
    ) -> Self {
        Self {
            valid_sessions,
            solana,
            pool,
            confirmation_period,
        }
    }
}

impl<S> ManagedTask for BurnConfirmer<S>
where
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl<S> BurnConfirmer<S>
where
    S: SolanaNetwork,
{
    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        // Initial confirmation period is two minutes
        let mut confirm_time = Instant::now() + Duration::from_secs(120);
        loop {
            #[rustfmt::skip]
            tokio::select! {
		_ = sleep_until(confirm_time) => {
                    // Initiate new burns
                    self.confirm_burns().await?;
                    confirm_time = Instant::now() + self.confirmation_period;
		}
		_ = shutdown.clone() => return Ok(()),
            }
        }
    }

    pub async fn confirm_burns(&self) -> anyhow::Result<()> {
        let pending_txns: Vec<PendingTxn> = sqlx::query_as(r#"SELECT * FROM pending_txns"#)
            .fetch_all(&self.pool)
            .await?;

        for pending_txn in pending_txns {
            let txn: Signature = pending_txn.signature.parse()?;
            let mut transaction = self.pool.begin().await?;
            let num_dcs = bytes_to_dc(pending_txn.rewardable_bytes as u64);
            sqlx::query(r#"DELETE FROM pending_txns WHERE signature = $1"#)
                .bind(pending_txn.signature)
                .execute(&mut transaction)
                .await?;
            if self.solana.confirm_transaction(&txn).await? {
                self.valid_sessions
                    .write(
                        ValidDataTransferSession {
                            pub_key: pending_txn.pub_key.into(),
                            payer: pending_txn.payer.into(),
                            upload_bytes: pending_txn.uploaded_bytes as u64,
                            download_bytes: pending_txn.downloaded_bytes as u64,
                            rewardable_bytes: pending_txn.rewardable_bytes as u64,
                            num_dcs,
                            first_timestamp: pending_txn.first_timestamp.encode_timestamp_millis(),
                            last_timestamp: pending_txn.last_timestamp.encode_timestamp_millis(),
                        },
                        &[],
                    )
                    .await?;
            } else {
                // If we can't confirm the transaction, we can just submit it and check the pending
                // transaction next time
                let new_txn = self
                    .solana
                    .make_burn_transaction(&pending_txn.payer, num_dcs)
                    .await?;
                sqlx::query(
		    r#"
		    INSERT INTO pending_txns (signature, pub_key, payer, num_dcs, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		    "#
		)
		    .bind(new_txn.get_signature().to_string())
		    .bind(pending_txn.pub_key)
		    .bind(pending_txn.payer)
		    .bind(num_dcs as i64)
		    .bind(pending_txn.uploaded_bytes)
		    .bind(pending_txn.downloaded_bytes)
		    .bind(pending_txn.rewardable_bytes)
		    .bind(pending_txn.first_timestamp)
		    .bind(pending_txn.last_timestamp)
		    .execute(&mut transaction)
		    .await?;
                let _ = self.solana.submit_transaction(&new_txn).await;
            }
        }

        Ok(())
    }
}

const BYTES_PER_DC: u64 = 20_000;

pub fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    // Integer div/ceil from: https://stackoverflow.com/a/2745086
    (bytes + BYTES_PER_DC - 1) / BYTES_PER_DC
}
