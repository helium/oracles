use crate::{
    datetime_from_epoch, error::DecodeError, keypair::Keypair, receipt_txn::handle_report_msg,
    txn_service::TransactionService, Result,
};
use chrono::{DateTime, Utc};
use db_store::MetaValue;
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::{blockchain_txn::Txn, BlockchainTxn};
use sqlx::{Pool, Postgres};
use std::env;
use tokio::time;

// 30 mins
pub const POC_IOT_TICK_TIME: time::Duration = time::Duration::from_secs(1800);
// TODO: decide some default value, not that it really matters if we set it in the .env
pub const DEFAULT_LAST_POC_SUBMISSION_TS: i64 = 0000000000;

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Keypair,
    txn_service: TransactionService,
    iot_verifier_store: FileStore,
    last_poc_submission_ts: MetaValue<i64>,
}

impl Server {
    pub async fn new(pool: Pool<Postgres>, keypair: Keypair) -> Result<Self> {
        // TODO: Find this time in meta store db first
        let last_poc_submission_ts = env::var("LAST_POC_SUBMISSION_TS")
            .unwrap_or_else(|_| DEFAULT_LAST_POC_SUBMISSION_TS.to_string())
            .parse()
            .map_err(DecodeError::from)?;

        let result = Self {
            pool: pool.clone(),
            keypair,
            txn_service: TransactionService::from_env()?,
            iot_verifier_store: FileStore::from_env().await?,
            last_poc_submission_ts: MetaValue::new(
                "last_poc_submission_ts",
                last_poc_submission_ts,
            ),
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting poc-iot-injector server");
        let mut poc_iot_timer = time::interval(POC_IOT_TICK_TIME);
        poc_iot_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = poc_iot_timer.tick() => match self.handle_poc_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal poc_iot_injector error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_poc_tick(&mut self) -> Result {
        let after_utc = datetime_from_epoch(*self.last_poc_submission_ts.value());
        let before_utc = Utc::now();

        submit_txns(
            &mut self.txn_service,
            &mut self.iot_verifier_store,
            &self.keypair,
            after_utc,
            before_utc,
        )
        .await?;

        // NOTE: All the poc_receipt txns for the corresponding after-before period will be
        // submitted above, may take a while to do that but we need to
        // update_last_poc_submission_ts once we're done doing it. This should ensure that we have
        // at least submitted all the receipts we could for that time period and will look at the
        // next incoming reports in the next poc_iot_timer tick.
        self.last_poc_submission_ts
            .update(&self.pool, before_utc.timestamp())
            .await?;

        Ok(())
    }
}

async fn submit_txns(
    txn_service: &mut TransactionService,
    store: &mut FileStore,
    keypair: &Keypair,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result {
    let file_list = store
        .list_all(FileType::LoraValidPoc, after_utc, before_utc)
        .await?;

    let mut stream = store.source(stream::iter(file_list).map(Ok).boxed());
    let ts = before_utc.timestamp_millis();

    while let Some(Ok(msg)) = stream.next().await {
        if let Ok(Some((txn, hash, hash_b64_url))) = handle_report_msg(msg, keypair, ts) {
            if txn_service
                .submit(
                    BlockchainTxn {
                        txn: Some(Txn::PocReceiptsV2(txn)),
                    },
                    &hash,
                )
                .await
                .is_ok()
            {
                tracing::debug!("txn submitted successfully, hash: {:?}", hash_b64_url);
            } else {
                tracing::warn!("txn submission failed!, hash: {:?}", hash_b64_url);
            }
        }
    }
    Ok(())
}
