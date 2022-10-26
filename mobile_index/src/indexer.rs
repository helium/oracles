use crate::{Result, Settings};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{FileStore, FileType, Stream};
use futures::{stream, StreamExt, TryStreamExt};
use helium_proto::{Message, SubnetworkReward, SubnetworkRewards};
use sqlx::{Pool, Postgres};
use tokio::time;

pub const DEFAULT_START_REWARD_BLOCK: i64 = 1477650;
/// default minutes to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 30;

pub struct Indexer {
    pool: Pool<Postgres>,
    interval: time::Duration,
    verifier_store: FileStore,
}

impl Indexer {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(10).await?;
        Ok(Self {
            interval: settings.interval(),
            verifier_store: FileStore::from_settings(&settings.verifier).await?,
            pool,
        })
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting mobile indexer");

        let mut interval_timer = tokio::time::interval(self.interval);

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping mobile indexer");
                return Ok(());
            }

            tokio::select! {
                _ = shutdown.clone() => (),
                _ = interval_timer.tick() => {
                    self.process_clock_tick().await?
                }
            }
        }
    }

    async fn process_clock_tick(&mut self) -> Result {
        let last_known_timestamp = MetaValue::<DateTime<Utc>>::fetch_or_insert_with(
            &self.pool,
            "last_verifier_timestamp",
            || Utc.timestamp(0, 0),
        )
        .await?
        .value()
        .to_owned();

        let time_range = last_known_timestamp..Utc::now();
        let file_list = self
            .verifier_store
            .list_all(
                FileType::SubnetworkRewards,
                time_range.start,
                time_range.end,
            )
            .await?;

        let _subnet_rewards: Stream<SubnetworkReward> = self
            .verifier_store
            .source(stream::iter(file_list).map(Ok).boxed())
            .map_ok(move |msg| (msg, time_range.clone()))
            // First decode each subnet rewards, filtering out non decodable
            // messages
            .try_filter_map(|(msg, time_range)| async move {
                Ok(SubnetworkRewards::decode(msg).ok().zip(Some(time_range)))
            })
            // We only care that the verified epoch ends in our rewards epoch,
            // so that we include verified epochs that straddle two rewards
            // epochs. map the matching subnet rewards to a stream of their
            // reward entries
            .try_filter_map(|(subnet_rewards, time_range)| async move {
                let reward_stream = time_range
                    .contains(&Utc.timestamp(subnet_rewards.end_epoch as i64, 0))
                    .then(|| stream::iter(subnet_rewards.rewards).map(Ok::<_, file_store::Error>));
                Ok(reward_stream)
            })
            .try_flatten()
            .boxed();

        Ok(())
    }
}

pub fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc = Utc.timestamp(last_reward_end_time, 0);
    let now = Utc::now();
    let stop_utc = now - Duration::minutes(DEFAULT_LOOKUP_DELAY);
    let start_utc = after_utc.min(stop_utc);
    (start_utc, stop_utc)
}
