// TODO: REMOVE ME WHEN DONE

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{Pool, Postgres, QueryBuilder};
use task_manager::ManagedTask;

type EntityKey = Vec<u8>;

#[derive(Debug, Clone, sqlx::FromRow)]
struct MobileRadio {
    entity_key: EntityKey,
    refreshed_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    location: Option<i64>,
    is_full_hotspot: Option<i32>,
    num_location_asserts: Option<i32>,
    is_active: Option<i32>,
    dc_onboarding_fee_paid: Option<i64>,
    device_type: String,
    deployment_info: Option<String>,
}

impl MobileRadio {
    fn hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(
            self.location
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 8])
                .as_ref(),
        );

        hasher.update(
            self.is_full_hotspot
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 4])
                .as_ref(),
        );

        hasher.update(
            self.num_location_asserts
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 4])
                .as_ref(),
        );

        hasher.update(
            self.is_active
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 4])
                .as_ref(),
        );

        hasher.update(
            self.dc_onboarding_fee_paid
                .map(|l| l.to_le_bytes())
                .unwrap_or([0_u8; 8])
                .as_ref(),
        );

        hasher.update(self.device_type.as_ref());

        hasher.update(
            self.deployment_info
                .clone()
                .unwrap_or("".to_string())
                .as_ref(),
        );

        hasher.finalize().to_string()
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct TrackedMobileRadio {
    pub entity_key: EntityKey,
    pub hash: String,
    pub last_changed_at: DateTime<Utc>,
    pub last_checked_at: DateTime<Utc>,
    pub asserted_location: Option<i64>,
    pub asserted_location_changed_at: Option<DateTime<Utc>>,
}

impl TrackedMobileRadio {
    fn new(radio: &MobileRadio) -> Self {
        let asserted_location_changed_at = if radio.location.is_some() {
            Some(radio.refreshed_at)
        } else {
            None
        };

        Self {
            entity_key: radio.entity_key.clone(),
            hash: radio.hash(),
            last_changed_at: radio.refreshed_at,
            last_checked_at: Utc::now(),
            asserted_location: radio.location,
            asserted_location_changed_at,
        }
    }

    fn update_from_radio(mut self, radio: &MobileRadio) -> Self {
        let new_hash = radio.hash();
        if self.hash != new_hash {
            self.hash = new_hash;
            self.last_changed_at = radio.refreshed_at;
        }
        if self.asserted_location != radio.location {
            self.asserted_location = radio.location;
            self.asserted_location_changed_at = Some(radio.refreshed_at);
            self.last_changed_at = radio.refreshed_at;
        }
        self.last_checked_at = Utc::now();
        self
    }
}

pub struct MobileRadioTracker {
    pool: Pool<Postgres>,
    metadata: Pool<Postgres>,
    interval: Duration,
}

impl ManagedTask for MobileRadioTracker {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl MobileRadioTracker {
    pub fn new(pool: Pool<Postgres>, metadata: Pool<Postgres>, interval: Duration) -> Self {
        Self {
            pool,
            metadata,
            interval,
        }
    }

    async fn run(self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting with interval: {:?}", self.interval);
        let mut interval = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    if let Err(err) = track_changes(&self.pool, &self.metadata).await {
                        tracing::error!(?err, "error in tracking changes to mobile radios");
                    }
                }
            }
        }

        tracing::info!("stopping");

        Ok(())
    }
}

pub async fn track_changes(pool: &Pool<Postgres>, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    tracing::info!("looking for changes to radios");
    let tracked_radios = get_tracked_radios(pool).await?;
    let all_mobile_radios = get_all_mobile_radios(metadata);

    let updates = identify_changes(all_mobile_radios, tracked_radios).await;
    tracing::info!("updating in db: {}", updates.len());

    update_tracked_radios(pool, updates).await?;
    tracing::info!("done");

    Ok(())
}

async fn identify_changes(
    all_mobile_radios: impl Stream<Item = MobileRadio>,
    tracked_radios: HashMap<EntityKey, TrackedMobileRadio>,
) -> Vec<TrackedMobileRadio> {
    all_mobile_radios
        .scan(tracked_radios, |tracked, radio| {
            let tracked_radio_opt = tracked.remove(&radio.entity_key);
            async { Some((radio, tracked_radio_opt)) }
        })
        .map(|(radio, tracked_radio_opt)| match tracked_radio_opt {
            Some(tracked_radio) => tracked_radio.update_from_radio(&radio),
            None => TrackedMobileRadio::new(&radio),
        })
        .collect()
        .await
}

pub async fn get_tracked_radios(
    pool: &Pool<Postgres>,
) -> anyhow::Result<HashMap<EntityKey, TrackedMobileRadio>> {
    sqlx::query_as::<_, TrackedMobileRadio>(
        r#"
        SELECT 
            entity_key,
            hash,
            last_changed_at,
            last_checked_at,
            asserted_location,
            asserted_location_changed_at
        FROM mobile_radio_tracker
        "#,
    )
    .fetch(pool)
    .try_fold(HashMap::new(), |mut map, tracked_radio| async move {
        map.insert(tracked_radio.entity_key.clone(), tracked_radio);
        Ok(map)
    })
    .map_err(anyhow::Error::from)
    .await
}

fn get_all_mobile_radios(metadata: &Pool<Postgres>) -> impl Stream<Item = MobileRadio> + '_ {
    sqlx::query_as::<_, MobileRadio>(
        r#"
        SELECT
            DISTINCT ON (kta.entity_key)
            kta.entity_key,
            mhi.asset,
            mhi.refreshed_at,
            mhi.created_at,
            mhi.location::bigint,
            mhi.is_full_hotspot::int,
            mhi.num_location_asserts,
            mhi.is_active::int,
            mhi.dc_onboarding_fee_paid::bigint,
            mhi.device_type::text,
            mhi.deployment_info::text
        FROM key_to_assets kta
        INNER JOIN mobile_hotspot_infos mhi ON
            kta.asset = mhi.asset
        WHERE kta.entity_key IS NOT NULL
        	AND mhi.refreshed_at IS NOT NULL
        ORDER BY kta.entity_key, refreshed_at DESC
    "#,
    )
    .fetch(metadata)
    .filter_map(|result| async move {
        if let Err(err) = &result {
            tracing::error!(?err, "error when reading radio metadata");
        }
        result.ok()
    })
    .boxed()
}

async fn update_tracked_radios(
    pool: &Pool<Postgres>,
    tracked_radios: Vec<TrackedMobileRadio>,
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;

    const BATCH_SIZE: usize = (u16::MAX / 6) as usize;

    for chunk in tracked_radios.chunks(BATCH_SIZE) {
        QueryBuilder::new(
            "INSERT INTO mobile_radio_tracker(entity_key, hash, last_changed_at, last_checked_at, asserted_location, asserted_location_changed_at)",
        )
        .push_values(chunk, |mut b, tracked_radio| {
            b.push_bind(&tracked_radio.entity_key)
                .push_bind(&tracked_radio.hash)
                .push_bind(tracked_radio.last_changed_at)
                .push_bind(tracked_radio.last_checked_at)
                .push_bind(tracked_radio.asserted_location)
                .push_bind(tracked_radio.asserted_location_changed_at);
        })
        .push(
            r#"
            ON CONFLICT (entity_key) DO UPDATE SET
                hash = EXCLUDED.hash,
                last_changed_at = EXCLUDED.last_changed_at,
                last_checked_at = EXCLUDED.last_checked_at,
                asserted_location = EXCLUDED.asserted_location,
                asserted_location_changed_at = EXCLUDED.asserted_location_changed_at
            "#,
        )
        .build()
        .execute(&mut *txn)
        .await?;
    }

    txn.commit().await?;

    Ok(())
}

// 1. Fill missed `asserted_location_changed_at` in mobile_radio_tracker table
pub async fn post_migrate_mobile_tracker_locations(
    mobile_config_pool: Pool<Postgres>,
    metadata_pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let mut txn = mobile_config_pool.begin().await?;

    let mobile_infos = get_all_mobile_radios(&metadata_pool)
        .filter(|v| futures::future::ready(v.location.is_some()))
        .filter(|v| futures::future::ready(v.num_location_asserts.is_some_and(|num| num >= 1)))
        .collect::<Vec<_>>()
        .await;

    // Get inconsistent mobile_radio_tracker rows
    let mut tracked_radios = get_tracked_radios(&mobile_config_pool).await?;
    tracked_radios
        .retain(|_k, v| v.asserted_location.is_some() && v.asserted_location_changed_at.is_none());
    tracing::info!(
        "Count of tracked radios with inconsistent info before migration: {}",
        tracked_radios.len()
    );

    for (k, _v) in tracked_radios {
        if let Some(mobile_info) = mobile_infos.iter().find(|v| v.entity_key == k) {
            sqlx::query(
                r#"
           UPDATE 
"mobile_radio_tracker" SET asserted_location_changed_at = $1 WHERE entity_key = $2
    "#,
            )
            .bind(mobile_info.created_at)
            .bind(&mobile_info.entity_key)
            .execute(&mut *txn)
            .await
            .unwrap();
        } else {
            tracing::error!("Radio {} is not found", PublicKeyBinary::from(k))
        }
    }

    txn.commit().await?;

    let mut tracked_radios = get_tracked_radios(&mobile_config_pool).await?;
    tracked_radios
        .retain(|_k, v| v.asserted_location.is_some() && v.asserted_location_changed_at.is_none());
    tracing::info!(
        "Count of tracked radios with inconsistent info after migration: {}",
        tracked_radios.len()
    );

    Ok(())
}

#[cfg(test)]
mod tests {

    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn records_tracking_for_new_radio() {
        let radio = mobile_radio(vec![1, 2, 3]);

        let result = identify_changes(stream::iter(vec![radio.clone()]), HashMap::new()).await;

        assert_eq!(result[0].entity_key, radio.entity_key);
        assert_eq!(result[0].hash, radio.hash());
        assert_eq!(result[0].last_changed_at, radio.refreshed_at);
    }

    #[tokio::test]
    async fn will_not_update_if_nothing_changes() {
        let mut radio = mobile_radio(vec![1, 2, 3]);
        let tracked_radio = TrackedMobileRadio::new(&radio);
        let original_refreshed_at = radio.refreshed_at;
        radio.refreshed_at = Utc::now();

        let mut tracked_radios = HashMap::new();
        tracked_radios.insert(tracked_radio.entity_key.clone(), tracked_radio);

        let result = identify_changes(stream::iter(vec![radio.clone()]), tracked_radios).await;

        assert_eq!(1, result.len());
        assert_eq!(original_refreshed_at, result[0].last_changed_at);
    }

    #[tokio::test]
    async fn will_update_last_changed_at_when_data_changes() {
        let mut radio = mobile_radio(vec![1, 2, 3]);
        let tracked_radio = TrackedMobileRadio::new(&radio);
        radio.refreshed_at = Utc::now();
        radio.location = None;

        let mut tracked_radios = HashMap::new();
        tracked_radios.insert(tracked_radio.entity_key.clone(), tracked_radio);

        let result = identify_changes(stream::iter(vec![radio.clone()]), tracked_radios).await;

        assert_eq!(radio.refreshed_at, result[0].last_changed_at);
        assert_eq!(radio.hash(), result[0].hash);
    }

    #[tokio::test]
    async fn asserted_location_changed_at_is_none_if_location_none() {
        // location None
        let mut radio = mobile_radio(vec![1, 2, 3]);
        radio.location = None;
        let tracked_radio = TrackedMobileRadio::new(&radio);
        let mut tracked_radios = HashMap::new();
        tracked_radios.insert(tracked_radio.entity_key.clone(), tracked_radio);

        let result = identify_changes(stream::iter(vec![radio.clone()]), tracked_radios).await;

        assert!(result[0].asserted_location_changed_at.is_none());
        assert!(result[0].asserted_location.is_none());
    }

    #[tokio::test]
    async fn will_update_last_asserted_location_changed_at_when_location_changes() {
        let mut radio = mobile_radio(vec![1, 2, 3]);
        radio.location = None;
        let tracked_radio = TrackedMobileRadio::new(&radio);
        radio.location = Some(1);

        let mut tracked_radios = HashMap::new();
        tracked_radios.insert(tracked_radio.entity_key.clone(), tracked_radio);

        let result = identify_changes(stream::iter(vec![radio.clone()]), tracked_radios).await;

        assert_eq!(
            result[0].asserted_location_changed_at.unwrap(),
            result[0].last_changed_at
        );
        assert_eq!(result[0].asserted_location.unwrap(), 1);

        let tracked_radio = TrackedMobileRadio::new(&radio);
        radio.location = Some(2);
        let mut tracked_radios = HashMap::new();
        tracked_radios.insert(tracked_radio.entity_key.clone(), tracked_radio);
        let result = identify_changes(stream::iter(vec![radio.clone()]), tracked_radios).await;

        assert_eq!(
            result[0].asserted_location_changed_at.unwrap(),
            result[0].last_changed_at,
        );
        assert_eq!(result[0].asserted_location.unwrap(), 2);
    }

    fn mobile_radio(entity_key: EntityKey) -> MobileRadio {
        MobileRadio {
            entity_key,
            refreshed_at: Utc::now() - chrono::Duration::hours(1),
            created_at: Utc::now() - chrono::Duration::hours(1),
            location: Some(1),
            is_full_hotspot: Some(1),
            num_location_asserts: Some(1),
            is_active: Some(1),
            dc_onboarding_fee_paid: Some(10),
            device_type: "wifi".to_string(),
            deployment_info: Some("deployment_info".to_string()),
        }
    }
}
