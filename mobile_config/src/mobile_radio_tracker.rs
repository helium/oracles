use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use sqlx::{Pool, Postgres, QueryBuilder};
use task_manager::ManagedTask;

type EntityKey = Vec<u8>;

#[derive(Debug, sqlx::FromRow)]
struct MobileRadio {
    entity_key: EntityKey,
    refreshed_at: DateTime<Utc>,
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
struct TrackedMobileRadio {
    entity_key: EntityKey,
    hash: String,
    last_changed_at: DateTime<Utc>,
    last_checked_at: DateTime<Utc>,
}

impl TrackedMobileRadio {
    fn new(radio: &MobileRadio) -> Self {
        Self {
            entity_key: radio.entity_key.clone(),
            hash: radio.hash(),
            last_changed_at: radio.refreshed_at,
            last_checked_at: Utc::now(),
        }
    }

    fn update_from_radio(mut self, radio: &MobileRadio) -> Self {
        let new_hash = radio.hash();
        if self.hash != new_hash {
            self.hash = new_hash;
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
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
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
        tracing::info!("starting MobileRadioTracker");
        let mut interval = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    //TODO probably shouldn't crash api when this fails
                    track_changes(&self.pool, &self.metadata).await?;
                }
            }
        }

        Ok(())
    }
}

async fn track_changes(pool: &Pool<Postgres>, metadata: &Pool<Postgres>) -> anyhow::Result<()> {
    let tracked_radios = get_tracked_radios(pool).await?;

    let updates: Vec<TrackedMobileRadio> = get_all_mobile_radios(metadata)
        .scan(tracked_radios, |tracked, radio| {
            let tracked_radio_opt = tracked.remove(&radio.entity_key);
            async move { Some((radio, tracked_radio_opt)) }
        })
        .map(|(radio, tracked_radio_opt)| match tracked_radio_opt {
            Some(tracked_radio) => tracked_radio.update_from_radio(&radio),
            None => TrackedMobileRadio::new(&radio),
        })
        .collect()
        .await;

    update_tracked_radios(pool, updates).await?;

    Ok(())
}

async fn get_tracked_radios(
    pool: &Pool<Postgres>,
) -> anyhow::Result<HashMap<EntityKey, TrackedMobileRadio>> {
    sqlx::query_as::<_, TrackedMobileRadio>(
        r#"
        SELECT 
            entity_key,
            hash,
            last_changed_at,
            last_checked_at
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

fn get_all_mobile_radios<'a>(metadata: &'a Pool<Postgres>) -> impl Stream<Item = MobileRadio> + 'a {
    sqlx::query_as::<_, MobileRadio>(
        r#"
        SELECT
        	kta.entity_key,
        	kta.refreshed_at,
        	mhi.location::bigint,
        	mhi.is_full_hotspot,
        	mhi.num_location_asserts,
        	mhi.is_active,
        	mhi.dc_onboarding_fee_paid::bigint,
        	mhi.device_type,
        	mhi.deployment_info
        FROM key_to_assets kta
        INNER JOIN mobile_hotspot_infos mhi ON
        	kta.asset = mhi.asset
        WHERE kta.entity_key IS NOT NULL
        	AND mhi.refreshed_at IS NOT NULL
    "#,
    )
    .fetch(metadata)
    .filter_map(|result| async move { result.ok() })
    .boxed()
}

async fn update_tracked_radios(
    pool: &Pool<Postgres>,
    tracked_radios: Vec<TrackedMobileRadio>,
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;

    const BATCH_SIZE: usize = (u16::MAX / 4) as usize;

    for chunk in tracked_radios.chunks(BATCH_SIZE) {
        QueryBuilder::new(
            "INSERT INTO mobile_radio_tracker(entity_key, hash, last_changed_at, last_checked_at)",
        )
        .push_values(chunk, |mut b, tracked_radio| {
            b.push_bind(&tracked_radio.entity_key)
                .push_bind(&tracked_radio.hash)
                .push_bind(tracked_radio.last_changed_at)
                .push_bind(tracked_radio.last_checked_at);
        })
        .push(
            r#"
            ON CONFLICT (entity_key) DO UPDATE SET
                hash = EXCLUDED.hash,
                last_changed_at = EXCLUDED.last_changed_at,
                last_checked_at = EXCLUDED.last_checked_at
            "#,
        )
        .build()
        .execute(&mut txn)
        .await?;
    }

    txn.commit().await?;

    Ok(())
}
