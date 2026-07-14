//! Trino-backed gateway resolution.
//!
//! [`GatewayResolver`] answers "is this gateway known" for report verification.
//! Gateway membership is served from an in-memory snapshot of the inventory
//! table (loaded at startup, kept fresh by [`GatewaySnapshotRefresher`]) with a
//! per-pubkey Trino fallback.

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use retainer::Cache;
use task_manager::Periodic;

/// Fully-qualified `catalog.schema.table` for the on-chain mobile hotspot
/// inventory queried by [`GatewayResolver::is_gateway_known`].
pub const MOBILE_HOTSPOT_INVENTORY_TABLE: &str = "network.chain.mobile_hotspot_inventory";

/// How often the fallback cache sweeps expired entries.
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_secs(60 * 60);

/// How soon to retry the inventory snapshot load after a failure, rather than
/// waiting the full `refresh_interval`. Keeps a Trino outage from leaving the
/// snapshot degraded for up to an hour.
const GATEWAY_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Resolver used by the running daemon.
///
/// Gateway membership is served from an in-memory snapshot of every pubkey in
/// the inventory table, loaded at startup and kept fresh by a companion
/// [`GatewaySnapshotRefresher`] (see [`GatewayResolver::refresher`]). A lookup
/// that misses the snapshot — an unknown gateway, or one onboarded since the
/// last refresh — falls back to a per-pubkey Trino query, cached to avoid
/// re-querying.
pub struct GatewayResolver {
    trino_client: trino_client::Client,
    inventory_table: String,
    known_gateways: Arc<RwLock<HashSet<PublicKeyBinary>>>,
    fallback_cache: Arc<Cache<PublicKeyBinary, bool>>,
    refresh_interval: Duration,
    /// When the startup snapshot was loaded (`None` if the load failed). Seeds
    /// the refresher's first-refresh timing via [`GatewayResolver::refresher`].
    snapshot_loaded_at: Option<Instant>,
}

impl GatewayResolver {
    pub async fn new(trino_client: trino_client::Client, refresh_interval: Duration) -> Self {
        Self::new_with_inventory_table(
            trino_client,
            MOBILE_HOTSPOT_INVENTORY_TABLE,
            refresh_interval,
        )
        .await
    }

    /// Like [`new`](Self::new), but with an explicit inventory table name.
    ///
    /// Tests use this to point at a per-test iceberg catalog. The test harness
    /// registers a uniquely-named catalog, so the production `network` catalog
    /// prefix isn't available; tests pass the two-part `chain.mobile_hotspot_inventory`
    /// name, which resolves against the Trino client's default catalog.
    pub async fn new_with_inventory_table(
        trino_client: trino_client::Client,
        inventory_table: impl Into<String>,
        refresh_interval: Duration,
    ) -> Self {
        let inventory_table = inventory_table.into();

        // Load every known gateway once at startup so the resolver is ready
        // before the daemon processes any files. A failure here is non-fatal: we
        // start with an empty snapshot and the refresher retries soon, with the
        // per-pubkey fallback covering lookups in the meantime.
        let (initial, snapshot_loaded_at) =
            match load_known_gateways(&trino_client, &inventory_table).await {
                Ok(set) => {
                    tracing::info!(count = set.len(), "loaded gateway inventory snapshot");
                    (set, Some(Instant::now()))
                }
                Err(err) => {
                    tracing::error!(
                        ?err,
                        "failed to load gateway inventory at startup; starting empty, will retry"
                    );
                    (HashSet::new(), None)
                }
            };
        let known_gateways = Arc::new(RwLock::new(initial));

        // Fallback cache for snapshot misses.
        let fallback_cache = Arc::new(Cache::new());
        let eviction = fallback_cache.clone();
        tokio::spawn(async move { eviction.monitor(4, 0.25, CACHE_EVICTION_FREQUENCY).await });

        Self {
            trino_client,
            inventory_table,
            known_gateways,
            fallback_cache,
            refresh_interval,
            snapshot_loaded_at,
        }
    }

    /// The background task that keeps this resolver's snapshot fresh. Register it
    /// with a [`task_manager::TaskManager`] via [`task_manager::periodic`].
    /// Intended to be called once, right after construction.
    pub fn refresher(&self) -> GatewaySnapshotRefresher {
        GatewaySnapshotRefresher {
            trino_client: self.trino_client.clone(),
            inventory_table: self.inventory_table.clone(),
            known_gateways: self.known_gateways.clone(),
            refresh_interval: self.refresh_interval,
            last_refresh: self.snapshot_loaded_at,
        }
    }

    pub async fn is_gateway_known(
        &self,
        public_key: &PublicKeyBinary,
        gateway_query_time: &DateTime<Utc>,
    ) -> bool {
        // Fast path: the startup/refresh snapshot of every known gateway.
        let in_snapshot = self.known_gateways.read().unwrap().contains(public_key);
        if in_snapshot {
            return true;
        }

        // Miss: an unknown gateway, or one onboarded since the last refresh.
        // Fall back to a per-pubkey query, cached to avoid re-querying.
        if let Some(cached) = self.fallback_cache.get(public_key).await {
            return *cached.value();
        }

        let is_known = match self
            .query_gateway_known(public_key, gateway_query_time)
            .await
        {
            Ok(is_known) => is_known,
            Err(err) => {
                // Don't cache transient failures — fail closed for this call only.
                tracing::warn!(?err, %public_key, "gateway fallback query failed");
                return false;
            }
        };
        self.fallback_cache
            .insert(public_key.clone(), is_known, self.refresh_interval)
            .await;
        is_known
    }

    async fn query_gateway_known(
        &self,
        public_key: &PublicKeyBinary,
        gateway_query_time: &DateTime<Utc>,
    ) -> trino_client::Result<bool> {
        use trino_rust_client::Trino;
        #[derive(Trino, serde::Serialize, serde::Deserialize)]
        struct Exists {
            is_known: bool,
        }

        let stmt = trino_client::Statement::new(format!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM {}
                WHERE pub_key = :address
                  AND inserted_at <= :timestamp
            ) AS is_known
            "#,
            self.inventory_table
        ))
        .bind("address", public_key.to_string())
        .bind("timestamp", *gateway_query_time)
        .typed::<Exists>();

        let rows = self.trino_client.get_all(stmt).await?;
        Ok(rows.first().is_some_and(|row| row.is_known))
    }
}

/// Periodically reloads the [`GatewayResolver`] snapshot from Trino. Registered
/// with the daemon's `TaskManager` via [`task_manager::periodic`], so it is
/// interleaved with file processing and stops on shutdown.
///
/// Ticks at [`GATEWAY_REFRESH_RETRY_INTERVAL`] but reloads only when due: once
/// per `refresh_interval` while healthy, and on every tick after a failed load,
/// so a Trino outage doesn't leave the snapshot stale for a full interval.
pub struct GatewaySnapshotRefresher {
    trino_client: trino_client::Client,
    inventory_table: String,
    known_gateways: Arc<RwLock<HashSet<PublicKeyBinary>>>,
    refresh_interval: Duration,
    last_refresh: Option<Instant>,
}

impl GatewaySnapshotRefresher {
    // Periodic tasks don't allow for changing schedules. So we check at the
    // minimum rate and early return when there's nothing to do. Should we add a
    // ScheduledTask?
    async fn should_run(&self) -> bool {
        match self.last_refresh {
            Some(last) => last.elapsed() >= self.refresh_interval,
            None => true,
        }
    }
}

impl Periodic for GatewaySnapshotRefresher {
    type Error = anyhow::Error;

    fn interval(&self) -> Duration {
        GATEWAY_REFRESH_RETRY_INTERVAL
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        if !self.should_run().await {
            return Ok(());
        }

        match load_known_gateways(&self.trino_client, &self.inventory_table).await {
            Ok(set) => {
                let count = set.len();
                *self.known_gateways.write().unwrap() = set;
                self.last_refresh = Some(Instant::now());
                tracing::info!(count, "refreshed gateway inventory snapshot");
            }
            Err(err) => tracing::warn!(
                ?err,
                "failed to refresh gateway inventory; keeping previous snapshot, retrying soon"
            ),
        }
        Ok(())
    }
}

/// Load every gateway pubkey from the inventory table into a set.
async fn load_known_gateways(
    trino_client: &trino_client::Client,
    inventory_table: &str,
) -> anyhow::Result<HashSet<PublicKeyBinary>> {
    use trino_rust_client::Trino;
    #[derive(Trino, serde::Serialize, serde::Deserialize)]
    struct Row {
        pub_key: String,
    }

    let stmt = trino_client::Statement::new(format!("SELECT pub_key FROM {inventory_table}"))
        .typed::<Row>();
    let rows = trino_client.get_all(stmt).await?;

    let mut known = HashSet::with_capacity(rows.len());
    for row in rows {
        match row.pub_key.parse::<PublicKeyBinary>() {
            Ok(pubkey) => {
                known.insert(pubkey);
            }
            Err(err) => {
                tracing::warn!(?err, pub_key = %row.pub_key, "skipping unparseable gateway pubkey")
            }
        }
    }
    Ok(known)
}
