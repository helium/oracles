//! Trino-backed gateway resolution.
//!
//! [`GatewayResolver`] answers "what is this gateway" for heartbeat and speedtest
//! verification — its device type and asserted location. Gateway state is served
//! from an in-memory snapshot of the on-chain inventory table (loaded at startup,
//! kept fresh by [`GatewaySnapshotRefresher`]) with a per-pubkey Trino fallback.
//!
//! This replaces the mobile-config gRPC gateway client. It mirrors the
//! mobile-packet-verifier resolver (`mobile_packet_verifier::gateway`), but keeps
//! device type + location per pubkey rather than a bare "known" set.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chrono::{DateTime, FixedOffset, Utc};
use helium_crypto::PublicKeyBinary;
use retainer::Cache;
use task_manager::Periodic;

/// Fully-qualified `catalog.schema.table` for the on-chain mobile hotspot
/// inventory queried by [`GatewayResolver::resolve_gateway`].
pub const MOBILE_HOTSPOT_INVENTORY_TABLE: &str = "network.chain.mobile_hotspot_inventory";

/// How often the fallback cache sweeps expired entries.
const CACHE_EVICTION_FREQUENCY: Duration = Duration::from_mins(60);

/// How soon to retry the inventory snapshot load after a failure, rather than
/// waiting the full `refresh_interval`. Keeps a Trino outage from leaving the
/// snapshot degraded for up to an hour.
const GATEWAY_REFRESH_RETRY_INTERVAL: Duration = Duration::from_mins(1);

/// The device type of an on-chain mobile hotspot. Local copy of what
/// mobile-config used to serve; parsed from the inventory table's `device_type`
/// column (snake_case, e.g. `wifi_indoor`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum DeviceType {
    Cbrs,
    WifiIndoor,
    WifiOutdoor,
    WifiDataOnly,
}

impl DeviceType {
    /// The inventory table's `device_type` string for this variant. Inverse of
    /// [`DeviceType::from_inventory`].
    pub fn as_str(&self) -> &'static str {
        match self {
            DeviceType::Cbrs => "CBRS",
            DeviceType::WifiIndoor => "WIFI_INDOOR",
            DeviceType::WifiOutdoor => "WIFI_OUTDOOR",
            DeviceType::WifiDataOnly => "WIFI_DATA_ONLY",
        }
    }

    /// Parse the `device_type` value stored in `mobile_hotspot_inventory`.
    /// Returns `None` for an unrecognized value.
    pub fn from_inventory(s: &str) -> Option<Self> {
        match s {
            "CBRS" => Some(DeviceType::Cbrs),
            "WIFI_INDOOR" => Some(DeviceType::WifiIndoor),
            "WIFI_OUTDOOR" => Some(DeviceType::WifiOutdoor),
            "WIFI_DATA_ONLY" => Some(DeviceType::WifiDataOnly),
            _ => None,
        }
    }
}

/// The outcome of resolving a gateway, in the precedence heartbeat/speedtest
/// verification expects: not-on-chain, data-only, asserted (with location +
/// device type), or on-chain-but-unasserted.
#[derive(Copy, Clone, Debug)]
pub enum GatewayResolution {
    GatewayNotFound,
    GatewayNotAsserted,
    AssertedLocation(u64, DeviceType),
    DataOnly,
}

impl GatewayResolution {
    pub fn is_not_found(&self) -> bool {
        matches!(self, GatewayResolution::GatewayNotFound)
    }
}

#[async_trait::async_trait]
pub trait GatewayResolver: Clone + Send + Sync + 'static {
    async fn resolve_gateway(
        &self,
        address: &PublicKeyBinary,
        gateway_query_timestamp: &DateTime<Utc>,
    ) -> anyhow::Result<GatewayResolution>;
}

/// A gateway's resolvable state: its device type and, when asserted, its
/// location (H3 cell as `u64`). `location == None` means on-chain but unasserted.
#[derive(Copy, Clone, Debug)]
struct GatewayMeta {
    device_type: DeviceType,
    location: Option<u64>,
}

#[derive(Copy, Clone, Debug)]
struct GatewayEntry {
    inserted_at: DateTime<Utc>,
    meta: GatewayMeta,
}

impl GatewayEntry {
    fn resolve_at(&self, gateway_query_time: &DateTime<Utc>) -> Option<GatewayMeta> {
        (self.inserted_at <= *gateway_query_time).then_some(self.meta)
    }
}

impl GatewayMeta {
    fn resolution(&self) -> GatewayResolution {
        // Precedence matches the old mobile-config resolver: data-only first,
        // then asserted-with-location, else unasserted.
        if self.device_type == DeviceType::WifiDataOnly {
            GatewayResolution::DataOnly
        } else if let Some(location) = self.location {
            GatewayResolution::AssertedLocation(location, self.device_type)
        } else {
            GatewayResolution::GatewayNotAsserted
        }
    }
}

/// Resolver used by the running daemon.
///
/// Gateway state is served from an in-memory snapshot of every pubkey in the
/// inventory table, loaded at startup and kept fresh by a companion
/// [`GatewaySnapshotRefresher`] (see [`TrinoGatewayResolver::refresher`]).
/// A lookup that misses the snapshot — an unknown gateway, or one onboarded
/// since the last refresh — falls back to a per-pubkey Trino query, cached to
/// avoid re-querying.
#[derive(Clone)]
pub struct TrinoGatewayResolver {
    trino_client: trino_client::Client,
    inventory_table: String,
    known_gateways: Arc<RwLock<HashMap<PublicKeyBinary, GatewayEntry>>>,
    fallback_cache: Arc<Cache<PublicKeyBinary, Option<GatewayEntry>>>,
    refresh_interval: Duration,
    /// When the startup snapshot was loaded (`None` if the load failed). Seeds
    /// the refresher's first-refresh timing via [`TrinoGatewayResolver::refresher`].
    snapshot_loaded_at: Option<Instant>,
}

impl TrinoGatewayResolver {
    pub async fn new(trino_client: trino_client::Client, refresh_interval: Duration) -> Self {
        Self::new_with_inventory_table(
            trino_client,
            MOBILE_HOTSPOT_INVENTORY_TABLE,
            refresh_interval,
        )
        .await
    }

    /// Like [`new`](Self::new), but with an explicit inventory table name. Tests
    /// use this to point at a per-test iceberg catalog (a two-part
    /// `chain.mobile_hotspot_inventory` name resolves against the Trino client's
    /// default catalog).
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
                Ok(map) => {
                    tracing::info!(count = map.len(), "loaded gateway inventory snapshot");
                    (map, Some(Instant::now()))
                }
                Err(err) => {
                    tracing::error!(
                        ?err,
                        "failed to load gateway inventory at startup; starting empty, will retry"
                    );
                    (HashMap::new(), None)
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

    async fn resolve_meta(
        &self,
        public_key: &PublicKeyBinary,
        gateway_query_time: &DateTime<Utc>,
    ) -> anyhow::Result<Option<GatewayMeta>> {
        // Fast path: the startup/refresh snapshot of every known gateway.
        if let Some(entry) = self.known_gateways.read().unwrap().get(public_key).copied() {
            return Ok(entry.resolve_at(gateway_query_time));
        }

        // Miss: an unknown gateway, or one onboarded since the last refresh.
        // Fall back to a per-pubkey query, cached to avoid re-querying.
        if let Some(cached) = self.fallback_cache.get(public_key).await {
            return Ok(cached
                .value()
                .and_then(|entry| entry.resolve_at(gateway_query_time)));
        }

        let entry = self.query_gateway(public_key).await?;
        self.fallback_cache
            .insert(public_key.clone(), entry, self.refresh_interval)
            .await;
        Ok(entry.and_then(|entry| entry.resolve_at(gateway_query_time)))
    }

    async fn query_gateway(
        &self,
        public_key: &PublicKeyBinary,
    ) -> anyhow::Result<Option<GatewayEntry>> {
        use trino_rust_client::Trino;
        #[derive(Trino, serde::Serialize, serde::Deserialize)]
        struct Row {
            inserted_at: DateTime<FixedOffset>,
            device_type: String,
            // Nullable: an unasserted gateway has a NULL `asserted_hex`.
            asserted_hex: Option<String>,
        }

        // The inventory table has one row per pubkey (unique index on `pub_key`).
        let stmt = trino_client::Statement::new(format!(
            r#"
            SELECT inserted_at, device_type, asserted_hex
            FROM {}
            WHERE pub_key = :address
            "#,
            self.inventory_table
        ))
        .bind("address", public_key.to_string())
        .typed::<Row>();

        let rows = self.trino_client.get_all(stmt).await?;
        Ok(rows.into_iter().next().and_then(|row| {
            row_to_meta(&row.device_type, row.asserted_hex.as_deref()).map(|meta| GatewayEntry {
                inserted_at: row.inserted_at.with_timezone(&Utc),
                meta,
            })
        }))
    }
}

#[async_trait::async_trait]
impl GatewayResolver for TrinoGatewayResolver {
    async fn resolve_gateway(
        &self,
        address: &PublicKeyBinary,
        gateway_query_timestamp: &DateTime<Utc>,
    ) -> anyhow::Result<GatewayResolution> {
        Ok(
            match self.resolve_meta(address, gateway_query_timestamp).await? {
                Some(meta) => meta.resolution(),
                None => GatewayResolution::GatewayNotFound,
            },
        )
    }
}

/// Periodically reloads the [`TrinoGatewayResolver`] snapshot from Trino.
/// Registered with the daemon's `TaskManager` via [`task_manager::periodic`], so
/// it is interleaved with file processing and stops on shutdown.
///
/// Ticks at [`GATEWAY_REFRESH_RETRY_INTERVAL`] but reloads only when due: once
/// per `refresh_interval` while healthy, and on every tick after a failed load,
/// so a Trino outage doesn't leave the snapshot stale for a full interval.
pub struct GatewaySnapshotRefresher {
    trino_client: trino_client::Client,
    inventory_table: String,
    known_gateways: Arc<RwLock<HashMap<PublicKeyBinary, GatewayEntry>>>,
    refresh_interval: Duration,
    last_refresh: Option<Instant>,
}

impl GatewaySnapshotRefresher {
    // Periodic tasks don't allow for changing schedules. So we check at the
    // minimum rate and early return when there's nothing to do.
    fn should_run(&self) -> bool {
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
        if !self.should_run() {
            return Ok(());
        }

        match load_known_gateways(&self.trino_client, &self.inventory_table).await {
            Ok(map) => {
                let count = map.len();
                *self.known_gateways.write().unwrap() = map;
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

/// Build a [`GatewayMeta`] from the inventory's raw `device_type` / `asserted_hex`
/// columns. Returns `None` for an unparseable device type (treated as unknown).
/// A NULL (or empty) `asserted_hex` means the gateway is on-chain but unasserted.
fn row_to_meta(device_type: &str, asserted_hex: Option<&str>) -> Option<GatewayMeta> {
    let device_type = DeviceType::from_inventory(device_type)?;
    let location = asserted_hex.and_then(parse_asserted_location);
    Some(GatewayMeta {
        device_type,
        location,
    })
}

/// Parse the inventory's `asserted_hex` column into an H3 cell index. The column
/// holds the H3 index as a hex string (e.g. `8c2681a3064d9ff`); a NULL (handled
/// by the caller) or empty value means unasserted. An unparseable non-empty
/// value is logged and treated as unasserted.
fn parse_asserted_location(asserted_hex: &str) -> Option<u64> {
    let trimmed = asserted_hex
        .trim()
        .strip_prefix("0x")
        .unwrap_or(asserted_hex.trim());
    if trimmed.is_empty() {
        return None;
    }
    match u64::from_str_radix(trimmed, 16) {
        Ok(location) => Some(location),
        Err(err) => {
            tracing::warn!(
                ?err,
                asserted_hex,
                "unparseable asserted_hex; treating as unasserted"
            );
            None
        }
    }
}

/// Load every gateway's state from the inventory table into a map keyed by
/// pubkey. The table has one row per pubkey (unique index on `pub_key`).
async fn load_known_gateways(
    trino_client: &trino_client::Client,
    inventory_table: &str,
) -> anyhow::Result<HashMap<PublicKeyBinary, GatewayEntry>> {
    use trino_rust_client::Trino;
    #[derive(Trino, serde::Serialize, serde::Deserialize)]
    struct Row {
        pub_key: String,
        inserted_at: DateTime<FixedOffset>,
        device_type: String,
        // Nullable: an unasserted gateway has a NULL `asserted_hex`.
        asserted_hex: Option<String>,
    }

    let stmt = trino_client::Statement::new(format!(
        "SELECT pub_key, inserted_at, device_type, asserted_hex FROM {inventory_table}"
    ))
    .typed::<Row>();
    let rows = trino_client.get_all(stmt).await?;

    let mut known = HashMap::with_capacity(rows.len());
    for row in rows {
        let pubkey = match row.pub_key.parse::<PublicKeyBinary>() {
            Ok(pubkey) => pubkey,
            Err(err) => {
                tracing::warn!(?err, pub_key = %row.pub_key, "skipping unparseable gateway pubkey");
                continue;
            }
        };
        if let Some(meta) = row_to_meta(&row.device_type, row.asserted_hex.as_deref()) {
            known.insert(
                pubkey,
                GatewayEntry {
                    inserted_at: row.inserted_at.with_timezone(&Utc),
                    meta,
                },
            );
        }
    }
    Ok(known)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_entry_respects_inserted_at() {
        let inserted_at = Utc::now();
        let entry = GatewayEntry {
            inserted_at,
            meta: GatewayMeta {
                device_type: DeviceType::WifiOutdoor,
                location: None,
            },
        };

        assert!(entry
            .resolve_at(&(inserted_at - chrono::Duration::seconds(1)))
            .is_none());
        assert!(matches!(
            entry.resolve_at(&inserted_at),
            Some(GatewayMeta {
                device_type: DeviceType::WifiOutdoor,
                location: None,
            })
        ));
    }

    #[tokio::test]
    #[ignore = "manual query validation"]
    async fn connect() -> anyhow::Result<()> {
        // Test load_known_gateways against real trino backend.
        // Grab the creds and insert them here, DO NOT COMMIT!!!

        let trino_client = trino_client::Client::from_settings(&trino_client::Settings {
            host: "xxx".to_string(),
            port: 443,
            user: "xxx".to_string(),
            catalog: None,
            schema: None,
            secure: true,
            insecure_skip_tls_verify: false,
            auth: Some(trino_client::AuthSettings::Basic {
                username: "xxx".to_string(),
                password: Some("xxx".to_string()),
            }),
        })?;
        let map = load_known_gateways(&trino_client, MOBILE_HOTSPOT_INVENTORY_TABLE).await?;
        // println!("map count: {}", map.len());
        assert!(!map.is_empty());
        Ok(())
    }

    #[test]
    fn device_type_round_trips_inventory_strings() {
        for dt in [
            DeviceType::Cbrs,
            DeviceType::WifiIndoor,
            DeviceType::WifiOutdoor,
            DeviceType::WifiDataOnly,
        ] {
            assert_eq!(DeviceType::from_inventory(dt.as_str()), Some(dt));
        }
        assert_eq!(DeviceType::from_inventory("nonsense"), None);
    }

    #[test]
    fn asserted_hex_parsing() {
        assert_eq!(
            parse_asserted_location("8c2681a3064d9ff"),
            Some(0x8c2681a3064d9ff)
        );
        assert_eq!(
            parse_asserted_location("0x8c2681a3064d9ff"),
            Some(0x8c2681a3064d9ff)
        );
        assert_eq!(parse_asserted_location(""), None);
        assert_eq!(parse_asserted_location("   "), None);
        assert_eq!(parse_asserted_location("not-hex"), None);
    }

    #[test]
    fn meta_resolution_precedence() {
        // data-only beats everything, even with a location.
        let data_only = GatewayMeta {
            device_type: DeviceType::WifiDataOnly,
            location: Some(1),
        };
        assert!(matches!(
            data_only.resolution(),
            GatewayResolution::DataOnly
        ));

        let asserted = GatewayMeta {
            device_type: DeviceType::WifiIndoor,
            location: Some(42),
        };
        assert!(matches!(
            asserted.resolution(),
            GatewayResolution::AssertedLocation(42, DeviceType::WifiIndoor)
        ));

        let unasserted = GatewayMeta {
            device_type: DeviceType::WifiOutdoor,
            location: None,
        };
        assert!(matches!(
            unasserted.resolution(),
            GatewayResolution::GatewayNotAsserted
        ));
    }
}
