use std::{
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use retainer::Cache;
use task_manager::Periodic;

use super::Heartbeat;

/// How far back the startup warm-up reads validated locations. Matches the
/// window a [`LastLocation`] stays usable for, so anything older would be
/// rejected by [`LastLocation::still_valid`] the moment it was read.
const LOOKBACK: Duration = Duration::hours(24);

/// How soon to retry a refresh after a failure, rather than waiting the full
/// interval. Mirrors the gateway snapshot refresher.
const REFRESH_RETRY_INTERVAL: StdDuration = StdDuration::from_secs(60);

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct LastLocation {
    pub location_validation_timestamp: DateTime<Utc>,
    pub heartbeat_timestamp: DateTime<Utc>,
    pub lat: f64,
    pub lon: f64,
}

impl LastLocation {
    pub fn new(
        location_validation_timestamp: DateTime<Utc>,
        heartbeat_timestamp: DateTime<Utc>,
        lat: f64,
        lon: f64,
    ) -> Self {
        Self {
            location_validation_timestamp,
            heartbeat_timestamp,
            lat,
            lon,
        }
    }

    pub fn from_heartbeat(
        heartbeat: &Heartbeat,
        location_validation_timestamp: DateTime<Utc>,
    ) -> Self {
        Self::new(
            location_validation_timestamp,
            heartbeat.timestamp,
            heartbeat.lat,
            heartbeat.lon,
        )
    }

    fn still_valid(&self, heartbeat_timestamp: DateTime<Utc>) -> bool {
        let diff = heartbeat_timestamp - self.location_validation_timestamp;
        diff <= LOOKBACK
    }
}

/// A cache of the last validated WiFi heartbeat location per hotspot.
///
/// Entries come from two places: heartbeats validated by this process (via
/// [`LocationCache::set`]), and a read of the last 24 hours of `poc.heartbeats`
/// out of Trino — once at startup ([`LocationCache::from_trino`]) and again on
/// an interval ([`LocationCacheRefresher`]). Together these replace what used to
/// be a per-hotspot `SELECT` against the `wifi_heartbeats` Postgres table on
/// cache miss; heartbeats are no longer written to Postgres at all, so Iceberg is
/// the only place that history exists.
///
/// The periodic reload matters because the cache is process-local while the old
/// Postgres table was shared: a location validated by *another* instance — during
/// a rolling deploy, say — is invisible here until the next reload picks it up
/// out of Trino.
///
/// A miss means "no validated location in the window", which is exactly what the
/// old query returned when it found no row.
#[derive(Clone)]
pub struct LocationCache {
    locations: Arc<Cache<PublicKeyBinary, LastLocation>>,
}

impl LocationCache {
    /// An empty cache. Entries accumulate as heartbeats are validated.
    pub fn new() -> Self {
        let locations = Arc::new(Cache::new());
        let locations_clone = locations.clone();
        tokio::spawn(async move {
            locations_clone
                .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 24))
                .await
        });
        Self { locations }
    }

    /// An empty cache warmed with the most recent validated location for every
    /// hotspot that reported one in the last `LOOKBACK`.
    ///
    /// A warm-up failure is fatal. Starting with an empty cache would deny a
    /// location to every hotspot that asserted one before startup, publishing a
    /// zero location-trust score for each — a fleet-wide misstatement caused by
    /// our own outage rather than by anything the hotspots did. Refusing to start
    /// is the honest response.
    pub async fn from_trino(trino: &trino_client::Client) -> anyhow::Result<Self> {
        let cache = Self::new();
        let loaded = cache
            .warm(trino)
            .await
            .context("warming last-location cache from trino")?;
        tracing::info!(loaded, "warmed last-location cache from trino");
        Ok(cache)
    }

    /// A companion task that reloads the cache from Trino every
    /// `refresh_interval`, picking up locations validated elsewhere.
    pub fn refresher(
        &self,
        trino: trino_client::Client,
        refresh_interval: StdDuration,
    ) -> LocationCacheRefresher {
        LocationCacheRefresher {
            trino,
            cache: self.clone(),
            refresh_interval,
            last_refresh: Some(Instant::now()),
        }
    }

    async fn warm(&self, trino: &trino_client::Client) -> anyhow::Result<usize> {
        let rows =
            crate::iceberg::heartbeat::latest_validated_locations(trino, Utc::now() - LOOKBACK)
                .await?;

        let mut loaded = 0;
        for row in rows {
            let hotspot: PublicKeyBinary = match row.hotspot_pubkey.parse() {
                Ok(hotspot) => hotspot,
                Err(err) => {
                    tracing::warn!(
                        pubkey = row.hotspot_pubkey,
                        ?err,
                        "skipping unparsable hotspot key while warming last-location cache"
                    );
                    continue;
                }
            };
            let last = LastLocation::new(
                row.location_validation_timestamp.with_timezone(&Utc),
                row.received_timestamp.with_timezone(&Utc),
                row.lat,
                row.lon,
            );
            if self.cache_if_newer(&hotspot, last).await {
                loaded += 1;
            }
        }

        Ok(loaded)
    }

    pub async fn set(&self, hotspot: &PublicKeyBinary, last_location: LastLocation) {
        self.cache_last_location(hotspot, last_location).await;
    }

    pub async fn get(
        &self,
        hotspot: &PublicKeyBinary,
        heartbeat_timestamp: DateTime<Utc>,
    ) -> Option<LastLocation> {
        // The value may still be cached according to the system clock but not
        // valid based on the time of the heartbeat in question.
        let last = *self.locations.get(hotspot).await?;
        Some(last).filter(|l| l.still_valid(heartbeat_timestamp))
    }

    /// Cache `last_location` only if it is more recent than what is already
    /// held.
    ///
    /// A reload is a merge, not a replacement: entries set from live heartbeats
    /// are newer than anything Trino can return for them, because `set` runs
    /// while a file is being processed and the matching Iceberg write only lands
    /// once the whole file is done. Blindly inserting would walk those hotspots
    /// backwards to their previous location.
    async fn cache_if_newer(&self, hotspot: &PublicKeyBinary, candidate: LastLocation) -> bool {
        // Copy out and drop the guard before inserting.
        let existing = self.locations.get(hotspot).await.map(|held| *held);
        if let Some(existing) = existing {
            if existing.heartbeat_timestamp >= candidate.heartbeat_timestamp {
                return false;
            }
        }

        self.cache_last_location(hotspot, candidate).await;
        true
    }

    /// Cache `last_location`.
    ///
    /// The TTL only reclaims memory — whether an entry may actually be used is
    /// decided per heartbeat by [`LastLocation::still_valid`], which compares the
    /// validation timestamp against the *heartbeat's* timestamp rather than the
    /// wall clock. Heartbeat files are processed with a lookback, so a heartbeat
    /// can be hours behind now and still be entitled to a location whose
    /// wall-clock validity has lapsed.
    async fn cache_last_location(&self, hotspot: &PublicKeyBinary, last_location: LastLocation) {
        self.locations
            .insert(
                hotspot.clone(),
                last_location,
                LOOKBACK.to_std().expect("positive lookback"),
            )
            .await;
    }
}

/// Reloads [`LocationCache`] from Trino on an interval. See
/// [`LocationCache::refresher`].
pub struct LocationCacheRefresher {
    trino: trino_client::Client,
    cache: LocationCache,
    refresh_interval: StdDuration,
    last_refresh: Option<Instant>,
}

impl LocationCacheRefresher {
    // Periodic tasks don't allow for changing schedules, so tick at the retry
    // rate and early return when a full interval hasn't elapsed.
    fn should_run(&self) -> bool {
        match self.last_refresh {
            Some(last) => last.elapsed() >= self.refresh_interval,
            None => true,
        }
    }
}

impl Periodic for LocationCacheRefresher {
    type Error = anyhow::Error;

    fn interval(&self) -> StdDuration {
        REFRESH_RETRY_INTERVAL
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        if !self.should_run() {
            return Ok(());
        }

        // Unlike the startup load, a failed refresh is not fatal: the cache is
        // still populated and correct for everything this process has seen, it
        // just may lag another instance until the next attempt.
        match self.cache.warm(&self.trino).await {
            Ok(loaded) => {
                self.last_refresh = Some(Instant::now());
                tracing::info!(loaded, "refreshed last-location cache from trino");
            }
            Err(err) => tracing::warn!(
                ?err,
                "failed to refresh last-location cache; keeping existing entries, retrying soon"
            ),
        }

        Ok(())
    }
}

impl Default for LocationCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{Duration, Utc};
    use helium_crypto::PublicKeyBinary;

    fn test_last_location(
        latest_timestamp: DateTime<Utc>,
        location_validation_timestamp: DateTime<Utc>,
    ) -> LastLocation {
        LastLocation {
            location_validation_timestamp,
            heartbeat_timestamp: latest_timestamp,
            lat: 0.0,
            lon: 0.0,
        }
    }

    #[tokio::test]
    async fn validation_timestamps_expire_after_24_hours() {
        let now = Utc::now();
        let cache = LocationCache::new();

        // Right at the 24 hour limit: still usable.
        let hotspot_limit = PublicKeyBinary::from(vec![2]);
        let limit_timestamp = now - Duration::hours(24);
        cache
            .set(&hotspot_limit, test_last_location(now, limit_timestamp))
            .await;

        let hotspot_good = PublicKeyBinary::from(vec![3]);
        let good_timestamp = now - Duration::hours(12);
        cache
            .set(&hotspot_good, test_last_location(now, good_timestamp))
            .await;

        assert_eq!(
            Some(test_last_location(now, limit_timestamp)),
            cache.get(&hotspot_limit, now).await,
            "Limit timestamp current"
        );
        assert_eq!(
            Some(test_last_location(now, good_timestamp)),
            cache.get(&hotspot_good, now).await,
            "Good timestamp current"
        );

        // Moving a day into the future should invalidate all timestamps
        // regardless of what has already been cached.
        let future = now + Duration::days(1);
        assert_eq!(
            None,
            cache.get(&hotspot_limit, future).await,
            "Limit timestamp future"
        );
        assert_eq!(
            None,
            cache.get(&hotspot_good, future).await,
            "Good timestamp future"
        );
    }

    #[tokio::test]
    async fn will_not_serve_invalid_validation_timestamps() {
        let cache = LocationCache::new();

        let now = Utc::now();
        let validation_timestamp = now - Duration::hours(25);

        let hotspot = PublicKeyBinary::from(vec![1]);
        let invalid_location = test_last_location(now, validation_timestamp);
        cache.set(&hotspot, invalid_location).await;

        assert_eq!(None, cache.get(&hotspot, now).await);
    }

    #[tokio::test]
    async fn will_cache_valid_validation_timestamps() {
        let cache = LocationCache::new();

        let now = Utc::now();
        let validation_timestamp = now - Duration::hours(12);

        let hotspot = PublicKeyBinary::from(vec![1]);
        let valid_location = test_last_location(now, validation_timestamp);
        cache.set(&hotspot, valid_location).await;

        assert_eq!(Some(valid_location), cache.get(&hotspot, now).await);
    }

    /// Heartbeat files are processed with a lookback, so a heartbeat can be
    /// hours behind the wall clock. Validity is judged against the heartbeat's
    /// own timestamp, not against now — a location that lapsed an hour ago is
    /// still good for a heartbeat that predates the lapse.
    #[tokio::test]
    async fn lagging_heartbeat_still_gets_a_recently_lapsed_location() {
        let now = Utc::now();
        let cache = LocationCache::new();

        let hotspot = PublicKeyBinary::from(vec![7]);
        let validation_timestamp = now - Duration::hours(25);
        cache
            .set(&hotspot, test_last_location(now, validation_timestamp))
            .await;

        // Judged against now, this location expired an hour ago.
        assert_eq!(None, cache.get(&hotspot, now).await);

        // Judged against a heartbeat from two hours ago, it was still valid.
        assert_eq!(
            Some(test_last_location(now, validation_timestamp)),
            cache.get(&hotspot, now - Duration::hours(2)).await,
        );
    }

    /// A reload must not walk a hotspot backwards: `set` runs during file
    /// processing, so a live entry is newer than anything Trino can yet return.
    #[tokio::test]
    async fn a_reload_does_not_overwrite_a_newer_live_entry() {
        let now = Utc::now();
        let cache = LocationCache::new();
        let hotspot = PublicKeyBinary::from(vec![4]);

        let live = LastLocation::new(now - Duration::hours(1), now, 1.0, 2.0);
        cache.set(&hotspot, live).await;

        // What a reload would carry: the same hotspot, but an older heartbeat.
        let stale = LastLocation::new(now - Duration::hours(3), now - Duration::hours(2), 9.0, 9.0);
        assert!(
            !cache.cache_if_newer(&hotspot, stale).await,
            "older row should be skipped"
        );
        assert_eq!(Some(live), cache.get(&hotspot, now).await);
    }

    #[tokio::test]
    async fn a_reload_adopts_a_newer_entry() {
        let now = Utc::now();
        let cache = LocationCache::new();
        let hotspot = PublicKeyBinary::from(vec![5]);

        let old = LastLocation::new(now - Duration::hours(3), now - Duration::hours(2), 1.0, 2.0);
        cache.set(&hotspot, old).await;

        // A location another instance validated more recently.
        let newer = LastLocation::new(now - Duration::hours(1), now, 9.0, 9.0);
        assert!(cache.cache_if_newer(&hotspot, newer).await);
        assert_eq!(Some(newer), cache.get(&hotspot, now).await);
    }

    #[tokio::test]
    async fn a_reload_adds_hotspots_it_has_never_seen() {
        let now = Utc::now();
        let cache = LocationCache::new();
        let hotspot = PublicKeyBinary::from(vec![6]);

        let from_elsewhere = LastLocation::new(now - Duration::hours(1), now, 3.0, 4.0);
        assert!(cache.cache_if_newer(&hotspot, from_elsewhere).await);
        assert_eq!(Some(from_elsewhere), cache.get(&hotspot, now).await);
    }

    #[tokio::test]
    async fn missing_hotspot_has_no_location() {
        let cache = LocationCache::new();
        let hotspot = PublicKeyBinary::from(vec![9]);

        assert_eq!(None, cache.get(&hotspot, Utc::now()).await);
    }
}
