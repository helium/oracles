use anyhow::bail;
use chrono::{DateTime, Utc};
use file_store::radio_location_estimates::Entity;
use helium_crypto::{KeyTag, Keypair, PublicKey, PublicKeyBinary};
use mobile_verifier::{
    heartbeats::location_cache::{LocationCache, LocationCacheKey, LocationCacheValue},
    radio_location_estimates::hash_key,
};
use proptest::{
    prelude::{Just, Strategy},
    prop_oneof,
    strategy::ValueTree,
};
use rand::{rngs::OsRng, Rng};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::time::Instant;

#[sqlx::test]
async fn test_get_untrusted_radious(pool: PgPool) -> anyhow::Result<()> {
    let setup = Instant::now();

    let location_cache = LocationCache::new(&pool).await?;

    let hotspot_n = 100_000;
    let max_estimates_per_hotspot = 5;
    let mut rng = rand::thread_rng();

    for _ in 0..hotspot_n {
        let key = generate_keypair();
        let public_key = key.public_key().to_owned();
        let value = LocationCacheValueTest::generate();
        location_cache
            .insert(
                LocationCacheKey::WifiPubKey(PublicKeyBinary::from(public_key.clone().to_vec())),
                value.to_location_cache_value(),
            )
            .await?;

        let n = rng.gen_range(1..=max_estimates_per_hotspot);

        for _ in 0..n {
            let estimate =
                RadioLocationEstimateTest::generate(public_key.clone(), value.lat, value.lon);
            estimate.insert(&pool).await?
        }
    }
    println!("Setup duration is: {:?}", setup.elapsed());

    let fn_run = Instant::now();
    let result = mobile_verifier::rewarder::get_untrusted_radios(&pool, &location_cache).await?;

    println!(
        "Time elapsed in mobile_verifier::rewarder::get_untrusted_radious() is: {:?}",
        fn_run.elapsed()
    );
    println!("Result size is {:?}", result.len());

    assert!(false);
    Ok(())
}

#[derive(Debug, Clone)]
pub struct LocationCacheValueTest {
    pub lat: f64,
    pub lon: f64,
    pub timestamp: DateTime<Utc>,
}

impl LocationCacheValueTest {
    pub fn generate() -> Self {
        LocationCacheValueTest::strategy()
            .new_tree(&mut Default::default())
            .unwrap()
            .current()
    }

    fn strategy() -> impl Strategy<Value = Self> {
        (
            -180.0..180.0f64, // lat
            -90.0..90.0f64,   // lon
            timestamp(),      // timestamp,
        )
            .prop_map(|(lat, lon, timestamp)| LocationCacheValueTest {
                lat,
                lon,
                timestamp,
            })
    }

    pub fn to_location_cache_value(&self) -> LocationCacheValue {
        LocationCacheValue {
            lat: self.lat,
            lon: self.lon,
            timestamp: self.timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RadioLocationEstimateTest {
    pub hashed_key: String,
    pub radio_type: String,
    pub radio_key: String,
    pub received_timestamp: DateTime<Utc>,
    pub radius: f64,
    pub lat: f64,
    pub lon: f64,
    pub confidence: f64,
    pub invalided_at: Option<DateTime<Utc>>,
}

impl RadioLocationEstimateTest {
    pub fn generate(public_key: PublicKey, lat: f64, lon: f64) -> Self {
        RadioLocationEstimateTest::strategy(public_key, lat, lon)
            .new_tree(&mut Default::default())
            .unwrap()
            .current()
    }

    pub async fn insert(&self, pool: &PgPool) -> anyhow::Result<()> {
        let query = format!(
            r#"
            INSERT INTO radio_location_estimates (
                hashed_key,
                radio_type,
                radio_key,
                received_timestamp,
                radius,
                lat,
                lon,
                confidence,
                inserted_at
            ) VALUES (
                '{}', '{}', '{}', '{}', {}, {}, {}, {}, NOW()
            );"#,
            self.hashed_key,
            self.radio_type,
            self.radio_key,
            self.received_timestamp,
            self.radius,
            self.lat,
            self.lon,
            self.confidence,
        );

        sqlx::query(&query).execute(pool).await?;
        Ok(())
    }

    fn strategy(public_key: PublicKey, lat: f64, lon: f64) -> impl Strategy<Value = Self> {
        let lat_variation = 0.0001..=0.01; // Range for latitude variation
        let lon_variation = 0.0001..=0.01; // Range for longitude variation

        (
            prop_oneof!["wifi"], // radio_type,
            Just(public_key),    // public_key
            timestamp(),         // received_timestamp,
            0.0..5000.0f64,      //radius
            lat_variation.prop_flat_map(move |v: f64| {
                prop_oneof![Just(v), Just(-v)].prop_map(move |delta| lat + delta)
            }), // Randomly add or subtract variation
            lon_variation.prop_flat_map(move |v: f64| {
                prop_oneof![Just(v), Just(-v)].prop_map(move |delta| lon + delta)
            }), // Randomly add or subtract variation
            0.5..0.99f64,        // confidence
        )
            .prop_map(
                |(radio_type, public_key, received_timestamp, radius, lat, lon, confidence)| {
                    let hashed_key = hashed_key(&public_key, received_timestamp, radius, lat, lon);
                    let radio_key = public_key.to_string();
                    RadioLocationEstimateTest {
                        hashed_key,
                        radio_type,
                        radio_key,
                        received_timestamp,
                        radius,
                        lat,
                        lon,
                        confidence,
                        invalided_at: None,
                    }
                },
            )
    }
}

pub fn timestamp() -> impl Strategy<Value = DateTime<Utc>> {
    Just(Utc::now())
}

fn generate_keypair() -> Keypair {
    Keypair::generate(KeyTag::default(), &mut OsRng)
}

fn hashed_key(
    public_key: &PublicKey,
    received_timestamp: DateTime<Utc>,
    radius: f64,
    lat: f64,
    lon: f64,
) -> String {
    hash_key(
        &Entity::WifiPubKey(PublicKeyBinary::from(public_key.to_vec())),
        received_timestamp,
        f64_to_decimal(radius).unwrap(),
        f64_to_decimal(lat).unwrap(),
        f64_to_decimal(lon).unwrap(),
    )
}

fn f64_to_decimal(value: f64) -> anyhow::Result<Decimal> {
    match Decimal::try_from(value) {
        Ok(decimal_value) => Ok(decimal_value),
        Err(_) => bail!("Conversion from f64 to Decimal failed."),
    }
}
