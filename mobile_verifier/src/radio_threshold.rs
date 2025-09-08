use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use sqlx::Postgres;
use std::{collections::HashSet, ops::Range};

#[derive(Debug, Clone, Default)]
pub struct VerifiedRadioThresholds {
    gateways: HashSet<PublicKeyBinary>,
}

impl VerifiedRadioThresholds {
    pub fn insert(&mut self, hotspot_key: PublicKeyBinary) {
        self.gateways.insert(hotspot_key);
    }

    pub fn is_verified(&self, key: PublicKeyBinary) -> bool {
        self.gateways.contains(&key)
    }
}

pub async fn verified_radio_thresholds(
    pool: &sqlx::Pool<Postgres>,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<VerifiedRadioThresholds, sqlx::Error> {
    let gateways = sqlx::query_scalar::<_, PublicKeyBinary>(
        "SELECT hotspot_pubkey FROM radio_threshold WHERE threshold_timestamp < $1",
    )
    .bind(reward_period.end)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect::<HashSet<_>>();

    Ok(VerifiedRadioThresholds { gateways })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use helium_crypto::{KeyTag, Keypair};
    use rand::rngs::OsRng;
    use sqlx::PgPool;

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }

    #[sqlx::test]
    async fn test_verified_radio_thresholds(pool: PgPool) {
        // Setup: Create radio thresholds with different timestamps
        let now = Utc::now();

        // Create two hotspot keypairs
        let hotspot_keypair1 = generate_keypair();
        let hotspot_pubkey1 = PublicKeyBinary::from(hotspot_keypair1.public_key().to_owned());

        let hotspot_keypair2 = generate_keypair();
        let hotspot_pubkey2 = PublicKeyBinary::from(hotspot_keypair2.public_key().to_owned());

        // Insert test data directly into the database
        // Hotspot 1 - before the reward period end (should be verified)
        sqlx::query(
            r#"
            INSERT INTO radio_threshold (
                hotspot_pubkey,
                bytes_threshold,
                subscriber_threshold,
                threshold_timestamp,
                threshold_met,
                recv_timestamp
            ) VALUES ($1, $2, $3, $4, true, $5)
            "#,
        )
        .bind(hotspot_pubkey1.to_string())
        .bind(1000i64)
        .bind(50i32)
        .bind(now - chrono::Duration::hours(5)) // Before the reward period end
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        // Hotspot 2 - after the reward period end (should not be verified)
        sqlx::query(
            r#"
            INSERT INTO radio_threshold (
                hotspot_pubkey,
                bytes_threshold,
                subscriber_threshold,
                threshold_timestamp,
                threshold_met,
                recv_timestamp
            ) VALUES ($1, $2, $3, $4, true, $5)
            "#,
        )
        .bind(hotspot_pubkey2.to_string())
        .bind(2000i64)
        .bind(100i32)
        .bind(now + chrono::Duration::hours(2)) // After the reward period end
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        // Define reward period for testing
        let reward_period = (now - chrono::Duration::hours(24))..now;

        // Call the function under test
        let verified_thresholds = verified_radio_thresholds(&pool, &reward_period)
            .await
            .unwrap();

        // Verify results
        assert!(
            verified_thresholds.is_verified(hotspot_pubkey1.clone()),
            "Hotspot 1 should be verified (threshold timestamp before period end)"
        );

        assert!(
            !verified_thresholds.is_verified(hotspot_pubkey2.clone()),
            "Hotspot 2 should not be verified (threshold timestamp after period end)"
        );

        // Verify total count
        let count = verified_thresholds.gateways.len();
        assert_eq!(count, 1, "Should have exactly 1 verified hotspot");
    }
}
