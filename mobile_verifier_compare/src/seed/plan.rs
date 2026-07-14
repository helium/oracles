use chrono::{DateTime, DurationRound, TimeDelta, Utc};

/// Which diff status a hotspot's seeded rows should produce when the
/// compare-trino subcommands are run after seeding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    Match,
    Mismatch,
    PgOnly,
    TrinoOnly,
}

impl Bucket {
    pub fn as_str(self) -> &'static str {
        match self {
            Bucket::Match => "match",
            Bucket::Mismatch => "mismatch",
            Bucket::PgOnly => "pg_only",
            Bucket::TrinoOnly => "trino_only",
        }
    }
}

/// One seeded hotspot.
#[derive(Debug, Clone)]
pub struct Hotspot {
    pub bucket: Bucket,
    /// Base58 PublicKeyBinary string. Deterministic — same `seed` produces the
    /// same value across runs so the user can diff repeatedly.
    pub pubkey: String,
    /// Payer pubkey paired with this hotspot for data-session rows.
    pub payer: String,
}

/// The whole fixture set: a list of hotspots tagged with their target bucket
/// plus the epoch window the seed is anchored on.
#[derive(Debug, Clone)]
pub struct Plan {
    pub epoch_start: DateTime<Utc>,
    pub epoch_end: DateTime<Utc>,
    pub hotspots: Vec<Hotspot>,
}

impl Plan {
    /// Split `total` hotspots into the four buckets using the default
    /// 60/20/10/10 distribution, with a deterministic seed for the pubkeys
    /// so re-runs produce the same fixture set.
    pub fn build(total: usize, seed: u64, now: DateTime<Utc>) -> Self {
        let epoch_end = now
            .duration_trunc(TimeDelta::hours(1))
            .expect("hour truncation");
        let epoch_start = epoch_end - chrono::Duration::hours(24);

        let total = total.max(4); // need at least one of each bucket
        let pg_only = (total / 10).max(1);
        let trino_only = (total / 10).max(1);
        let mismatch = (total / 5).max(1);
        let matched = total - pg_only - trino_only - mismatch;

        let mut hotspots = Vec::with_capacity(total);
        let mut push = |bucket: Bucket, count: usize, base: u64| {
            for i in 0..count {
                let pubkey = deterministic_pubkey(seed.wrapping_add(base).wrapping_add(i as u64));
                let payer = deterministic_pubkey(
                    seed.wrapping_add(0xDEAD_BEEF)
                        .wrapping_add(base)
                        .wrapping_add(i as u64),
                );
                hotspots.push(Hotspot {
                    bucket,
                    pubkey,
                    payer,
                });
            }
        };
        push(Bucket::Match, matched, 0x0000);
        push(Bucket::Mismatch, mismatch, 0x1000);
        push(Bucket::PgOnly, pg_only, 0x2000);
        push(Bucket::TrinoOnly, trino_only, 0x3000);

        Self {
            epoch_start,
            epoch_end,
            hotspots,
        }
    }

    pub fn counts(&self) -> [(Bucket, usize); 4] {
        let mut counts = [
            (Bucket::Match, 0),
            (Bucket::Mismatch, 0),
            (Bucket::PgOnly, 0),
            (Bucket::TrinoOnly, 0),
        ];
        for h in &self.hotspots {
            for c in counts.iter_mut() {
                if c.0 == h.bucket {
                    c.1 += 1;
                }
            }
        }
        counts
    }
}

/// Build a deterministic PublicKeyBinary-shaped string from a seed. We don't
/// actually need a valid ed25519 keypair here — the Postgres columns are TEXT
/// and the production code only round-trips the string. A 32-byte payload
/// rendered through `PublicKeyBinary` keeps the shape realistic.
fn deterministic_pubkey(seed: u64) -> String {
    use helium_crypto::PublicKeyBinary;
    use rand::{rngs::StdRng, RngCore, SeedableRng};
    let mut rng = StdRng::seed_from_u64(seed);
    // 33 bytes: 1 key-tag byte (ed25519 + mainnet) + 32 pubkey bytes.
    // Tag 0x01 = network=Mainnet, key_type=Ed25519. Matches what helium-crypto
    // emits for real keypairs.
    let mut buf = [0u8; 33];
    buf[0] = 0x01;
    rng.fill_bytes(&mut buf[1..]);
    PublicKeyBinary::from(buf.to_vec()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_distributes_all_four_buckets() {
        let plan = Plan::build(10, 42, Utc::now());
        let counts = plan.counts();
        for (_, n) in counts {
            assert!(n >= 1, "every bucket should have at least one hotspot");
        }
        assert_eq!(plan.hotspots.len(), 10);
    }

    #[test]
    fn plan_is_deterministic_for_same_seed() {
        let now = Utc::now();
        let a = Plan::build(8, 7, now);
        let b = Plan::build(8, 7, now);
        let keys_a: Vec<_> = a.hotspots.iter().map(|h| h.pubkey.clone()).collect();
        let keys_b: Vec<_> = b.hotspots.iter().map(|h| h.pubkey.clone()).collect();
        assert_eq!(keys_a, keys_b);
    }

    #[test]
    fn plan_epoch_is_hourly_aligned() {
        let plan = Plan::build(4, 1, Utc::now());
        assert_eq!(plan.epoch_end.timestamp() % 3600, 0);
        assert_eq!(plan.epoch_start.timestamp() % 3600, 0);
        assert_eq!(
            plan.epoch_end - plan.epoch_start,
            chrono::Duration::hours(24)
        );
    }

    #[test]
    fn pubkeys_parse_as_public_key_binary() {
        use helium_crypto::PublicKeyBinary;
        let plan = Plan::build(4, 99, Utc::now());
        for h in &plan.hotspots {
            let _: PublicKeyBinary = h
                .pubkey
                .parse()
                .expect("generated pubkey should round-trip through PublicKeyBinary");
        }
    }
}
