use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{CoverageObjectValidity, SignalLevel};
use mobile_verifier::{
    coverage::{CoverageClaimTimeCache, CoverageObject, CoveredHexCache},
    heartbeats::KeyType,
};
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
#[ignore]
async fn test_save_wifi_coverage_object(pool: PgPool) -> anyhow::Result<()> {
    let cache = CoveredHexCache::new(&pool);
    let uuid = Uuid::new_v4();
    let coverage_claim_time = "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();

    assert!(cache.fetch_coverage(&uuid).await?.is_none());

    let co = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::HotspotKey(
            "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
        ),
        coverage_claim_time,
        coverage: vec![
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46622dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46632dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46642dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
        ],
        indoor: true,
        trust_score: 1000,
        signature: Vec::new(),
    };
    let co = CoverageObject {
        coverage_object: co,
        validity: CoverageObjectValidity::Valid,
    };

    let mut transaction = pool.begin().await?;
    co.save(&mut transaction).await?;

    // Test coverage claim time
    let cctc = CoverageClaimTimeCache::new();
    let key: PublicKeyBinary = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
        .parse()
        .unwrap();
    let key = KeyType::from(&key);
    let expected_coverage_claim_time = cctc
        .fetch_coverage_claim_time(key, &Some(uuid), &mut transaction)
        .await?
        .unwrap();

    assert_eq!(expected_coverage_claim_time, coverage_claim_time);

    transaction.commit().await?;

    let coverage = cache.fetch_coverage(&uuid).await?.unwrap();

    assert_eq!(coverage.coverage.len(), 3);

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn test_save_cbrs_coverage_object(pool: PgPool) -> anyhow::Result<()> {
    let cache = CoveredHexCache::new(&pool);
    let uuid = Uuid::new_v4();
    let coverage_claim_time = "2023-08-23 00:00:00.000000000 UTC".parse().unwrap();

    assert!(cache.fetch_coverage(&uuid).await?.is_none());

    let co = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(
            "P27-SCE4255W120200039521XGB0103".to_string(),
        ),
        coverage_claim_time,
        coverage: vec![
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46622dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46632dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
            file_store::coverage::RadioHexSignalLevel {
                location: "8a1fb46642dffff".parse().unwrap(),
                signal_level: SignalLevel::High,
                signal_power: 1000,
            },
        ],
        indoor: true,
        trust_score: 1000,
        signature: Vec::new(),
    };
    let co = CoverageObject {
        coverage_object: co,
        validity: CoverageObjectValidity::Valid,
    };

    let mut transaction = pool.begin().await?;
    co.save(&mut transaction).await?;

    // Test coverage claim time
    let cctc = CoverageClaimTimeCache::new();
    let key = "P27-SCE4255W120200039521XGB0103";
    let key = KeyType::from(key);
    let expected_coverage_claim_time = cctc
        .fetch_coverage_claim_time(key, &Some(uuid), &mut transaction)
        .await?
        .unwrap();

    assert_eq!(expected_coverage_claim_time, coverage_claim_time);

    transaction.commit().await?;

    let coverage = cache.fetch_coverage(&uuid).await?.unwrap();

    assert_eq!(coverage.coverage.len(), 3);

    Ok(())
}
