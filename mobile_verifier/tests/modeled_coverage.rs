use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{CoverageObjectValidity, SignalLevel};
use mobile_verifier::coverage::CoverageObject;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test]
#[ignore]
async fn test_save_wifi_coverage_object(pool: PgPool) -> anyhow::Result<()> {
    let uuid = Uuid::new_v4();
    let co = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::HotspotKey(
            "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
                .parse()
                .unwrap(),
        ),
        coverage_claim_time: Utc::now(),
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

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM hex_coverage")
        .fetch_one(&mut transaction)
        .await?;

    assert_eq!(count, 3);

    Ok(())
}

#[sqlx::test]
#[ignore]
async fn test_save_cbrs_coverage_object(pool: PgPool) -> anyhow::Result<()> {
    let uuid = Uuid::new_v4();
    let co = file_store::coverage::CoverageObject {
        pub_key: PublicKeyBinary::from(vec![1]),
        uuid,
        key_type: file_store::coverage::KeyType::CbsdId(
            "P27-SCE4255W120200039521XGB0103".to_string(),
        ),
        coverage_claim_time: Utc::now(),
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

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM hex_coverage")
        .fetch_one(&mut transaction)
        .await?;

    assert_eq!(count, 3);

    Ok(())
}
