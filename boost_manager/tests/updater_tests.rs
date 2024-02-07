use async_trait::async_trait;
use boost_manager::{db, updater::Updater, OnChainStatus};
use chrono::{DateTime, Utc};
use file_store::hex_boost::BoostedHexActivation;
use solana::start_boost::{GetSignature, SolanaNetwork};
use solana_sdk::signature::Signature;
use sqlx::{PgPool, Postgres, Transaction};
use std::{string::ToString, sync::Mutex, time::Duration};

const BOOSTED_HEX1_PUBKEY: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const BOOSTED_HEX2_PUBKEY: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const BOOSTED_HEX3_PUBKEY: &str = "11hd7HoicRgBPjBGcqcT2Y9hRQovdZeff5eKFMbCSuDYQmuCiF1";
const BOOSTED_HEX_CONFIG_PUBKEY: &str = "112QhnxqU8QZ3jUXpoRk51quuQVft9Pf5P5zzDDvLxj7Q9QqbMh7";

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct MockTransaction {
    signature: Signature,
    activations: Vec<BoostedHexActivation>,
}

pub struct MockSolanaConnection {
    submitted: Mutex<Vec<MockTransaction>>,
    error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MockSignature {
    pub signature: String,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("not found")]
    SubmitError(String),
}

impl MockSolanaConnection {
    fn ok() -> Self {
        Self {
            submitted: Mutex::new(vec![]),
            error: None,
        }
    }

    fn with_error(error: String) -> Self {
        Self {
            submitted: Mutex::new(vec![]),
            error: Some(error),
        }
    }
}

#[async_trait]
impl SolanaNetwork for MockSolanaConnection {
    type Error = Error;
    type Transaction = MockTransaction;

    async fn make_start_boost_transaction(
        &self,
        batch: &[BoostedHexActivation],
    ) -> Result<Self::Transaction, Self::Error> {
        Ok(MockTransaction {
            signature: Signature::new_unique(),
            activations: batch.to_owned(),
        })
    }

    async fn submit_transaction(&self, txn: &Self::Transaction) -> Result<(), Self::Error> {
        self.submitted.lock().unwrap().push(txn.clone());
        self.error
            .as_ref()
            .map(|str| Err(Error::SubmitError(str.clone())))
            .unwrap_or(Ok(()))
    }

    async fn confirm_transaction(&self, _id: &str) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl GetSignature for MockTransaction {
    fn get_signature(&self) -> &Signature {
        &self.signature
    }
}

#[sqlx::test]
async fn test_process_activations_success(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let solana_connection = MockSolanaConnection::ok();
    let updater = Updater::new(
        pool.clone(),
        true,
        Duration::from_secs(10),
        solana_connection,
    )?;

    let mut txn = pool.begin().await?;
    seed_activations(&mut txn, now).await?;
    txn.commit().await?;

    updater.process_activations().await?;

    let res = db::query_activation_statuses(&pool).await?;
    println!("res: {:?}", res);
    assert_eq!(res[0].status, OnChainStatus::Success);
    assert_eq!(res[0].location, 0x8a1fb466d2dffff_u64);
    assert_eq!(res[1].status, OnChainStatus::Success);
    assert_eq!(res[1].location, 0x8a1fb49642dffff_u64);
    assert_eq!(res[2].status, OnChainStatus::Success);
    assert_eq!(res[2].location, 0x8c2681a306607ff_u64);
    Ok(())
}

#[sqlx::test]
async fn test_process_activations_failure(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let solana_connection = MockSolanaConnection::with_error("txn failed".to_string());
    let updater = Updater::new(
        pool.clone(),
        true,
        Duration::from_secs(10),
        solana_connection,
    )?;

    let mut txn = pool.begin().await?;
    seed_activations(&mut txn, now).await?;
    txn.commit().await?;

    // ensure the activations are processed at least 10 times
    // submit_txn will bork each time
    // pushing the retries value to exceed max and
    // thus forcing it to FAILED status
    for _ in 1..=11 {
        updater.process_activations().await?;
    }
    let res = db::query_activation_statuses(&pool).await?;
    println!("res: {:?}", res);
    assert_eq!(res[0].status, OnChainStatus::Failed);
    assert_eq!(res[0].location, 0x8a1fb466d2dffff_u64);
    assert_eq!(res[1].status, OnChainStatus::Failed);
    assert_eq!(res[1].location, 0x8a1fb49642dffff_u64);
    assert_eq!(res[2].status, OnChainStatus::Failed);
    assert_eq!(res[2].location, 0x8c2681a306607ff_u64);
    Ok(())
}

async fn seed_activations(
    txn: &mut Transaction<'_, Postgres>,
    activation_ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    db::insert_activated_hex(
        txn,
        0x8a1fb466d2dffff_u64,
        &BOOSTED_HEX1_PUBKEY.to_string(),
        &BOOSTED_HEX_CONFIG_PUBKEY.to_string(),
        activation_ts,
    )
    .await?;
    db::insert_activated_hex(
        txn,
        0x8a1fb49642dffff_u64,
        &BOOSTED_HEX2_PUBKEY.to_string(),
        &BOOSTED_HEX_CONFIG_PUBKEY.to_string(),
        activation_ts,
    )
    .await?;
    db::insert_activated_hex(
        txn,
        0x8c2681a306607ff_u64,
        &BOOSTED_HEX3_PUBKEY.to_string(),
        &BOOSTED_HEX_CONFIG_PUBKEY.to_string(),
        activation_ts,
    )
    .await?;
    Ok(())
}
