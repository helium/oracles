use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use chrono::{DateTime, FixedOffset, Utc};
use file_store::traits::TimestampEncode;
use file_store_oracles::mobile_session::DataTransferSessionReq;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidDataTransferSession;
use sqlx::{prelude::FromRow, Pool, Postgres, Row, Transaction};

use crate::bytes_to_dc;

const METRIC_NAME: &str = "pending_dc_burn";

use trino_rust_client::Trino;

#[derive(Trino, serde::Serialize, serde::Deserialize)]
pub struct DataTransferSessionTrino {
    pub_key: String,
    payer: String,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    rewardable_bytes: i64,
    first_timestamp: DateTime<FixedOffset>,
    last_timestamp: DateTime<FixedOffset>,
}

impl From<DataTransferSessionTrino> for DataTransferSession {
    fn from(value: DataTransferSessionTrino) -> Self {
        Self {
            pub_key: PublicKeyBinary::from_str(&value.pub_key).expect("pubkey"),
            payer: PublicKeyBinary::from_str(&value.payer).expect("payer"),
            uploaded_bytes: value.uploaded_bytes,
            downloaded_bytes: value.downloaded_bytes,
            rewardable_bytes: value.rewardable_bytes,
            first_timestamp: value.first_timestamp.into(),
            last_timestamp: value.last_timestamp.into(),
        }
    }
}

#[derive(Debug, Clone, FromRow, PartialEq)]
pub struct DataTransferSession {
    pub_key: PublicKeyBinary,
    payer: PublicKeyBinary,
    uploaded_bytes: i64,
    downloaded_bytes: i64,
    rewardable_bytes: i64,
    first_timestamp: DateTime<Utc>,
    last_timestamp: DateTime<Utc>,
}

impl DataTransferSession {
    const TABLE_NAME: &'static str = "data_transfer_sessions";

    pub fn dc_to_burn(&self) -> u64 {
        bytes_to_dc(self.rewardable_bytes as u64)
    }

    pub fn from_req(req: &DataTransferSessionReq, last_timestamp: DateTime<Utc>) -> Self {
        DataTransferSession {
            pub_key: req.data_transfer_usage.pub_key.clone(),
            payer: req.data_transfer_usage.payer.clone(),
            uploaded_bytes: req.data_transfer_usage.upload_bytes as i64,
            downloaded_bytes: req.data_transfer_usage.download_bytes as i64,
            rewardable_bytes: req.rewardable_bytes as i64,
            // timestamps are the same upon ingest
            first_timestamp: last_timestamp,
            last_timestamp,
        }
    }

    pub fn table_def(schema_name: &str) -> helium_iceberg::TableDefinition {
        use helium_iceberg::*;

        TableDefinition::builder(Self::TABLE_NAME)
            .with_fields([
                FieldDefinition::required("pub_key", PrimitiveType::String),
                FieldDefinition::required("payer", PrimitiveType::String),
                FieldDefinition::required("uploaded_bytes", PrimitiveType::Int),
                FieldDefinition::required("downloaded_bytes", PrimitiveType::Int),
                FieldDefinition::required("rewardable_bytes", PrimitiveType::Int),
                FieldDefinition::required("first_timestamp", PrimitiveType::Timestamptz),
                FieldDefinition::required("last_timestamp", PrimitiveType::Timestamptz),
            ])
            .with_partition(PartitionDefinition::day(
                "first_timestamp",
                "first_timestamp_day",
            ))
            .with_location(format!("s3://iceberg/{}/events", schema_name))
            .build()
            .expect("valid data transfer sessions table")
    }

    fn to_trino_insert(&self) -> String {
        format!(
            "
            (
                '{pub_key}', '{payer}', {uploaded_bytes}, {downloaded_bytes},
                {rewardable_bytes}, TIMESTAMP '{first_timestamp}', TIMESTAMP '{last_timestamp}'
            )
            ",
            pub_key = self.pub_key.to_string(),
            payer = self.payer.to_string(),
            uploaded_bytes = self.uploaded_bytes as u64,
            downloaded_bytes = self.downloaded_bytes as u64,
            rewardable_bytes = self.rewardable_bytes as u64,
            // NOTE(mj): putting 6 decimals helps with PartialEq when doing a roundtrip, but may be overkill.
            // Does make life a lot easier though.
            first_timestamp = self.first_timestamp.format("%Y-%m-%d %H:%M:%S%.6f"),
            last_timestamp = self.last_timestamp.format("%Y-%m-%d %H:%M:%S%.6f")
        )
    }

    pub async fn trino_write(
        sessions: &[Self],
        trino: &trino_rust_client::Client,
    ) -> anyhow::Result<()> {
        if sessions.is_empty() {
            return Ok(());
        }
        let sessions = sessions
            .iter()
            .map(|session| session.to_trino_insert())
            .collect::<Vec<String>>();
        let query = format!(
            "
            INSERT INTO {table_name}
                (
                    pub_key, payer, uploaded_bytes, downloaded_bytes,
                    rewardable_bytes, first_timestamp, last_timestamp
                )
            VALUES {data}
            ",
            table_name = Self::TABLE_NAME,
            data = sessions.join(", ")
        );

        trino.execute(query).await?;
        Ok(())
    }

    pub async fn trino_delete(
        trino: &trino_rust_client::Client,
        payer: &PublicKeyBinary,
    ) -> anyhow::Result<()> {
        let query = format!(
            "DELETE FROM {table_name} WHERE payer = '{payer}'",
            table_name = Self::TABLE_NAME,
            payer = payer.to_string()
        );

        trino.execute(query).await?;
        Ok(())
    }

    pub async fn get_all(trino: &trino_rust_client::Client) -> anyhow::Result<Vec<Self>> {
        let all = trino
            .get_all::<DataTransferSessionTrino>(format!(
                "SELECT \
                    pub_key, \
                    any_value(payer) AS payer, \
                    SUM(uploaded_bytes) AS uploaded_bytes, \
                    SUM(downloaded_bytes) AS downloaded_bytes, \
                    SUM(rewardable_bytes) AS rewardable_bytes, \
                    MIN(first_timestamp) AS first_timestamp, \
                    MAX(last_timestamp) AS last_timestamp \
                 FROM {} \
                 GROUP BY pub_key",
                Self::TABLE_NAME
            ))
            .await;

        let all = match all {
            Ok(all) => all,
            Err(trino_rust_client::error::Error::EmptyData) => return Ok(vec![]),
            Err(err) => return Err(err.into()),
        };

        Ok(all.into_vec().into_iter().map(Self::from).collect())
    }
}

impl From<DataTransferSession> for ValidDataTransferSession {
    fn from(session: DataTransferSession) -> Self {
        let num_dcs = session.dc_to_burn();

        ValidDataTransferSession {
            pub_key: session.pub_key.into(),
            payer: session.payer.into(),
            upload_bytes: session.uploaded_bytes as u64,
            download_bytes: session.downloaded_bytes as u64,
            rewardable_bytes: session.rewardable_bytes as u64,
            num_dcs,
            first_timestamp: session.first_timestamp.encode_timestamp_millis(),
            last_timestamp: session.last_timestamp.encode_timestamp_millis(),
            burn_timestamp: Utc::now().encode_timestamp_millis(),
        }
    }
}

pub struct PendingPayerBurn {
    pub payer: PublicKeyBinary,
    pub total_dcs: u64,
    pub sessions: Vec<DataTransferSession>,
}

pub async fn initialize(conn: &Pool<Postgres>) -> anyhow::Result<()> {
    let results = sqlx::query(
        r#"
        SELECT payer, sum(rewardable_bytes)::bigint as total_rewardable_bytes
        FROM data_transfer_sessions
        GROUP BY payer
        "#,
    )
    .fetch_all(conn)
    .await?;

    for row in results {
        let payer: PublicKeyBinary = row.get("payer");
        let total_rewardable_bytes: u64 = row.get::<i64, _>("total_rewardable_bytes") as u64;

        set_metric(&payer, bytes_to_dc(total_rewardable_bytes));
    }

    Ok(())
}

pub async fn get_all(
    conn: &Pool<Postgres>,
    trino: Option<&trino_rust_client::Client>,
) -> anyhow::Result<Vec<DataTransferSession>> {
    let results = sqlx::query_as("SELECT * FROM data_transfer_sessions")
        .fetch_all(conn)
        .await?;

    if let Some(trino) = trino {
        let ts_owned = DataTransferSession::get_all(trino).await?;

        // Create vectors of references so we can sort without moving the owned collections.
        let mut pg_refs: Vec<&DataTransferSession> = results.iter().collect();
        let mut tr_refs: Vec<&DataTransferSession> = ts_owned.iter().collect();

        pg_refs.sort_by_key(|x| (x.first_timestamp, x.pub_key.clone()));
        tr_refs.sort_by_key(|x| (x.first_timestamp, x.pub_key.clone()));

        // Shadow the original names with the sorted reference collections so the following
        // debug_assert_eq! will compare the sorted lists.
        let results = pg_refs;
        let ts = tr_refs;
        debug_assert_eq!(results, ts, "trino results should match postgres");
    }

    Ok(results)
}

pub async fn get_all_payer_burns(
    conn: &Pool<Postgres>,
    trino: Option<&trino_rust_client::Client>,
) -> anyhow::Result<Vec<PendingPayerBurn>> {
    let pending_payer_burns = get_all(conn, trino)
        .await?
        .into_iter()
        .fold(
            HashMap::<PublicKeyBinary, PendingPayerBurn>::new(),
            |mut map, session| {
                let dc_to_burn = session.dc_to_burn();

                match map.get_mut(&session.payer) {
                    Some(pending_payer_burn) => {
                        pending_payer_burn.total_dcs += dc_to_burn;
                        pending_payer_burn.sessions.push(session);
                    }
                    None => {
                        map.insert(
                            session.payer.clone(),
                            PendingPayerBurn {
                                payer: session.payer.clone(),
                                total_dcs: dc_to_burn,
                                sessions: vec![session],
                            },
                        );
                    }
                }

                map
            },
        )
        .into_values()
        .collect();

    Ok(pending_payer_burns)
}

pub async fn save_data_transfer_session_reqs(
    txn: &mut Transaction<'_, Postgres>,
    reqs: &[DataTransferSessionReq],
    last_timestamp: DateTime<Utc>,
    trino: Option<&trino_rust_client::Client>,
) -> anyhow::Result<()> {
    let sessions = reqs
        .iter()
        .map(|x| DataTransferSession::from_req(x, last_timestamp))
        .collect::<Vec<_>>();

    save_data_transfer_sessions(txn, &sessions, trino).await?;

    Ok(())
}

pub async fn save_data_transfer_sessions(
    txn: &mut Transaction<'_, Postgres>,
    data_transfer_session: &[DataTransferSession],
    trino: Option<&trino_rust_client::Client>,
) -> anyhow::Result<()> {
    postgres_save_data_transfer_sessions(txn, data_transfer_session).await?;

    if let Some(trino) = trino {
        DataTransferSession::trino_write(data_transfer_session, trino)
            .await
            .expect("writing to trino");
    }

    Ok(())
}

pub async fn postgres_save_data_transfer_sessions(
    txn: &mut Transaction<'_, Postgres>,
    data_transfer_session: &[DataTransferSession],
) -> anyhow::Result<()> {
    // Pre-aggregate by (pub_key, payer) to avoid "ON CONFLICT DO UPDATE command
    // cannot affect row a second time" when duplicates exist in a single batch.
    let mut merged: HashMap<(String, String), DataTransferSession> = HashMap::new();
    for s in data_transfer_session {
        let key = (s.pub_key.to_string(), s.payer.to_string());
        merged
            .entry(key)
            .and_modify(|existing| {
                existing.uploaded_bytes += s.uploaded_bytes;
                existing.downloaded_bytes += s.downloaded_bytes;
                existing.rewardable_bytes += s.rewardable_bytes;
                existing.first_timestamp = existing.first_timestamp.min(s.first_timestamp);
                existing.last_timestamp = existing.last_timestamp.max(s.last_timestamp);
            })
            .or_insert_with(|| s.clone());
    }

    let merged_sessions: Vec<DataTransferSession> = merged.into_values().collect();

    let pub_keys = merged_sessions
        .iter()
        .map(|s| s.pub_key.to_string())
        .collect::<Vec<String>>();
    let payers = merged_sessions
        .iter()
        .map(|s| s.payer.to_string())
        .collect::<Vec<String>>();
    let uploaded = merged_sessions
        .iter()
        .map(|s| s.uploaded_bytes)
        .collect::<Vec<i64>>();
    let downloaded = merged_sessions
        .iter()
        .map(|s| s.downloaded_bytes)
        .collect::<Vec<i64>>();
    let rewardable = merged_sessions
        .iter()
        .map(|s| s.rewardable_bytes)
        .collect::<Vec<i64>>();
    let first_ts = merged_sessions
        .iter()
        .map(|s| s.first_timestamp)
        .collect::<Vec<DateTime<Utc>>>();
    let last_ts = merged_sessions
        .iter()
        .map(|s| s.last_timestamp)
        .collect::<Vec<DateTime<Utc>>>();

    sqlx::query(
        r#"
        INSERT INTO data_transfer_sessions
            (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
        SELECT
            pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp
        FROM UNNEST(
            $1::text[],
            $2::text[],
            $3::bigint[],
            $4::bigint[],
            $5::bigint[],
            $6::timestamptz[],
            $7::timestamptz[]
        ) AS t(
            pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp
        )
        ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            first_timestamp = LEAST(data_transfer_sessions.first_timestamp, EXCLUDED.first_timestamp),
            last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
        "#
    )
    .bind(pub_keys)
    .bind(payers)
    .bind(uploaded)
    .bind(downloaded)
    .bind(rewardable)
    .bind(first_ts)
    .bind(last_ts)
    .execute(&mut **txn)
    .await.context("inserting multiple into postgres")?;

    Ok(())
}

pub async fn delete_for_payer(
    conn: &Pool<Postgres>,
    payer: &PublicKeyBinary,
    trino: Option<&trino_rust_client::Client>,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM data_transfer_sessions WHERE payer = $1")
        .bind(payer)
        .execute(conn)
        .await?;

    if let Some(trino) = trino {
        DataTransferSession::trino_delete(trino, payer).await?;
    };

    Ok(())
}

fn set_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).set(value as f64);
}

pub fn increment_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).increment(value as f64);
}

pub fn decrement_metric(payer: &PublicKeyBinary, value: u64) {
    metrics::gauge!(METRIC_NAME, "payer" => payer.to_string()).decrement(value as f64);
}
