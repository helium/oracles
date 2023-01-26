use crate::balances::{Balances, ConfigureOrgError, DebitError};
use chrono::Utc;
use file_store::file_sink::FileSinkClient;
use futures::{Stream, StreamExt};
use helium_crypto::Keypair;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    iot_config::config_org_client::OrgClient, router::PacketRouterPacketReportV1, Channel,
};
use helium_proto::services::{
    iot_config::OrgGetReqV1,
    packet_verifier::{InvalidPacket, ValidPacket},
};
use solana_sdk::pubkey::Pubkey;
use sqlx::{Pool, Postgres};
use std::collections::{HashMap, HashSet};

pub struct Verifier {
    pub keypair: Keypair,
    pub sub_dao: Pubkey,
    pub balances: Balances,
    pub org_client: OrgClient<Channel>,
    pub valid_packets: FileSinkClient,
    pub invalid_packets: FileSinkClient,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PacketId {
    ts: u64,
    oui: u64,
    hash: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
pub enum VerificationError {
    #[error("Get org error: {0}")]
    GetOrgError(#[from] tonic::Status),
    #[error("Debit error: {0}")]
    DebitError(#[from] DebitError),
    #[error("Configure org error: {0}")]
    ConfigureOrgError(#[from] ConfigureOrgError),
    #[error("File store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
}

impl Verifier {
    pub async fn verify(
        &mut self,
        pool: &Pool<Postgres>,
        reports: impl Stream<Item = PacketRouterPacketReportV1>,
    ) -> Result<(), VerificationError> {
        let mut org_cache = HashMap::<u64, PublicKeyBinary>::new();
        // This may need to be in the database so that we can set last_verified_report
        // after this function.
        let mut packets_seen = HashSet::<PacketId>::new();

        tokio::pin!(reports);

        while let Some(report) = reports.next().await {
            let debit_amount = payload_size_to_dc(report.payload_size as u64);

            let packet_id = PacketId {
                ts: report.gateway_timestamp_ms,
                oui: report.oui,
                hash: report.payload_hash.clone(),
            };
            if packets_seen.contains(&packet_id) {
                continue;
            }
            packets_seen.insert(packet_id);

            if !org_cache.contains_key(&report.oui) {
                let req = OrgGetReqV1 { oui: report.oui };
                let pubkey = PublicKeyBinary::from(
                    self.org_client
                        .get(req)
                        .await?
                        .into_inner()
                        .org
                        .unwrap()
                        .owner,
                );
                org_cache.insert(report.oui, pubkey);
            }

            let payer = org_cache.get(&report.oui).unwrap();
            let sufficiency = self
                .balances
                .debit_if_sufficient(&self.sub_dao, payer, debit_amount)
                .await?;

            // TODO: Use transactions and write manifests
            if sufficiency.is_sufficient() {
                // Add the amount burned into the pending burns table
                sqlx::query(
                    r#"
                    INSERT INTO pending_burns (payer, amount, last_burn)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (payer) DO UPDATE SET
                    amount = pending_burns.amount + $2
                    "#,
                )
                .bind(payer)
                .bind(debit_amount as i64)
                .bind(Utc::now().naive_utc())
                .fetch_one(pool)
                .await?;

                self.valid_packets
                    .write(
                        ValidPacket {
                            payload_size: report.payload_size,
                            gateway: report.gateway,
                            payload_hash: report.payload_hash,
                        },
                        [],
                    )
                    .await?;
            } else {
                self.invalid_packets
                    .write(
                        InvalidPacket {
                            payload_size: report.payload_size,
                            gateway: report.gateway,
                            payload_hash: report.payload_hash,
                        },
                        [],
                    )
                    .await?;
            }

            sufficiency
                .configure_org(&mut self.org_client, &self.keypair, report.oui)
                .await?;
        }

        Ok(())
    }
}

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    payload_size.min(24) / 24
}
