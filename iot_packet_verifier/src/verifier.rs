use async_trait::async_trait;
use chrono::Utc;
use file_store::file_sink::FileSinkClient;
use futures::{Stream, StreamExt};
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::services::{
    iot_config::OrgGetReqV1,
    packet_verifier::{InvalidPacket, ValidPacket},
};
use helium_proto::{
    services::{
        iot_config::{config_org_client::OrgClient, OrgDisableReqV1, OrgEnableReqV1},
        router::PacketRouterPacketReportV1,
        Channel,
    },
    Message,
};
use sqlx::{Postgres, Transaction};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    mem,
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct Verifier<D, C> {
    pub debiter: D,
    pub config_server: C,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PacketId {
    ts: u64,
    oui: u64,
    hash: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
pub enum VerificationError<DE, CE, BE, VPE, IPE> {
    #[error("Debit error: {0}")]
    DebitError(DE),
    #[error("Config server error: {0}")]
    ConfigError(CE),
    #[error("Burn error: {0}")]
    BurnError(BE),
    #[error("Valid packet writer error: {0}")]
    ValidPacketWriterError(VPE),
    #[error("Invalid packet writer error: {0}")]
    InvalidPacketWriterError(IPE),
}

impl<D, C> Verifier<D, C>
where
    D: Debiter,
    C: ConfigServer,
{
    /// Verify a stream of packet reports. Writes out `valid_packets` and `invalid_packets`.
    pub async fn verify<B, R, VP, IP>(
        &mut self,
        mut burner: B,
        reports: R,
        mut valid_packets: VP,
        mut invalid_packets: IP,
    ) -> Result<(), VerificationError<D::Error, C::Error, B::Error, VP::Error, IP::Error>>
    where
        B: Burner,
        R: Stream<Item = PacketRouterPacketReportV1>,
        VP: PacketWriter<ValidPacket>,
        IP: PacketWriter<InvalidPacket>,
    {
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

            let payer = self
                .config_server
                .fetch_org(report.oui, &mut org_cache)
                .await
                .map_err(VerificationError::ConfigError)?;
            if self
                .debiter
                .debit_if_sufficient(&payer, debit_amount)
                .await
                .map_err(VerificationError::DebitError)?
            {
                burner
                    .burn(&payer, debit_amount)
                    .await
                    .map_err(VerificationError::BurnError)?;
                valid_packets
                    .write(ValidPacket {
                        payload_size: report.payload_size,
                        gateway: report.gateway,
                        payload_hash: report.payload_hash,
                    })
                    .await
                    .map_err(VerificationError::ValidPacketWriterError)?;
                self.config_server
                    .enable_org(report.oui)
                    .await
                    .map_err(VerificationError::ConfigError)?;
            } else {
                invalid_packets
                    .write(InvalidPacket {
                        payload_size: report.payload_size,
                        gateway: report.gateway,
                        payload_hash: report.payload_hash,
                    })
                    .await
                    .map_err(VerificationError::InvalidPacketWriterError)?;
                self.config_server
                    .disable_org(report.oui)
                    .await
                    .map_err(VerificationError::ConfigError)?;
            }
        }

        Ok(())
    }
}

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    payload_size.min(24) / 24
}

#[async_trait]
pub trait Debiter {
    type Error;

    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<bool, Self::Error>;
}

#[async_trait]
impl Debiter for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = ();

    async fn debit_if_sufficient(&self, payer: &PublicKeyBinary, amount: u64) -> Result<bool, ()> {
        let map = self.lock().await;
        let balance = map.get(payer).unwrap();
        // Don't debit the amount if we're mocking. That is a job for the burner.
        Ok(*balance >= amount)
    }
}

#[async_trait]
pub trait ConfigServer {
    type Error;

    async fn fetch_org(
        &mut self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error>;

    async fn enable_org(&mut self, oui: u64) -> Result<(), Self::Error>;

    async fn disable_org(&mut self, oui: u64) -> Result<(), Self::Error>;
}

// TODO: Move this somewhere else

// Probably should change name to something like OrgClientCache to be more
// consistent with BalanceCache
pub struct CachedOrgClient {
    pub keypair: Keypair,
    pub enabled_clients: HashMap<u64, bool>,
    pub client: OrgClient<Channel>,
}

impl CachedOrgClient {
    pub fn new(client: OrgClient<Channel>, keypair: Keypair) -> Self {
        CachedOrgClient {
            keypair,
            enabled_clients: HashMap::new(),
            client,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum OrgClientError {
    #[error("Rpc error: {0}")]
    RpcError(#[from] tonic::Status),
    #[error("Crypto error: {0}")]
    CryptoError(#[from] helium_crypto::Error),
}

#[async_trait]
impl ConfigServer for CachedOrgClient {
    type Error = OrgClientError;

    async fn fetch_org(
        &mut self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error> {
        if !cache.contains_key(&oui) {
            let req = OrgGetReqV1 { oui };
            let pubkey =
                PublicKeyBinary::from(self.client.get(req).await?.into_inner().org.unwrap().owner);
            cache.insert(oui, pubkey);
        }
        Ok(cache.get(&oui).unwrap().clone())
    }

    async fn enable_org(&mut self, oui: u64) -> Result<(), Self::Error> {
        if !mem::replace(self.enabled_clients.entry(oui).or_insert(false), true) {
            let mut req = OrgEnableReqV1 {
                oui,
                timestamp: Utc::now().timestamp_millis() as u64,
                signer: self.keypair.public_key().to_vec(),
                signature: vec![],
            };
            let signature = self.keypair.sign(&req.encode_to_vec())?;
            req.signature = signature;
            let _ = self.client.enable(req).await?;
        }
        Ok(())
    }

    async fn disable_org(&mut self, oui: u64) -> Result<(), Self::Error> {
        if mem::replace(self.enabled_clients.entry(oui).or_insert(true), false) {
            let mut req = OrgDisableReqV1 {
                oui,
                timestamp: Utc::now().timestamp_millis() as u64,
                signer: self.keypair.public_key().to_vec(),
                signature: vec![],
            };
            let signature = self.keypair.sign(&req.encode_to_vec())?;
            req.signature = signature;
            let _ = self.client.disable(req).await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait Burner {
    type Error;

    async fn burn(&mut self, payer: &PublicKeyBinary, amount: u64) -> Result<(), Self::Error>;
}

#[async_trait]
impl Burner for &'_ mut Transaction<'_, Postgres> {
    type Error = sqlx::Error;

    async fn burn(&mut self, payer: &PublicKeyBinary, amount: u64) -> Result<(), Self::Error> {
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
        .bind(amount as i64)
        .bind(Utc::now().naive_utc())
        .fetch_one(&mut **self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Burner for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = ();

    async fn burn(&mut self, payer: &PublicKeyBinary, amount: u64) -> Result<(), ()> {
        let mut map = self.lock().await;
        let balance = map.get_mut(payer).unwrap();
        *balance -= amount;
        Ok(())
    }
}

#[async_trait]
pub trait PacketWriter<T> {
    type Error;

    // The redundant &mut receivers we see for PacketWriter and Burner ars so
    // that we are able to resolve to either a mutable or immutable ref without
    // having to take ownership of the mutable reference.
    async fn write(&mut self, packet: T) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T: prost::Message + 'static> PacketWriter<T> for &'_ FileSinkClient {
    type Error = file_store::Error;

    async fn write(&mut self, packet: T) -> Result<(), Self::Error> {
        (*self).write(packet, []).await?;
        Ok(())
    }
}

#[async_trait]
impl<T: Send> PacketWriter<T> for &'_ mut Vec<T> {
    type Error = ();

    async fn write(&mut self, packet: T) -> Result<(), ()> {
        (*self).push(packet);
        Ok(())
    }
}
