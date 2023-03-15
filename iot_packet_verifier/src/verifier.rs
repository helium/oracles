use async_trait::async_trait;
use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient, iot_packet::PacketRouterPacketReport, traits::MsgTimestamp,
};
use futures::{Stream, StreamExt};
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::services::{
    iot_config::OrgGetReqV1,
    packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket},
};
use helium_proto::{
    services::{
        iot_config::{config_org_client::OrgClient, OrgDisableReqV1, OrgEnableReqV1},
        Channel,
    },
    Message,
};
use sqlx::{Postgres, Transaction};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    mem,
};

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
        mut pending_burns: B,
        reports: R,
        mut valid_packets: VP,
        mut invalid_packets: IP,
    ) -> Result<(), VerificationError<D::Error, C::Error, B::Error, VP::Error, IP::Error>>
    where
        B: PendingBurns,
        R: Stream<Item = PacketRouterPacketReport>,
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
            let report_ts = report.timestamp();
            let packet_id = PacketId {
                ts: report_ts,
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
                pending_burns
                    .add_burn(&payer, debit_amount)
                    .await
                    .map_err(VerificationError::BurnError)?;
                valid_packets
                    .write(ValidPacket {
                        payload_size: report.payload_size,
                        gateway: report.gateway.into(),
                        payload_hash: report.payload_hash,
                        num_dcs: debit_amount as u32,
                        packet_timestamp: report_ts,
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
                        gateway: report.gateway.into(),
                        payload_hash: report.payload_hash,
                        reason: InvalidPacketReason::InsufficientBalance as i32,
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
    let payload_size = payload_size.max(24);
    // perform a div_ciel function:
    (payload_size + 24 - 1) / 24
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
        if let Entry::Vacant(e) = cache.entry(oui) {
            let req = OrgGetReqV1 { oui };
            let pubkey =
                PublicKeyBinary::from(self.client.get(req).await?.into_inner().org.unwrap().owner);
            e.insert(pubkey);
        }
        Ok(cache.get(&oui).unwrap().clone())
    }

    async fn enable_org(&mut self, oui: u64) -> Result<(), Self::Error> {
        if !mem::replace(self.enabled_clients.entry(oui).or_insert(false), true) {
            let mut req = OrgEnableReqV1 {
                oui,
                timestamp: Utc::now().timestamp_millis() as u64,
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
pub trait PendingBurns {
    type Error;

    async fn add_burn(&mut self, payer: &PublicKeyBinary, amount: u64) -> Result<(), Self::Error>;
}

#[async_trait]
impl PendingBurns for &'_ mut Transaction<'_, Postgres> {
    type Error = sqlx::Error;

    async fn add_burn(&mut self, payer: &PublicKeyBinary, amount: u64) -> Result<(), Self::Error> {
        // Add the amount burned into the pending burns table
        sqlx::query(
            r#"
            INSERT INTO pending_burns (payer, amount, last_burn)
            VALUES ($1, $2, $3)
            ON CONFLICT (payer) DO UPDATE SET
            amount = pending_burns.amount + $2
            RETURNING *
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
pub trait PacketWriter<T> {
    type Error;

    // The redundant &mut receivers we see for PacketWriter and Burner are so
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

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{TimeZone, Utc};
    use futures_util::stream;
    use helium_proto::{DataRate, Region};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[async_trait]
    impl Debiter for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
        type Error = ();

        async fn debit_if_sufficient(
            &self,
            payer: &PublicKeyBinary,
            amount: u64,
        ) -> Result<bool, ()> {
            let map = self.lock().await;
            let balance = map.get(payer).unwrap();
            // Don't debit the amount if we're mocking. That is a job for the burner.
            Ok(*balance >= amount)
        }
    }

    #[async_trait]
    impl PendingBurns for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
        type Error = ();

        async fn add_burn(&mut self, payer: &PublicKeyBinary, amount: u64) -> Result<(), ()> {
            let mut map = self.lock().await;
            let balance = map.get_mut(payer).unwrap();
            *balance -= amount;
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

    struct MockConfig {
        payer: PublicKeyBinary,
        enabled: bool,
    }

    #[derive(Default)]
    struct MockConfigServer {
        payers: HashMap<u64, MockConfig>,
    }

    impl MockConfigServer {
        fn insert(&mut self, oui: u64, payer: PublicKeyBinary) {
            self.payers.insert(
                oui,
                MockConfig {
                    payer,
                    enabled: false,
                },
            );
        }
    }

    #[async_trait]
    impl ConfigServer for MockConfigServer {
        type Error = ();

        async fn fetch_org(
            &mut self,
            oui: u64,
            _cache: &mut HashMap<u64, PublicKeyBinary>,
        ) -> Result<PublicKeyBinary, ()> {
            Ok(self.payers.get(&oui).unwrap().payer.clone())
        }

        async fn enable_org(&mut self, oui: u64) -> Result<(), ()> {
            self.payers.get_mut(&oui).unwrap().enabled = true;
            Ok(())
        }

        async fn disable_org(&mut self, oui: u64) -> Result<(), ()> {
            self.payers.get_mut(&oui).unwrap().enabled = false;
            Ok(())
        }
    }

    fn packet_report(
        oui: u64,
        timestamp: u64,
        payload_size: u32,
        payload_hash: Vec<u8>,
    ) -> PacketRouterPacketReport {
        PacketRouterPacketReport {
            gateway_timestamp: Utc.timestamp_opt(timestamp as i64, 0).unwrap(),
            oui,
            net_id: 0,
            rssi: 0,
            frequency: 0,
            snr: 0.0,
            data_rate: DataRate::Fsk50,
            region: Region::As9231,
            gateway: PublicKeyBinary::from(vec![]),
            payload_hash,
            payload_size,
        }
    }

    fn valid_packet(payload_size: u32, payload_hash: Vec<u8>, timestamp: u64) -> ValidPacket {
        ValidPacket {
            payload_size,
            payload_hash,
            gateway: vec![],
            num_dcs: payload_size_to_dc(payload_size as u64) as u32,
            packet_timestamp: timestamp,
        }
    }

    fn invalid_packet(payload_size: u32, payload_hash: Vec<u8>) -> InvalidPacket {
        InvalidPacket {
            payload_size,
            payload_hash,
            gateway: vec![],
            reason: InvalidPacketReason::InsufficientBalance as i32,
        }
    }

    #[tokio::test]
    async fn test_verifier() {
        let packets = vec![
            // Packets for first OUI
            packet_report(0, 0, 24, vec![1]),
            packet_report(0, 1, 48, vec![2]),
            packet_report(0, 2, 1, vec![3]),
            // Packets for second OUI
            packet_report(1, 0, 24, vec![4]),
            packet_report(1, 1, 48, vec![5]),
            packet_report(1, 1, 48, vec![5]),
            packet_report(1, 2, 1, vec![6]),
            // Packets for third OUI
            packet_report(2, 0, 24, vec![7]),
            packet_report(2, 0, 24, vec![7]),
        ];
        // Set up orgs:
        let mut orgs = MockConfigServer::default();
        orgs.insert(0_u64, PublicKeyBinary::from(vec![0]));
        orgs.insert(1_u64, PublicKeyBinary::from(vec![1]));
        orgs.insert(2_u64, PublicKeyBinary::from(vec![2]));
        // Set up balances:
        let mut balances = HashMap::new();
        balances.insert(PublicKeyBinary::from(vec![0]), 3);
        balances.insert(PublicKeyBinary::from(vec![1]), 4);
        balances.insert(PublicKeyBinary::from(vec![2]), 1);
        let balances = Arc::new(Mutex::new(balances));
        // Set up output:
        let mut valid_packets = Vec::new();
        let mut invalid_packets = Vec::new();
        // Set up verifier:
        let mut verifier = Verifier {
            debiter: balances.clone(),
            config_server: orgs,
        };

        // Run the verifier:
        verifier
            .verify(
                balances.clone(),
                stream::iter(packets),
                &mut valid_packets,
                &mut invalid_packets,
            )
            .await
            .unwrap();

        // Verify packet reports:
        assert_eq!(
            valid_packets,
            vec![
                // TODO: some weird shit with the timestamp values in the tests
                //       work it out, hardcoding them to expect values atm
                // First two packets for OUI #0 are valid
                valid_packet(24, vec![1], 0),
                valid_packet(48, vec![2], 1000),
                // All packets for OUI #1 are valid
                valid_packet(24, vec![4], 0),
                valid_packet(48, vec![5], 1000),
                valid_packet(1, vec![6], 2000),
                // All packets for OUI #2 are valid
                valid_packet(24, vec![7], 0),
            ]
        );

        assert_eq!(invalid_packets, vec![invalid_packet(1, vec![3]),]);

        // Verify that only org #0 is disabled:
        assert!(!verifier.config_server.payers.get(&0).unwrap().enabled);
        assert!(verifier.config_server.payers.get(&1).unwrap().enabled);
        assert!(verifier.config_server.payers.get(&2).unwrap().enabled);
    }
}
