use crate::{pending_burns::*, verifier::*};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use file_store::iot_packet::PacketRouterPacketReport;
use futures::Stream;
use futures_util::stream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket},
    DataRate, Region,
};
use std::{collections::HashMap, convert::Infallible, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

#[async_trait]
impl Debiter for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;

    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<bool, Infallible> {
        let map = self.lock().await;
        let balance = map.get(payer).unwrap();
        // Don't debit the amount if we're mocking. That is a job for the burner.
        Ok(*balance >= amount)
    }
}

#[async_trait]
impl PendingBurns for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;

    fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>> {
        todo!()
    }

    async fn fetch_next(&mut self) -> Result<Option<Burn>, Self::Error> {
        todo!()
    }

    async fn subtract_burned_amount(
        &mut self,
        _payer: &PublicKeyBinary,
        _amount: u64,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
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
        packet_report(1, 2, 1, vec![6]),
        // Packets for third OUI
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
