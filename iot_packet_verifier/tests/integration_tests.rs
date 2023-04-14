use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use file_store::iot_packet::PacketRouterPacketReport;
use futures::{Stream, StreamExt};
use futures_util::stream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket},
    DataRate, Region,
};
use iot_packet_verifier::{
    balances::BalanceCache,
    burner::Burner,
    pending_burns::{Burn, PendingBurns},
    verifier::{payload_size_to_dc, ConfigServer, Debiter, Verifier, BYTES_PER_DC},
};
use std::{collections::HashMap, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

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

#[derive(Clone)]
struct InstantBurnedBalance(Arc<Mutex<HashMap<PublicKeyBinary, u64>>>);

#[async_trait]
impl Debiter for InstantBurnedBalance {
    type Error = ();

    async fn debit_if_sufficient(&self, payer: &PublicKeyBinary, amount: u64) -> Result<bool, ()> {
        let map = self.0.lock().await;
        let balance = map.get(payer).unwrap();
        // Don't debit the amount if we're mocking. That is a job for the burner.
        Ok(*balance >= amount)
    }
}

#[async_trait::async_trait]
impl PendingBurns for InstantBurnedBalance {
    type Error = std::convert::Infallible;

    async fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>> {
        stream::iter(std::iter::empty()).boxed()
    }

    async fn fetch_next(&mut self) -> Result<Option<Burn>, Self::Error> {
        Ok(None)
    }

    async fn subtract_burned_amount(
        &mut self,
        _payer: &PublicKeyBinary,
        _amount: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        let mut map = self.0.lock().await;
        let balance = map.get_mut(payer).unwrap();
        *balance -= amount;
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

fn valid_packet(timestamp: u64, payload_size: u32, payload_hash: Vec<u8>) -> ValidPacket {
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
    let balances = InstantBurnedBalance(Arc::new(Mutex::new(balances)));
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
            // First two packets for OUI #0 are valid
            valid_packet(0, 24, vec![1]),
            valid_packet(1000, 48, vec![2]),
            // All packets for OUI #1 are valid
            valid_packet(0, 24, vec![4]),
            valid_packet(1000, 48, vec![5]),
            valid_packet(2000, 1, vec![6]),
            // All packets for OUI #2 are valid
            valid_packet(0, 24, vec![7]),
        ]
    );

    assert_eq!(invalid_packets, vec![invalid_packet(1, vec![3]),]);

    // Verify that only org #0 is disabled:
    assert!(!verifier.config_server.payers.get(&0).unwrap().enabled);
    assert!(verifier.config_server.payers.get(&1).unwrap().enabled);
    assert!(verifier.config_server.payers.get(&2).unwrap().enabled);
}

#[tokio::test]
async fn test_end_to_end() {
    let payer = PublicKeyBinary::from(vec![0]);

    // Pending burns:
    let mut pending_burns: Arc<Mutex<HashMap<PublicKeyBinary, u64>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Solana network:
    let mut solana_network = HashMap::new();
    solana_network.insert(payer.clone(), 3_u64); // Start with 3 data credits
    let solana_network = Arc::new(Mutex::new(solana_network));

    // Balance cache:
    let balance_cache = BalanceCache::new(&mut pending_burns, solana_network.clone())
        .await
        .unwrap();

    // Burner:
    let mut burner = Burner::new(
        pending_burns.clone(),
        &balance_cache,
        0, // Burn period does not matter, we manually burn
        solana_network.clone(),
    );

    // Orgs:
    let mut orgs = MockConfigServer::default();
    orgs.insert(0_u64, payer.clone());

    // Packet output:
    let mut valid_packets = Vec::new();
    let mut invalid_packets = Vec::new();

    // Set up verifier:
    let mut verifier = Verifier {
        debiter: balance_cache,
        config_server: orgs,
    };

    // Verify four packets, each costing one DC. The last one should be invalid
    verifier
        .verify(
            pending_burns.clone(),
            stream::iter(vec![
                packet_report(0, 0, BYTES_PER_DC as u32, vec![1]),
                packet_report(0, 1, BYTES_PER_DC as u32, vec![2]),
                packet_report(0, 2, BYTES_PER_DC as u32, vec![3]),
                packet_report(0, 3, BYTES_PER_DC as u32, vec![4]),
            ]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();

    // Org 0 should be disabled now:
    assert!(!verifier.config_server.payers.get(&0).unwrap().enabled,);

    assert_eq!(
        valid_packets,
        vec![
            valid_packet(0, BYTES_PER_DC as u32, vec![1]),
            valid_packet(1000, BYTES_PER_DC as u32, vec![2]),
            valid_packet(2000, BYTES_PER_DC as u32, vec![3]),
        ]
    );

    // Last packet is invalid:
    assert_eq!(
        invalid_packets,
        vec![invalid_packet(BYTES_PER_DC as u32, vec![4])]
    );

    // Check current balance:
    let balance = {
        let balances = verifier.debiter.balances();
        let balances = balances.lock().await;
        *balances.get(&payer).unwrap()
    };
    assert_eq!(balance.balance, 3);
    assert_eq!(balance.burned, 3);

    // Check that 3 DC are pending to be burned:
    let pending_burn = *pending_burns.lock().await.get(&payer).unwrap();
    assert_eq!(pending_burn, 3);

    // Initiate the burn:
    burner.burn().await.unwrap();

    // Now that we've burn, the balances and burn amount should be reset:
    let balance = {
        let balances = verifier.debiter.balances();
        let balances = balances.lock().await;
        *balances.get(&payer).unwrap()
    };
    assert_eq!(balance.balance, 0);
    assert_eq!(balance.burned, 0);

    // Pending burns should be empty as well:
    let pending_burn = *pending_burns.lock().await.get(&payer).unwrap();
    assert_eq!(pending_burn, 0);

    // Additionally, the balance on the solana network should be zero:
    let solana_balance = *solana_network.lock().await.get(&payer).unwrap();
    assert_eq!(solana_balance, 0);

    // Attempting to validate one packet should fail now:
    valid_packets.clear();
    invalid_packets.clear();

    verifier
        .verify(
            pending_burns.clone(),
            stream::iter(vec![packet_report(0, 4, BYTES_PER_DC as u32, vec![5])]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();

    assert_eq!(valid_packets, vec![]);

    assert_eq!(
        invalid_packets,
        vec![invalid_packet(BYTES_PER_DC as u32, vec![5])]
    );

    // Add one DC to the balance:
    *solana_network.lock().await.get_mut(&payer).unwrap() = 1;

    valid_packets.clear();
    invalid_packets.clear();

    // First packet should be invalid since it is too large, second
    // should clear
    verifier
        .verify(
            pending_burns.clone(),
            stream::iter(vec![
                packet_report(0, 5, 2 * BYTES_PER_DC as u32, vec![6]),
                packet_report(0, 6, BYTES_PER_DC as u32, vec![7]),
            ]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();

    assert_eq!(
        invalid_packets,
        vec![invalid_packet(2 * BYTES_PER_DC as u32, vec![6])]
    );
    assert_eq!(
        valid_packets,
        vec![valid_packet(6000, BYTES_PER_DC as u32, vec![7])]
    );

    let balance = {
        let balances = verifier.debiter.balances();
        let balances = balances.lock().await;
        *balances.get(&payer).unwrap()
    };
    assert_eq!(balance.balance, 1);
    assert_eq!(balance.burned, 1);

    // The last packet was valid, so the org should be enabled now
    assert!(verifier.config_server.payers.get(&0).unwrap().enabled);
}
