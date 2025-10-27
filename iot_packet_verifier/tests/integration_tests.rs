use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use file_store_oracles::iot_packet::PacketRouterPacketReport;
use futures_util::stream;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::{
        packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket},
        router::packet_router_packet_report_v1::PacketType,
    },
    DataRate, Region,
};
use iot_packet_verifier::{
    balances::{BalanceCache, PayerAccount},
    burner::{BurnError, Burner},
    pending::{confirm_pending_txns, AddPendingBurn, Burn, PendingTables, BURN_THRESHOLD},
    verifier::{payload_size_to_dc, ConfigServer, ConfigServerError, Org, Verifier, BYTES_PER_DC},
};
use solana::{
    burn::{SolanaNetwork, TestSolanaClientMap},
    GetSignature, Signature,
};
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

struct MockConfig {
    payer: PublicKeyBinary,
    enabled: bool,
}

#[derive(Default, Clone)]
struct MockConfigServer {
    payers: Arc<Mutex<HashMap<u64, MockConfig>>>,
}

impl MockConfigServer {
    async fn insert(&self, oui: u64, payer: PublicKeyBinary) {
        self.payers.lock().await.insert(
            oui,
            MockConfig {
                payer,
                enabled: true,
            },
        );
    }
}

#[async_trait]
impl ConfigServer for MockConfigServer {
    async fn fetch_org(
        &self,
        oui: u64,
        _cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, ConfigServerError> {
        Ok(self.payers.lock().await.get(&oui).unwrap().payer.clone())
    }

    async fn disable_org(&self, oui: u64) -> Result<(), ConfigServerError> {
        self.payers.lock().await.get_mut(&oui).unwrap().enabled = false;
        Ok(())
    }

    async fn enable_org(&self, oui: u64) -> Result<(), ConfigServerError> {
        self.payers.lock().await.get_mut(&oui).unwrap().enabled = true;
        Ok(())
    }

    async fn list_orgs(&self) -> Result<Vec<Org>, ConfigServerError> {
        Ok(self
            .payers
            .lock()
            .await
            .iter()
            .map(|(oui, config)| Org {
                oui: *oui,
                payer: config.payer.clone(),
                locked: !config.enabled,
            })
            .collect())
    }
}

fn packet_report(
    oui: u64,
    timestamp: u64,
    payload_size: u32,
    payload_hash: Vec<u8>,
    free: bool,
) -> PacketRouterPacketReport {
    PacketRouterPacketReport {
        received_timestamp: Utc.timestamp_opt(timestamp as i64, 0).unwrap(),
        oui,
        net_id: 0,
        rssi: 0,
        free,
        frequency: 0,
        snr: 0.0,
        data_rate: DataRate::Fsk50,
        region: Region::As9231,
        gateway: PublicKeyBinary::from(vec![]),
        payload_hash,
        payload_size,
        packet_type: PacketType::Uplink,
    }
}

fn join_packet_report(
    oui: u64,
    timestamp: u64,
    payload_size: u32,
    payload_hash: Vec<u8>,
) -> PacketRouterPacketReport {
    PacketRouterPacketReport {
        received_timestamp: Utc.timestamp_opt(timestamp as i64, 0).unwrap(),
        oui,
        net_id: 0,
        rssi: 0,
        free: true,
        frequency: 0,
        snr: 0.0,
        data_rate: DataRate::Fsk50,
        region: Region::As9231,
        gateway: PublicKeyBinary::from(vec![]),
        payload_hash,
        payload_size,
        packet_type: PacketType::Join,
    }
}

fn valid_packet(
    timestamp: u64,
    payload_size: u32,
    payload_hash: Vec<u8>,
    paid: bool,
) -> ValidPacket {
    ValidPacket {
        payload_size,
        payload_hash,
        gateway: vec![],
        num_dcs: if paid {
            payload_size_to_dc(payload_size as u64) as u32
        } else {
            0
        },
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

#[sqlx::test]
async fn test_config_unlocking(pool: PgPool) -> anyhow::Result<()> {
    let payer = PublicKeyBinary::from(vec![0]);

    // Set up orgs:
    let orgs = MockConfigServer::default();
    orgs.insert(0_u64, payer.clone()).await;
    // Set up balances:
    let solana_network = TestSolanaClientMap::default();
    solana_network.insert(&payer, 3).await;

    // Set up cache:
    let mut txn = pool.begin().await?;
    txn.add_burned_amount(&payer, 3).await?;
    txn.commit().await?;
    let balances = BalanceCache::new(&pool, solana_network.clone()).await?;

    // Set up verifier:
    let mut verifier = Verifier {
        debiter: balances.clone(),
        config_server: orgs.clone(),
    };
    let mut valid_packets = Vec::new();
    let mut invalid_packets = Vec::new();
    let mut pending_burn_txn = pool.begin().await?;
    verifier
        .verify(
            1,
            &mut pending_burn_txn,
            stream::iter(vec![
                packet_report(0, 0, 24, vec![1], false),
                packet_report(0, 1, 48, vec![2], false),
                packet_report(0, 2, 1, vec![3], false),
            ]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();
    pending_burn_txn.commit().await?;

    assert!(!orgs.payers.lock().await.get(&0).unwrap().enabled);

    // Update the solana network:
    solana_network.insert(&payer, 50).await;

    let (trigger, listener) = triggered::trigger();

    // Calling monitor funds should re-enable the org and update the
    // verifier's cache
    let solana = solana_network.clone();
    let balance_cache = balances.balances().clone();
    let orgs_clone = orgs.clone();
    tokio::spawn(async move {
        orgs_clone
            .monitor_funds(
                solana.clone(),
                balance_cache,
                1,
                Duration::from_secs(100),
                listener,
            )
            .await
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    // We should be re-enabled
    assert!(orgs.payers.lock().await.get(&0).unwrap().enabled);
    assert_eq!(
        balances
            .balances()
            .lock()
            .await
            .get(&payer)
            .unwrap()
            .balance,
        50
    );

    trigger.trigger();

    let mut pending_burn_txn = pool.begin().await?;
    verifier
        .verify(
            1,
            &mut pending_burn_txn,
            stream::iter(vec![
                packet_report(0, 0, 24, vec![1], false),
                packet_report(0, 1, 48, vec![2], false),
                packet_report(0, 2, 1, vec![3], false),
            ]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();
    pending_burn_txn.commit().await?;

    // Still enabled:
    assert!(orgs.payers.lock().await.get(&0).unwrap().enabled);

    let payer_account = balances
        .get_payer_balance(&payer)
        .await
        .expect("known payer");
    assert_eq!(
        payer_account.balance, 50,
        "balance has not been deducted because no solana burn has been sent"
    );
    assert_eq!(
        payer_account.burned, 7,
        "burned is previous amount plus new sufficient amount"
    );

    Ok(())
}

#[sqlx::test]
async fn test_verifier_free_packets(pool: PgPool) -> anyhow::Result<()> {
    // Org packets
    let packets = vec![
        packet_report(0, 0, 24, vec![4], true),
        packet_report(0, 1, 48, vec![5], true),
        packet_report(0, 2, 1, vec![6], true),
    ];

    let org_pubkey = PublicKeyBinary::from(vec![0]);

    // Set up orgs:
    let orgs = MockConfigServer::default();
    orgs.insert(0_u64, org_pubkey.clone()).await;

    // Set up balances:
    let solana_network = TestSolanaClientMap::default();
    solana_network.insert(&org_pubkey, 5).await;
    let balances = BalanceCache::new(&pool, solana_network).await?;

    // Set up output:
    let mut valid_packets = Vec::new();
    let mut invalid_packets = Vec::new();

    // Set up verifier:
    let mut verifier = Verifier {
        debiter: balances.clone(),
        config_server: orgs,
    };

    // Run the verifier:
    let mut pending_burn_txn = pool.begin().await?;
    verifier
        .verify(
            1,
            &mut pending_burn_txn,
            stream::iter(packets),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();
    pending_burn_txn.commit().await?;

    // Verify packet reports:
    assert_eq!(
        valid_packets,
        vec![
            valid_packet(0, 24, vec![4], false),
            valid_packet(1000, 48, vec![5], false),
            valid_packet(2000, 1, vec![6], false),
        ]
    );

    assert!(invalid_packets.is_empty());

    let payers = verifier.config_server.payers.lock().await;
    assert!(payers.get(&0).unwrap().enabled);

    let payer_balance = balances
        .get_payer_balance(&org_pubkey)
        .await
        .expect("known payer");
    assert_eq!(payer_balance.balance, 5, "balance should not have changed");
    assert_eq!(payer_balance.burned, 0, "nothing should be burned");

    Ok(())
}

#[sqlx::test]
async fn test_verifier(pool: PgPool) -> anyhow::Result<()> {
    let packets = vec![
        // Packets for first OUI
        packet_report(0, 0, 24, vec![1], false),
        packet_report(0, 1, 48, vec![2], false),
        packet_report(0, 2, 1, vec![3], false),
        join_packet_report(0, 3, 1, vec![4]),
        // Packets for second OUI
        packet_report(1, 0, 24, vec![4], false),
        packet_report(1, 1, 48, vec![5], false),
        packet_report(1, 2, 1, vec![6], false),
        join_packet_report(1, 3, 1, vec![4]),
        // Packets for third OUI
        packet_report(2, 0, 24, vec![7], false),
        join_packet_report(2, 1, 1, vec![4]),
    ];
    // Set up orgs:
    let orgs = MockConfigServer::default();
    orgs.insert(0_u64, PublicKeyBinary::from(vec![0])).await;
    orgs.insert(1_u64, PublicKeyBinary::from(vec![1])).await;
    orgs.insert(2_u64, PublicKeyBinary::from(vec![2])).await;
    // Set up balances:
    let solana_network = TestSolanaClientMap::default();
    solana_network
        .insert(&PublicKeyBinary::from(vec![0]), 3)
        .await;
    solana_network
        .insert(&PublicKeyBinary::from(vec![1]), 5)
        .await;
    solana_network
        .insert(&PublicKeyBinary::from(vec![2]), 2)
        .await;
    let balances = BalanceCache::new(&pool, solana_network.clone()).await?;

    // Set up output:
    let mut valid_packets = Vec::new();
    let mut invalid_packets = Vec::new();
    // Set up verifier:
    let mut verifier = Verifier {
        debiter: balances,
        config_server: orgs,
    };

    // Run the verifier:
    let mut pending_burn_txn = pool.begin().await?;
    verifier
        .verify(
            1,
            &mut pending_burn_txn,
            stream::iter(packets),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();
    pending_burn_txn.commit().await?;

    // Verify packet reports:
    assert_eq!(
        valid_packets,
        vec![
            // First two packets for OUI #0 are valid
            valid_packet(0, 24, vec![1], true),
            valid_packet(1000, 48, vec![2], true),
            // All packets for OUI #1 are valid
            valid_packet(0, 24, vec![4], true),
            valid_packet(1000, 48, vec![5], true),
            valid_packet(2000, 1, vec![6], true),
            // All packets for OUI #2 are valid
            valid_packet(0, 24, vec![7], true),
        ]
    );

    assert_eq!(invalid_packets, vec![invalid_packet(1, vec![3]),]);

    // Verify that only org #0 is disabled:
    let payers = verifier.config_server.payers.lock().await;
    assert!(!payers.get(&0).unwrap().enabled);
    assert!(payers.get(&1).unwrap().enabled);
    assert!(payers.get(&2).unwrap().enabled);

    Ok(())
}

#[sqlx::test]
async fn test_end_to_end(pool: PgPool) -> anyhow::Result<()> {
    let payer = PublicKeyBinary::from(vec![0]);

    // Our balance and packet size has to surpass BURN_THRESHOLD
    // for burning to consider the verified packets.
    const STARTING_BALANCE: u64 = BURN_THRESHOLD as u64 * 3;
    const LARGE_PACKET_SIZE: u32 = (BURN_THRESHOLD as u64 * BYTES_PER_DC) as u32;

    // Solana network:
    let solana_network = TestSolanaClientMap::default();
    solana_network.insert(&payer, STARTING_BALANCE).await; // Start with 3 data credits

    // Balance cache:
    let balance_cache = BalanceCache::new(&pool, solana_network.clone()).await?;

    // Burner:
    let mut burner = Burner::new(
        pool.clone(),
        &balance_cache,
        Duration::default(), // Burn period does not matter, we manually burn
        solana_network.clone(),
    );

    // Orgs:
    let orgs = MockConfigServer::default();
    orgs.insert(0_u64, payer.clone()).await;

    // Packet output:
    let mut valid_packets = Vec::new();
    let mut invalid_packets = Vec::new();

    // Set up verifier:
    let mut verifier = Verifier {
        debiter: balance_cache.clone(),
        config_server: orgs,
    };

    // Verify four packets, each costing one DC. The last one should be invalid
    let mut pending_burn_txn = pool.begin().await?;
    verifier
        .verify(
            1,
            &mut pending_burn_txn,
            stream::iter(vec![
                packet_report(0, 0, LARGE_PACKET_SIZE, vec![1], false),
                packet_report(0, 1, LARGE_PACKET_SIZE, vec![2], false),
                packet_report(0, 2, LARGE_PACKET_SIZE, vec![3], false),
                packet_report(0, 3, LARGE_PACKET_SIZE, vec![4], false),
            ]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();
    pending_burn_txn.commit().await?;

    // Org 0 should be disabled now:
    assert!(
        !verifier
            .config_server
            .payers
            .lock()
            .await
            .get(&0)
            .unwrap()
            .enabled
    );

    assert_eq!(
        valid_packets,
        vec![
            valid_packet(0, LARGE_PACKET_SIZE, vec![1], true),
            valid_packet(1000, LARGE_PACKET_SIZE, vec![2], true),
            valid_packet(2000, LARGE_PACKET_SIZE, vec![3], true),
        ]
    );

    // Last packet is invalid:
    assert_eq!(
        invalid_packets,
        vec![invalid_packet(LARGE_PACKET_SIZE, vec![4])]
    );

    // Check current balance:
    // Check that 3x the BURN_THRESHOLD DC are pending to be burned:
    let balance = verifier
        .debiter
        .get_payer_balance(&payer)
        .await
        .expect("known payer");
    assert_eq!(balance.balance, STARTING_BALANCE);
    assert_eq!(balance.burned, STARTING_BALANCE);

    // Initiate the burn:
    burner.burn().await.unwrap();

    // Now that we've burn, the balances and burn amount should be reset:
    let balance = verifier
        .debiter
        .get_payer_balance(&payer)
        .await
        .expect("known payer");
    assert_eq!(balance.balance, 0);
    assert_eq!(balance.burned, 0);

    // Pending burns should be empty as well:
    let payer_balance = balance_cache
        .get_payer_balance(&payer)
        .await
        .expect("known payer");
    assert_eq!(payer_balance.burned, 0, "pending was burned");

    // Additionally, the balance on the solana network should be zero:
    let solana_balance = solana_network.get_payer_balance(&payer).await;
    assert_eq!(solana_balance, 0, "solana balance");

    // Attempting to validate one packet should fail now:
    valid_packets.clear();
    invalid_packets.clear();

    let mut pending_burn_txn = pool.begin().await?;
    verifier
        .verify(
            1,
            &mut pending_burn_txn,
            stream::iter(vec![packet_report(0, 4, LARGE_PACKET_SIZE, vec![5], false)]),
            &mut valid_packets,
            &mut invalid_packets,
        )
        .await
        .unwrap();
    pending_burn_txn.commit().await?;

    assert_eq!(valid_packets, vec![]);

    assert_eq!(
        invalid_packets,
        vec![invalid_packet(LARGE_PACKET_SIZE, vec![5])]
    );

    Ok(())
}

#[sqlx::test]
async fn test_pending_txns(pool: PgPool) -> anyhow::Result<()> {
    const CONFIRMED_BURN_AMOUNT: u64 = 7;
    const UNCONFIRMED_BURN_AMOUNT: u64 = 11;
    let payer: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
        .parse()
        .unwrap();
    let mut ledger = HashMap::new();
    ledger.insert(
        payer.clone(),
        CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT,
    );
    let mut cache = HashMap::new();
    cache.insert(
        payer.clone(),
        PayerAccount {
            balance: CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT,
            burned: CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT,
        },
    );
    let solana_network = TestSolanaClientMap::default();
    solana_network
        .insert(&payer, CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT)
        .await;

    // Add both the burn amounts to the pending burns table
    {
        let mut transaction = pool.begin().await.unwrap();
        transaction
            .add_burned_amount(&payer, CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }

    // First transaction is confirmed
    {
        let txn = solana_network
            .make_burn_transaction(&payer, CONFIRMED_BURN_AMOUNT)
            .await
            .unwrap();

        pool.do_add_pending_transaction(
            &payer,
            CONFIRMED_BURN_AMOUNT,
            txn.get_signature(),
            Utc::now() - chrono::Duration::minutes(2),
        )
        .await
        .unwrap();
        solana_network.add_confirmed(*txn.get_signature()).await;
        solana_network.submit_transaction(&txn).await.unwrap();
    }

    // Second is unconfirmed
    // Make submission time in past to bypass confirm txn sleep
    {
        let txn = solana_network
            .make_burn_transaction(&payer, UNCONFIRMED_BURN_AMOUNT)
            .await
            .unwrap();
        pool.do_add_pending_transaction(
            &payer,
            UNCONFIRMED_BURN_AMOUNT,
            txn.get_signature(),
            Utc::now() - chrono::Duration::minutes(3),
        )
        .await
        .unwrap();
    }

    // Confirm pending transactions
    confirm_pending_txns(&pool, &solana_network, &Arc::new(Mutex::new(cache)))
        .await
        .unwrap();

    let pending_burn: Burn = sqlx::query_as("SELECT * FROM pending_burns LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    // The unconfirmed burn amount should be what's left
    assert_eq!(pending_burn.amount, UNCONFIRMED_BURN_AMOUNT);

    Ok(())
}

#[sqlx::test]
async fn test_burn_with_pending_txn_triggers_confirmation(pool: PgPool) -> anyhow::Result<()> {
    let org_one = PublicKeyBinary::from(vec![0]);
    let org_two = PublicKeyBinary::from(vec![1]);

    let solana_network = TestSolanaClientMap::default();
    solana_network.insert(&org_one, 50_000).await;
    solana_network.insert(&org_two, 50_000).await;

    // Add both burns
    let mut transaction = pool.begin().await?;
    transaction.add_burned_amount(&org_one, 10_000).await?;
    transaction.add_burned_amount(&org_two, 20_000).await?;
    transaction.commit().await?;

    // Add random signature so nothing can be confirmed automatically
    solana_network.add_confirmed(Signature::new_unique()).await;

    // Mark the first txn as pending
    {
        let txn = solana_network
            .make_burn_transaction(&org_one, 10_000)
            .await?;

        pool.do_add_pending_transaction(
            &org_one,
            10_000,
            txn.get_signature(),
            Utc::now() - chrono::Duration::minutes(2),
        )
        .await?;
        solana_network.submit_transaction(&txn).await?;

        // Add signature after submission so the txn is marked as pending but
        // can be confirmed later in the test.
        solana_network.add_confirmed(*txn.get_signature()).await;
    }

    // Assert initial pending values
    assert_pending_burns(&pool, &[(&org_one, 10_000), (&org_two, 20_000)]).await?;

    // Create balance cache and burner after adding pending burns so we pick up
    // correct balance values.
    let balances = BalanceCache::new(&pool, solana_network.clone()).await?;
    let mut burner = Burner::new(
        pool.clone(),
        &balances,
        Duration::default(), // Burn period does not matter, we manually burn
        solana_network.clone(),
    );

    // No burn should happen, pending values should be the same
    match burner.burn().await {
        Err(BurnError::ExistingPendingTransactions(1)) => (),
        other => panic!("unexpected burn response: {other:?}"),
    }
    assert_pending_burns(&pool, &[(&org_one, 10_000), (&org_two, 20_000)]).await?;

    // Confirm existing and assert burn was deducted
    confirm_pending_txns(&pool, &solana_network, &balances.balances()).await?;
    assert_pending_burns(&pool, &[(&org_one, 0), (&org_two, 20_000)]).await?;

    // Burn again, and all amounts should be zeroed
    match burner.burn().await {
        Ok(()) => (),
        other => panic!("unexpected burn response: {other:?}"),
    }
    assert_pending_burns(&pool, &[(&org_one, 0), (&org_two, 0)]).await?;

    Ok(())
}

async fn assert_pending_burns(
    pool: &PgPool,
    expected: &[(&PublicKeyBinary, u64)],
) -> anyhow::Result<()> {
    let burns = sqlx::query_as("SELECT * from pending_burns")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|burn: Burn| (burn.payer, burn.amount))
        .collect::<std::collections::HashMap<_, _>>();

    for (key, expected_amount) in expected {
        let amount = burns
            .get(key)
            .unwrap_or_else(|| panic!("{key:?} does not exist in pending burns"));
        assert_eq!(amount, expected_amount);
    }

    Ok(())
}
