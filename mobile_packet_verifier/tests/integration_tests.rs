use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient,
    mobile_session::{DataTransferEvent, DataTransferSessionReq},
};
use helium_crypto::PublicKeyBinary;
use mobile_packet_verifier::{burner::Burner, pending_burns, pending_txns};
use solana::burn::TestSolanaClientMap;
use sqlx::PgPool;

#[sqlx::test]
fn burn_checks_for_sufficient_balance(pool: PgPool) -> anyhow::Result<()> {
    let payer_insufficent = PublicKeyBinary::from(vec![1]);
    let payer_sufficient = PublicKeyBinary::from(vec![2]);
    const ORIGINAL_BALANCE: u64 = 10_000;

    // Initialize payers with balances
    let solana_network = TestSolanaClientMap::default();
    solana_network
        .insert(&payer_insufficent, ORIGINAL_BALANCE)
        .await;
    solana_network
        .insert(&payer_sufficient, ORIGINAL_BALANCE)
        .await;

    // Add Data Transfer Session for both payers
    save_data_transfer_sessions(
        &pool,
        &[
            (&payer_insufficent, &payer_insufficent, 1_000_000_000), // exceed balance
            (&payer_sufficient, &payer_sufficient, 1_000_000),       // within balance
        ],
    )
    .await?;

    // Ensure we see 2 pending burns
    let pre_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(pre_burns.len(), 2, "2 burns for 2 payers");

    // Setup the Burner
    let (valid_sessions_tx, mut valid_sessions_rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
    );

    // Burn what we can
    burner.burn(&pool).await?;

    // 1 burn succeeded, the other payer has insufficient balance
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 1, "1 burn left");

    // Ensure no pending transactions
    let pending = pending_txns::fetch_all_pending_txns(&pool).await?;
    assert!(
        pending.is_empty(),
        "pending txns should be removed after burn"
    );

    // Ensure balance for payers through solana mock
    assert_eq!(
        solana_network.get_payer_balance(&payer_insufficent).await,
        ORIGINAL_BALANCE,
        "original balance"
    );
    assert!(
        solana_network.get_payer_balance(&payer_sufficient).await < ORIGINAL_BALANCE,
        "reduced balance"
    );

    // Ensure successful data transfer sessions were output
    let written_sessions = get_written_sessions(&mut valid_sessions_rx);
    assert_eq!(written_sessions.len(), 1, "1 data transfer session written");

    Ok(())
}

fn mk_data_transfer_session(
    payer_key: &PublicKeyBinary,
    pubkey: &PublicKeyBinary,
    rewardable_bytes: u64,
) -> DataTransferSessionReq {
    use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
    DataTransferSessionReq {
        data_transfer_usage: DataTransferEvent {
            pub_key: pubkey.clone(),
            upload_bytes: rewardable_bytes / 2,
            download_bytes: rewardable_bytes / 2,
            radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
            event_id: "event-id".to_string(),
            payer: payer_key.clone(),
            timestamp: Utc::now(),
            signature: vec![],
        },
        rewardable_bytes,
        pub_key: pubkey.clone(),
        signature: vec![],
    }
}

async fn save_data_transfer_sessions(
    pool: &PgPool,
    sessions: &[(&PublicKeyBinary, &PublicKeyBinary, u64)],
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;
    for (payer, pubkey, amount) in sessions {
        let session = mk_data_transfer_session(payer, pubkey, *amount);
        pending_burns::save_data_transfer_session_req(&mut txn, &session, Utc::now()).await?;
    }
    txn.commit().await?;

    Ok(())
}

fn get_written_sessions<T>(rx: &mut tokio::sync::mpsc::Receiver<T>) -> Vec<T> {
    let mut written_sessions = vec![];
    while let Ok(session) = rx.try_recv() {
        written_sessions.push(session);
    }
    written_sessions
}
