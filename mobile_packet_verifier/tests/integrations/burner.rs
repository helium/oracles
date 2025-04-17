use anyhow::Context;
use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient,
    mobile_session::{DataTransferEvent, DataTransferSessionReq},
};
use helium_crypto::PublicKeyBinary;
use mobile_packet_verifier::{burner::Burner, bytes_to_dc, pending_burns, pending_txns};
use solana::{burn::TestSolanaClientMap, Signature};
use sqlx::PgPool;
use tokio::sync::mpsc::error::TryRecvError;

#[sqlx::test]
fn burn_checks_for_sufficient_balance(pool: PgPool) -> anyhow::Result<()> {
    let payer_insufficent = PublicKeyBinary::from(vec![1]);
    let payer_sufficient = PublicKeyBinary::from(vec![2]);
    const ORIGINAL_BALANCE: u64 = 10_000;

    // Initialize payers with balances
    let solana_network = TestSolanaClientMap::default();
    solana_network
        .insert(&payer_insufficent.to_string(), ORIGINAL_BALANCE)
        .await;
    solana_network
        .insert(&payer_sufficient.to_string(), ORIGINAL_BALANCE)
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
        solana_network
            .get_escrow_balance(&payer_insufficent.to_string())
            .await,
        ORIGINAL_BALANCE,
        "original balance"
    );
    assert!(
        solana_network
            .get_escrow_balance(&payer_sufficient.to_string())
            .await
            < ORIGINAL_BALANCE,
        "reduced balance"
    );

    // Ensure successful data transfer sessions were output
    let written_sessions = get_written_sessions(&mut valid_sessions_rx);
    assert_eq!(written_sessions.len(), 1, "1 data transfer session written");

    Ok(())
}

#[sqlx::test]
async fn test_confirm_pending_txns(pool: PgPool) -> anyhow::Result<()> {
    let payer_one = PublicKeyBinary::from(vec![1]);
    let payer_two = PublicKeyBinary::from(vec![2]);

    let solana_network = TestSolanaClientMap::default();
    solana_network.insert(&payer_one.to_string(), 10_000).await;

    save_data_transfer_sessions(
        &pool,
        &[
            (&payer_one, &payer_one, 1_000),
            (&payer_two, &payer_two, 1_000),
        ],
    )
    .await?;

    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 2, "two burns for two payers");

    // First transaction is confirmed
    // Make submission time in past to bypass confirm txn sleep
    let confirmed_signature = Signature::new_unique();
    pending_txns::do_add_pending_txn(
        &pool,
        &payer_one,
        1_000,
        &confirmed_signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Second transaction is unconfirmed
    // Make submission time in past to bypass confirm txn sleep
    let unconfirmed_signature = Signature::new_unique();
    pending_txns::do_add_pending_txn(
        &pool,
        &payer_two,
        500,
        &unconfirmed_signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;
    assert_eq!(pending_txns::fetch_all_pending_txns(&pool).await?.len(), 2);

    // Tell mock Solana which txn to confirm
    solana_network.add_confirmed(confirmed_signature).await;
    // solana_network.add_confirmed(unconfirmed_signature).await; // uncomment for failure

    let (valid_sessions_tx, _valid_sessions_rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
    );
    burner.confirm_pending_txns(&pool).await?;

    // confirmed and unconfirmed txns have been cleared
    assert_eq!(pending_txns::fetch_all_pending_txns(&pool).await?.len(), 0);

    // The unconfirmed txn is moved back to ready for burning
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 1, "the unconfirmed txn has moved back to burn");

    let payer_burn = &burns[0];
    assert_eq!(payer_burn.payer, payer_two);
    assert_eq!(payer_burn.total_dcs, bytes_to_dc(2_000));
    assert_eq!(payer_burn.sessions.len(), 1);

    Ok(())
}

#[sqlx::test]
fn confirmed_pending_txns_writes_out_sessions(pool: PgPool) -> anyhow::Result<()> {
    // Insert a pending txn for some sessions.
    // Insert more sessions after the pending txn.
    // Confirming a txn should write out the sessions that were present before the txn.
    // Sessions written after the txn is pending should not be written.

    let payer = PublicKeyBinary::from(vec![0]);
    let pubkey_one = PublicKeyBinary::from(vec![1]);
    let pubkey_two = PublicKeyBinary::from(vec![2]);

    // Add a transfer session for payer
    save_data_transfer_sessions(
        &pool,
        &[(&payer, &pubkey_one, 1_000), (&payer, &pubkey_two, 1_000)],
    )
    .await?;

    // Mark the session as pending
    let signature = Signature::new_unique();
    pending_txns::do_add_pending_txn(
        &pool,
        &payer,
        1_000,
        &signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Add another session that should NOT be written out
    save_data_transfer_sessions(
        &pool,
        &[(&payer, &pubkey_one, 5_000), (&payer, &pubkey_two, 5_000)],
    )
    .await?;

    let solana_network = TestSolanaClientMap::default();
    let (valid_sessions_tx, mut valid_sessions_rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
    );
    burner.confirm_pending_txns(&pool).await?;

    // In flight session is written out
    let written_sessions = get_written_sessions(&mut valid_sessions_rx);
    assert_eq!(
        written_sessions.len(),
        2,
        "2 data transfer sessions written"
    );

    // Late added session is still waiting for burn
    let payer_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(payer_burns.len(), 1);

    let payer_burn = &payer_burns[0];
    assert_eq!(payer_burn.payer, payer);
    // DC is calculated for each session individually, then summed
    assert_eq!(
        payer_burn.total_dcs,
        bytes_to_dc(5_000) + bytes_to_dc(5_000)
    );
    assert_eq!(payer_burn.sessions.len(), 2);

    Ok(())
}

#[sqlx::test]
fn unconfirmed_pending_txn_moves_data_session_back_to_primary_table(
    pool: PgPool,
) -> anyhow::Result<()> {
    // After making a pending_txn, and the data sessions are moved for
    // processing. If the txn cannot be finalized, the data sessions that were
    // going to be written need to be moved back to being considered for burn.

    let payer = PublicKeyBinary::from(vec![0]);
    let pubkey_one = PublicKeyBinary::from(vec![1]);
    let pubkey_two = PublicKeyBinary::from(vec![2]);

    // Insert sessions
    save_data_transfer_sessions(
        &pool,
        &[(&payer, &pubkey_one, 1_000), (&payer, &pubkey_two, 1_000)],
    )
    .await?;

    // Mark as pending txn
    let signature = Signature::new_unique();
    pending_txns::do_add_pending_txn(
        &pool,
        &payer,
        1_000,
        &signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Insert more sessions
    save_data_transfer_sessions(
        &pool,
        &[(&payer, &pubkey_one, 5_000), (&payer, &pubkey_two, 5_000)],
    )
    .await?;

    // There are sessions for burning, but we cannot because there is also a pending txn.
    let payer_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(
        payer_burns.len(),
        1,
        "still have sessions ready for burning"
    );
    assert_eq!(
        payer_burns[0].total_dcs,
        bytes_to_dc(5_000) + bytes_to_dc(5_000)
    );

    let txn_count = pending_txns::pending_txn_count(&pool).await?;
    assert_eq!(txn_count, 1, "there should be a single pending txn");

    // Fail the pending txn.
    // Adding a random confirmed txn wll cause other txns to not be considered finalized
    let solana_network = TestSolanaClientMap::default();
    solana_network.add_confirmed(Signature::new_unique()).await;

    let (valid_sessions_tx, _valid_sessions_rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
    );
    // Txn fails to be finalized here
    burner.confirm_pending_txns(&pool).await?;
    let txn_count = pending_txns::pending_txn_count(&pool).await?;
    assert_eq!(txn_count, 0, "should be no more pending txns");

    // Sessions are merged with 2nd set of sessions for burning.
    // There is still only 1 payer bur, but the amount contains both sets of sessions.
    let payer_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(payer_burns.len(), 1, "still have 1 butn to go");
    assert_eq!(
        payer_burns[0].total_dcs,
        // 1_000 from failed sessions + 5_000 from late sessions
        bytes_to_dc(6_000) + bytes_to_dc(6_000)
    );

    Ok(())
}

#[sqlx::test]
fn will_not_burn_when_pending_txns(pool: PgPool) -> anyhow::Result<()> {
    // Trigger a burn when there are data sessions that can be burned _and_ pending txns.
    // Nothing should happen until the pending_txns are gone.

    let payer = PublicKeyBinary::from(vec![0]);
    let pubkey_one = PublicKeyBinary::from(vec![1]);
    let pubkey_two = PublicKeyBinary::from(vec![2]);

    // Add a transfer session for payer
    save_data_transfer_sessions(
        &pool,
        &[(&payer, &pubkey_one, 1_000), (&payer, &pubkey_two, 1_000)],
    )
    .await?;

    // Mark the session as pending
    let signature = Signature::new_unique();
    pending_txns::do_add_pending_txn(
        &pool,
        &payer,
        1_000,
        &signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Add more sessions that are ready for burning
    save_data_transfer_sessions(
        &pool,
        &[(&payer, &pubkey_one, 5_000), (&payer, &pubkey_two, 5_000)],
    )
    .await?;

    // Burn does nothing because of pending transactions
    let solana_network = TestSolanaClientMap::default();
    solana_network.insert(&payer.to_string(), 10_000).await;

    let (valid_sessions_tx, mut valid_sessions_rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(
        valid_sessions,
        solana_network.clone(),
        0,
        std::time::Duration::default(),
    );
    burner
        .burn(&pool)
        .await
        .context("burning with pending txn")?;

    // No sessions written because of pending txn
    match valid_sessions_rx.try_recv() {
        Ok(_) => panic!("nothing should be written"),
        Err(TryRecvError::Disconnected) => panic!("file sink client was incorrectly dropped"),
        Err(TryRecvError::Empty) => (),
    }

    // Remove pending burn.
    // Data Transfer Sessions should go through now.
    pending_txns::remove_pending_txn_failure(&pool, &signature).await?;
    burner.burn(&pool).await.context("burning after ")?;

    // Written sessions should be all combined values
    let written_sessions = get_written_sessions(&mut valid_sessions_rx);
    assert_eq!(written_sessions.len(), 2, "2 data transfer session written");
    let total_rewardable = written_sessions
        .iter()
        .map(|x| match x {
            file_store::file_sink::Message::Data(_confirm, session) => session.rewardable_bytes,
            _ => panic!("wrong type of Message"),
        })
        .sum::<u64>();
    assert_eq!(total_rewardable, 12_000);

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
