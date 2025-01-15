use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient,
    mobile_session::{DataTransferEvent, DataTransferSessionReq},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
use mobile_packet_verifier::{burner::Burner, pending_burns};
use solana::{burn::test_client::TestSolanaClientMap, Signature};
use sqlx::PgPool;
use tokio::sync::mpsc::error::TryRecvError;

#[sqlx::test]
fn burn_checks_for_sufficient_balance(pool: PgPool) -> anyhow::Result<()> {
    let payer_insufficient = PublicKeyBinary::from(vec![1]);
    let payer_sufficient = PublicKeyBinary::from(vec![2]);
    const ORIGINAL_BALANCE: u64 = 10_000;

    // Initialize payers with balances
    let mut solana_network = TestSolanaClientMap::default();
    solana_network
        .insert(payer_insufficient.clone(), ORIGINAL_BALANCE)
        .await;
    solana_network
        .insert(payer_sufficient.clone(), ORIGINAL_BALANCE)
        .await;

    // Add Data Transfer Sessiosn for both payers
    let mut txn = pool.begin().await?;
    let session_one =
        mk_data_transfer_session(&payer_insufficient, &payer_insufficient, 1_000_000_000); // exceeds balance
    let session_two = mk_data_transfer_session(&payer_sufficient, &payer_sufficient, 1_000_000); // within balance
    pending_burns::save(&mut txn, &session_one, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_two, Utc::now()).await?;
    txn.commit().await?;

    // Ensure we see 2 pending burns
    let pre_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(pre_burns.len(), 2, "2 burns for 2 payers");

    // Burn what we can
    let (valid_sessions_tx, mut rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(valid_sessions, solana_network.clone());
    burner.burn(&pool).await?;

    // 1 burn succeeded, the other payer has insufficient balance
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 1, "1 burn left");

    // Ensure no pending transactions
    let pending = pending_burns::fetch_all_pending_txns(&pool).await?;
    assert!(pending.is_empty(), "pending txn should be removed");

    // Ensure balance for payers through solana mock
    assert_eq!(
        solana_network.get_payer_balance(&payer_insufficient).await,
        ORIGINAL_BALANCE,
        "original balance"
    );
    assert!(
        solana_network.get_payer_balance(&payer_sufficient).await < ORIGINAL_BALANCE,
        "reduced balance"
    );

    // Ensure successful data transfer sessions were output
    let mut written_sessions = vec![];
    while let Ok(session) = rx.try_recv() {
        written_sessions.push(session);
    }
    assert_eq!(written_sessions.len(), 1, "1 data transfer session written");

    Ok(())
}

#[sqlx::test]
async fn test_confirm_pending_txns(pool: PgPool) -> anyhow::Result<()> {
    let payer_one = PublicKeyBinary::from(vec![1]);

    let mut solana_network = TestSolanaClientMap::default();
    solana_network.insert(payer_one.clone(), 10_000).await;

    // First transaction is confirmed
    // Make submission time in past to bypass confirm txn sleep
    let confirmed_signature = Signature::new_unique();
    pending_burns::do_add_pending_transaction(
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
    pending_burns::do_add_pending_transaction(
        &pool,
        &payer_one,
        500,
        &unconfirmed_signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Tell Mock Solana which txn to confirm
    solana_network.add_confirmed(confirmed_signature).await;
    // solana_network.add_confirmed(unconfirmed_txn).await; // uncomment for failure

    assert_eq!(pending_burns::fetch_all_pending_txns(&pool).await?.len(), 2);
    let (valid_sessions_tx, _rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(valid_sessions, solana_network);
    burner.confirm_pending_txns(&pool).await?;
    // confirmed and unconfirmed txns have been cleared
    assert_eq!(pending_burns::fetch_all_pending_txns(&pool).await?.len(), 0);

    // The unconfirmed txn is moved back to ready for burning
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 1, "the unconfirmed txn has moved back to burn");

    let payer_burn = &burns[0];
    assert_eq!(payer_burn.payer, payer_two);
    assert_eq!(payer_burn.total_dcs, pending_burns::bytes_to_dc(2_000));
    assert_eq!(payer_burn.sessions.len(), 1);

    Ok(())
}

#[sqlx::test]
fn confirming_pending_txns_writes_out_sessions(pool: PgPool) -> anyhow::Result<()> {
    // Insert a pending txn for some sessions.
    // Insert more sessions after the pending txn.
    // confirm txns and ensure sessions are written.
    // ensure sessions not written for that pendint txn are still there.

    let payer = PublicKeyBinary::from(vec![0]);
    let pubkey_one = PublicKeyBinary::from(vec![1]);
    let pubkey_two = PublicKeyBinary::from(vec![2]);

    // Add a transfer session for payer
    let session_one = mk_data_transfer_session(&payer, &pubkey_one, 1_000);
    let session_two = mk_data_transfer_session(&payer, &pubkey_two, 1_000);
    let mut txn = pool.begin().await?;
    pending_burns::save(&mut txn, &session_one, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_two, Utc::now()).await?;
    txn.commit().await?;

    // Mark the session as pending
    let signature = Signature::new_unique();
    pending_burns::do_add_pending_transaction(
        &pool,
        &payer,
        1_000,
        &signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Add another transfer session that should not be written out
    let session_three = mk_data_transfer_session(&payer, &pubkey_one, 5_000);
    let session_four = mk_data_transfer_session(&payer, &pubkey_two, 5_000);
    let mut txn = pool.begin().await?;
    pending_burns::save(&mut txn, &session_three, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_four, Utc::now()).await?;
    txn.commit().await?;

    let solana_network = TestSolanaClientMap::default();
    let (valid_sessions_tx, mut rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let burner = Burner::new(valid_sessions, solana_network);
    burner.confirm_pending_txns(&pool).await?;

    // In flight session is written out
    let mut written_sessions = vec![];
    while let Ok(session) = rx.try_recv() {
        written_sessions.push(session);
    }
    assert_eq!(written_sessions.len(), 2, "2 data transfer session written");

    // Late added session is still waiting for burn
    let payer_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(payer_burns.len(), 1);

    let payer_burn = &payer_burns[0];
    assert_eq!(payer_burn.payer, payer);
    // DC is calculated for each session individually, then summed
    assert_eq!(
        payer_burn.total_dcs,
        pending_burns::bytes_to_dc(5_000) + pending_burns::bytes_to_dc(5_000),
    );
    assert_eq!(payer_burn.sessions.len(), 2);

    Ok(())
}

#[sqlx::test]
fn unconfirmed_pending_txn_moves_data_session_back_to_primary_table(
    pool: PgPool,
) -> anyhow::Result<()> {
    // After making a pending_txn, and the data sessions are moved for
    // processing If the txn cannot be finalized, the data sessions that were
    // going to be written need to be moved back to being considerd for burn.

    let payer = PublicKeyBinary::from(vec![0]);
    let pubkey_one = PublicKeyBinary::from(vec![1]);
    let pubkey_two = PublicKeyBinary::from(vec![2]);

    // Insert sessions
    let session_one = mk_data_transfer_session(&payer, &pubkey_one, 1_000);
    let session_two = mk_data_transfer_session(&payer, &pubkey_two, 1_000);
    let mut txn = pool.begin().await?;
    pending_burns::save(&mut txn, &session_one, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_two, Utc::now()).await?;
    txn.commit().await?;

    // Mark as pending txns
    let signature = Signature::new_unique();
    pending_burns::do_add_pending_transaction(
        &pool,
        &payer,
        1_000,
        &signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Insert more sessions
    let session_three = mk_data_transfer_session(&payer, &pubkey_one, 5_000);
    let session_four = mk_data_transfer_session(&payer, &pubkey_two, 5_000);
    let mut txn = pool.begin().await?;
    pending_burns::save(&mut txn, &session_three, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_four, Utc::now()).await?;
    txn.commit().await?;

    // There are sessions for burning, but we cannot because there are also pending txns.
    let payer_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(
        payer_burns.len(),
        1,
        "still have sessions ready for burning"
    );
    assert_eq!(
        payer_burns[0].total_dcs,
        pending_burns::bytes_to_dc(5_000) + pending_burns::bytes_to_dc(5_000)
    );
    let txn_count = pending_burns::pending_txn_count(&pool).await?;
    assert_eq!(txn_count, 1, "there should be a single pending txn");

    // Fail the pending txns
    let (valid_sessions_tx, _rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let mut solana_network = TestSolanaClientMap::default();
    // Adding a random confirmed txn will cause other txns to not be considered finalized
    solana_network.add_confirmed(Signature::new_unique()).await;

    let burner = Burner::new(valid_sessions, solana_network.clone());
    burner.confirm_pending_txns(&pool).await?;

    // Sessions are merged with 2nd set of sessions for burning
    let txn_count = pending_burns::pending_txn_count(&pool).await?;
    assert_eq!(txn_count, 0, "should be no more pending txns");

    // There is still only 1 payer burn, but the amount to burn contains both sets of sessions.
    let payer_burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(payer_burns.len(), 1, "still have 1 burn to go");
    assert_eq!(
        payer_burns[0].total_dcs,
        pending_burns::bytes_to_dc(5_000) + pending_burns::bytes_to_dc(5_000)
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
    let session_one = mk_data_transfer_session(&payer, &pubkey_one, 1_000);
    let session_two = mk_data_transfer_session(&payer, &pubkey_two, 1_000);
    let mut txn = pool.begin().await?;
    pending_burns::save(&mut txn, &session_one, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_two, Utc::now()).await?;
    txn.commit().await?;

    // Mark the session as pending
    let signature = Signature::new_unique();
    pending_burns::do_add_pending_transaction(
        &pool,
        &payer,
        1_000,
        &signature,
        Utc::now() - chrono::Duration::minutes(2),
    )
    .await?;

    // Add more sessions that are ready for burning
    let session_three = mk_data_transfer_session(&payer, &pubkey_one, 5_000);
    let session_four = mk_data_transfer_session(&payer, &pubkey_two, 5_000);
    let mut txn = pool.begin().await?;
    pending_burns::save(&mut txn, &session_three, Utc::now()).await?;
    pending_burns::save(&mut txn, &session_four, Utc::now()).await?;
    txn.commit().await?;

    // Burn does nothing because of pending transactions
    let (valid_sessions_tx, mut rx) = tokio::sync::mpsc::channel(10);
    let valid_sessions = FileSinkClient::new(valid_sessions_tx, "test");
    let mut solana_network = TestSolanaClientMap::default();
    solana_network.insert(payer.clone(), 10_000).await;

    let burner = Burner::new(valid_sessions, solana_network.clone());
    burner.burn(&pool).await?;

    match rx.try_recv() {
        Ok(_) => panic!("nothing should be written"),
        Err(TryRecvError::Disconnected) => panic!("file sink client was incorrectly dropped"),
        Err(TryRecvError::Empty) => (),
    }

    // Remove pending burn.
    // Data Transfer Sessions should go through now.
    pending_burns::remove_pending_transaction_success(&pool, &signature).await?;
    burner.burn(&pool).await?;

    let mut written_sessions = vec![];
    while let Ok(session) = rx.try_recv() {
        written_sessions.push(session);
    }
    assert_eq!(written_sessions.len(), 2, "2 data transfer session written");

    Ok(())
}

fn mk_data_transfer_session(
    payer_key: &PublicKeyBinary,
    pubkey: &PublicKeyBinary,
    rewardable_bytes: u64,
) -> DataTransferSessionReq {
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
