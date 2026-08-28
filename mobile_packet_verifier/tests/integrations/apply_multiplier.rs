//! HIP-150: applying the multiplier to what a payer burns.
//!
//! Sessions accumulate without a multiplier, one row per hotspot per file. The
//! burn joins each row against the ticket history at that row's timestamp.
//!
//! Two things have to hold, and most of these tests are about one or the other:
//!
//! * Bytes that moved before a grant took effect bill at the old rate, and bytes
//!   after it at the new one.
//! * It doesn't matter when we processed the ticket, only when it took effect.
//!   A ticket effective at 1:30 applies to a 1:40 row even if we didn't see the
//!   ticket until 1:55.
//!
//! The arithmetic is not per row. `bytes_to_dc` rounds up off a one-DC floor, so
//! converting every file separately would charge a full DC for each. Rows are
//! grouped by the multiplier the burn resolved, summed within a group, and
//! converted once.

use chrono::{DateTime, Duration, Utc};
use file_store::file_sink::FileSinkClient;
use file_store_oracles::mobile::data_transfer_multiplier::DataTransferMultiplier;
use file_store_oracles::mobile_session::{
    DataTransferEvent, DataTransferSessionIngestReport, DataTransferSessionReq,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{CarrierIdV2, DataTransferRadioAccessTechnology};
use mobile_packet_verifier::{
    banning, bytes_to_dc,
    daemon::handle_data_transfer_session_file,
    multiplier::db::{self, GrantedMultiplier},
    pending_burns,
    routing::RoutingKeys,
};
use rust_decimal::dec;
use sqlx::PgPool;

use crate::common::{self, hotspot_inventory::MobileHotspotInventory};

fn gateway(byte: u8) -> PublicKeyBinary {
    PublicKeyBinary::from(vec![byte])
}

fn payer() -> PublicKeyBinary {
    PublicKeyBinary::from(vec![0])
}

/// A report of `rewardable_bytes` from `gateway`. `event_id` is unique per call
/// so repeated reports are not rejected as duplicates.
fn report(
    gateway: &PublicKeyBinary,
    rewardable_bytes: u64,
    event: &str,
    received_timestamp: DateTime<Utc>,
) -> DataTransferSessionIngestReport {
    DataTransferSessionIngestReport {
        received_timestamp,
        report: DataTransferSessionReq {
            rewardable_bytes,
            pub_key: gateway.clone(),
            signature: vec![],
            carrier_id: CarrierIdV2::Carrier9,
            sampling: false,
            data_transfer_usage: DataTransferEvent {
                pub_key: gateway.clone(),
                upload_bytes: rewardable_bytes,
                download_bytes: 0,
                radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                event_id: event.to_string(),
                payer: payer(),
                timestamp: received_timestamp,
                signature: vec![],
            },
        },
    }
}

/// Record a granted multiplier as taking effect at `effective`, the way the
/// ticket ingestor does.
async fn grant(
    pool: &PgPool,
    gateway: &PublicKeyBinary,
    value: rust_decimal::Decimal,
    effective: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut txn = pool.begin().await?;
    db::save(
        &mut txn,
        &[GrantedMultiplier {
            hotspot_pubkey: gateway.clone(),
            multiplier: DataTransferMultiplier::new(value).expect("valid multiplier"),
            effective_timestamp: effective,
        }],
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// Accumulate a batch of reports as one file landing at `file_ts`.
///
/// `file_ts` becomes the sessions' `last_timestamp`, which is the instant the
/// burn prices them at.
async fn accumulate(
    pool: &PgPool,
    gateways: &[PublicKeyBinary],
    reports: Vec<DataTransferSessionIngestReport>,
    file_ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    let harness = common::setup_iceberg().await?;
    common::hotspot_inventory::seed(
        &harness,
        gateways
            .iter()
            .map(|g| MobileHotspotInventory::known(g, Utc::now() - Duration::hours(6)))
            .collect(),
    )
    .await?;
    let resolver = common::gateway_resolver(&harness).await?;
    let routing_keys: RoutingKeys = gateways.iter().cloned().collect();

    let mut txn = pool.begin().await?;
    let (verified_tx, _verified_rx) = tokio::sync::mpsc::channel(10);
    let verified_sink = FileSinkClient::new(verified_tx, "test");
    let banned_radios = banning::get_banned_radios(&mut txn, Utc::now()).await?;

    handle_data_transfer_session_file(
        &mut txn,
        None,
        None,
        "test_write_id",
        banned_radios,
        &resolver,
        &routing_keys,
        &verified_sink,
        file_ts,
        futures::stream::iter(reports),
    )
    .await?;
    txn.commit().await?;

    Ok(())
}

/// Total DC the burn would charge `payer`.
async fn pending_dc(pool: &PgPool) -> anyhow::Result<u64> {
    let burns = pending_burns::get_all_payer_burns(pool).await?;
    Ok(burns.iter().map(|b| b.total_dcs).sum())
}

/// The baseline that must not move: with no tickets, every hotspot is at 1 and
/// the burn is exactly what it was before HIP-150.
#[sqlx::test]
async fn no_tickets_burns_exactly_what_it_did_before(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);

    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![
            report(&gw, 150_000, "a", Utc::now()),
            report(&gw, 150_000, "b", Utc::now()),
        ],
        Utc::now(),
    )
    .await?;

    // 300,000 bytes summed, converted once: 3 DC. Unchanged by HIP-150.
    assert_eq!(pending_dc(&pool).await?, bytes_to_dc(300_000));
    assert_eq!(pending_dc(&pool).await?, 3);

    Ok(())
}

#[sqlx::test]
async fn a_multiplier_scales_what_is_burned(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let now = Utc::now();

    grant(&pool, &gw, dec!(1.5), now - Duration::hours(1)).await?;
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", now)],
        now,
    )
    .await?;

    // 200,000 bytes -> 2 DC -> x1.5 -> 3.
    assert_eq!(pending_dc(&pool).await?, 3);

    Ok(())
}

/// HIP-150: "rounded down, so a payer never burns more than the multiplier
/// earns". 3 DC at 1.5 is 4.5, which must charge 4 rather than 5.
#[sqlx::test]
async fn the_multiplier_rounds_down(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let now = Utc::now();

    grant(&pool, &gw, dec!(1.5), now - Duration::hours(1)).await?;
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 250_000, "a", now)],
        now,
    )
    .await?;

    // 250,000 bytes -> ceil = 3 DC -> x1.5 = 4.5 -> floor = 4.
    assert_eq!(pending_dc(&pool).await?, 4);

    Ok(())
}

/// Data credits are derived first, then multiplied -- not the other way round.
///
/// HIP-150: "It applies to the data credits derived from that Hotspot's
/// rewardable bytes, not to the bytes themselves: a rewardable-byte count is a
/// measurement and does not change."
///
/// The two orderings give different answers, so this pins ours. 100,000 bytes is
/// 1 DC, and 1 x 1.5 floored is 1. Multiplying the bytes first would give
/// 150,000 bytes, which rounds up to 2.
///
/// 2 would also break the rule the HIP states alongside it -- "a payer never
/// burns more than the multiplier earns" -- because the multiplier earns 1.5
/// here. `floor(m x dc)` can never exceed `m x dc`; `ceil(m x bytes / 100k)`
/// can.
#[sqlx::test]
async fn bytes_convert_to_dc_before_the_multiplier_is_applied(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let now = Utc::now();

    grant(&pool, &gw, dec!(1.5), now - Duration::hours(1)).await?;
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 100_000, "a", now)],
        now,
    )
    .await?;

    assert_eq!(
        pending_dc(&pool).await?,
        1,
        "1 DC at 1.5x floors to 1; multiplying the bytes first would charge 2"
    );

    Ok(())
}

/// The session arrives and accumulates, and only then is the ticket processed --
/// a ticket that had already taken effect before the data moved.
///
/// Looking the multiplier up at burn time means it still applies. Stamping it on
/// arrival would have left this hotspot at 1x forever, because nothing knew
/// about the grant when its bytes landed.
#[sqlx::test]
async fn a_ticket_processed_late_still_prices_the_session_it_predates(
    pool: PgPool,
) -> anyhow::Result<()> {
    let gw = gateway(1);
    let session_ts = Utc::now() - Duration::minutes(10);

    // The session lands first, with no ticket on record for this hotspot.
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", session_ts)],
        session_ts,
    )
    .await?;
    assert_eq!(
        pending_dc(&pool).await?,
        2,
        "nothing known yet, so the session prices at 1"
    );

    // The ticket is processed afterwards, but took effect five minutes before
    // the data moved.
    grant(&pool, &gw, dec!(1.5), session_ts - Duration::minutes(5)).await?;

    // 200,000 bytes -> 2 DC -> x1.5 -> 3.
    assert_eq!(
        pending_dc(&pool).await?,
        3,
        "the burn must price at what was in force when the data moved"
    );

    Ok(())
}

/// The other half of that rule. A grant that took effect after the data moved
/// does not apply to it.
#[sqlx::test]
async fn a_ticket_does_not_reach_back_over_older_sessions(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let session_ts = Utc::now() - Duration::hours(2);

    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", session_ts)],
        session_ts,
    )
    .await?;

    // Effective an hour *after* the data moved.
    grant(&pool, &gw, dec!(5), session_ts + Duration::hours(1)).await?;

    assert_eq!(
        pending_dc(&pool).await?,
        2,
        "a later grant must not be applied to data that predates it"
    );

    Ok(())
}

/// With several grants on record, a session is priced by the newest one that had
/// taken effect by the time the data moved — not the newest overall.
#[sqlx::test]
async fn the_newest_grant_in_force_wins(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let session_ts = Utc::now() - Duration::hours(1);

    grant(&pool, &gw, dec!(5), session_ts - Duration::hours(2)).await?;
    grant(&pool, &gw, dec!(2), session_ts - Duration::minutes(30)).await?;
    // Superseded by nothing yet: this one is in the future relative to the data.
    grant(&pool, &gw, dec!(3), session_ts + Duration::hours(1)).await?;

    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", session_ts)],
        session_ts,
    )
    .await?;

    // 2 DC at x2 = 4. Not x5 (superseded) and not x3 (not yet in force).
    assert_eq!(pending_dc(&pool).await?, 4);

    Ok(())
}

/// A reset to 1 is an ordinary grant and takes effect the same way. This is the
/// path the re-assertion cron drives.
#[sqlx::test]
async fn a_reset_to_one_takes_effect_like_any_other_grant(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let session_ts = Utc::now() - Duration::hours(1);

    grant(&pool, &gw, dec!(5), session_ts - Duration::hours(2)).await?;
    grant(&pool, &gw, dec!(1), session_ts - Duration::minutes(10)).await?;

    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", session_ts)],
        session_ts,
    )
    .await?;

    assert_eq!(pending_dc(&pool).await?, 2, "back to unmultiplied");

    Ok(())
}

/// A multiplier attaches to one hotspot. Its neighbour on the same payer is
/// unaffected, and both are billed to the same payer.
#[sqlx::test]
async fn a_multiplier_does_not_leak_between_hotspots(pool: PgPool) -> anyhow::Result<()> {
    let multiplied = gateway(1);
    let plain = gateway(2);
    let now = Utc::now();

    grant(&pool, &multiplied, dec!(5), now - Duration::hours(1)).await?;
    accumulate(
        &pool,
        &[multiplied.clone(), plain.clone()],
        vec![
            report(&multiplied, 200_000, "a", now),
            report(&plain, 200_000, "b", now),
        ],
        now,
    )
    .await?;

    // 2 DC x5 = 10, plus 2 DC x1 = 2.
    assert_eq!(pending_dc(&pool).await?, 12);

    Ok(())
}

/// Bytes billed at the same rate convert **once**, however many files they
/// arrived in.
///
/// `bytes_to_dc` charges a full DC for anything under 100KB and rounds up above
/// it, and you pay that once per conversion. Three files of 100,000 bytes summed
/// are 300,000, so 3 DC, and 3 x 1.5 floors to 4. Converted a row at a time they
/// would be 1 DC each, and `floor(1 x 1.5)` is 1, so three rows would charge 3.
///
/// Pick numbers that tell those apart. An earlier version of this test used
/// 40,000-byte files, which give 3 either way and so proved nothing.
#[sqlx::test]
async fn bytes_from_several_files_convert_together(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let start = Utc::now() - Duration::hours(1);

    grant(&pool, &gw, dec!(1.5), start - Duration::hours(1)).await?;
    for (i, event) in ["a", "b", "c"].iter().enumerate() {
        accumulate(
            &pool,
            std::slice::from_ref(&gw),
            vec![report(&gw, 100_000, event, start)],
            start + Duration::minutes(i as i64 * 10),
        )
        .await?;
    }

    let rows = pending_burns::get_all(&pool).await?;
    assert_eq!(rows.len(), 3, "one row per file");

    assert_eq!(
        pending_dc(&pool).await?,
        4,
        "300,000 bytes -> 3 DC -> x1.5 -> 4; converting per row would charge 3"
    );

    Ok(())
}

/// The same grouping rule with no multiplier in play, where it costs the payer
/// rather than the deployer.
///
/// Three files of 40,000 bytes are 120,000 summed, which is 2 DC. Each one alone
/// is under the 100KB floor and so costs a full DC, making 3. This is not a
/// HIP-150 behaviour -- it is why sessions were accumulated before any of this
/// existed -- but per-file rows are new, so it needs holding down.
#[sqlx::test]
async fn small_files_do_not_each_pay_the_one_dc_floor(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let start = Utc::now() - Duration::hours(1);

    for (i, event) in ["a", "b", "c"].iter().enumerate() {
        accumulate(
            &pool,
            std::slice::from_ref(&gw),
            vec![report(&gw, 40_000, event, start)],
            start + Duration::minutes(i as i64 * 10),
        )
        .await?;
    }

    assert_eq!(pending_burns::get_all(&pool).await?.len(), 3, "three files");
    assert_eq!(
        pending_dc(&pool).await?,
        bytes_to_dc(120_000),
        "summed and converted once"
    );
    assert_eq!(
        pending_dc(&pool).await?,
        2,
        "3 DC would mean each file paid the floor on its own"
    );

    Ok(())
}

/// A ticket takes effect at 1:30. The file before it bills at the old rate, the
/// file after it at the new one.
///
/// The ticket is not processed until after both files have landed, so the split
/// has to be worked out at burn time from the history.
#[sqlx::test]
async fn a_ticket_mid_window_splits_the_burn_at_the_boundary(pool: PgPool) -> anyhow::Result<()> {
    let gw = gateway(1);
    let one_thirty = Utc::now() - Duration::minutes(30);

    // 1:15 — before the ticket.
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(
            &gw,
            600_000,
            "before",
            one_thirty - Duration::minutes(15),
        )],
        one_thirty - Duration::minutes(15),
    )
    .await?;

    // 1:45 — after it.
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(
            &gw,
            500_000,
            "after",
            one_thirty + Duration::minutes(15),
        )],
        one_thirty + Duration::minutes(15),
    )
    .await?;

    // Only now is the ticket processed, taking effect at 1:30.
    grant(&pool, &gw, dec!(1.5), one_thirty).await?;

    // 600,000 -> 6 DC at 1x   -> 6
    // 500,000 -> 5 DC at 1.5x -> 7 (7.5 floored)
    assert_eq!(pending_dc(&pool).await?, 13);

    // Two groups, billed at two rates, from one hotspot in one burn window.
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns.len(), 1, "one payer");
    let mut rates: Vec<_> = burns[0].sessions().iter().map(|s| s.multiplier()).collect();
    rates.sort();
    assert_eq!(rates, vec![dec!(1), dec!(1.5)]);

    Ok(())
}

/// The same boundary, checked through the burned records rather than the total.
///
/// A total can come out right while the split is wrong, so this asserts the
/// bytes on each side separately.
#[sqlx::test]
async fn each_side_of_the_boundary_carries_its_own_bytes(pool: PgPool) -> anyhow::Result<()> {
    use mobile_packet_verifier::{burner::Burner, iceberg::burned_session};

    let harness = common::setup_iceberg().await?;
    let burn_writer = harness.get_table_writer(burned_session::TABLE_NAME).await?;

    let gw = gateway(1);
    let boundary = Utc::now() - Duration::minutes(30);

    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(
            &gw,
            600_000,
            "before",
            boundary - Duration::minutes(15),
        )],
        boundary - Duration::minutes(15),
    )
    .await?;
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(
            &gw,
            500_000,
            "after",
            boundary + Duration::minutes(15),
        )],
        boundary + Duration::minutes(15),
    )
    .await?;
    grant(&pool, &gw, dec!(1.5), boundary).await?;

    let solana = solana::burn::TestSolanaClientMap::default();
    solana.insert(&payer(), 1_000_000).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    Burner::new(
        FileSinkClient::new(tx, "test"),
        solana,
        0,
        std::time::Duration::default(),
        Some(burn_writer),
    )
    .burn(&pool)
    .await?;

    let mut burned = burned_session::get_all(harness.trino()).await?;
    burned.sort_by_key(|b| b.rewardable_bytes);
    assert_eq!(burned.len(), 2, "one record per rate billed");

    // 500,000 bytes at 1.5x -> 5 DC -> 7.
    assert_eq!(burned[0].rewardable_bytes, 500_000);
    assert_eq!(burned[0].num_dcs, 7);
    // 600,000 bytes at 1x -> 6 DC -> 6.
    assert_eq!(burned[1].rewardable_bytes, 600_000);
    assert_eq!(burned[1].num_dcs, 6);

    Ok(())
}

/// The burned record has to say which multiplier produced its `num_dcs`.
///
/// `num_dcs` is the figure after the multiplier: what the payer was charged, and
/// what the reward path divides up. Without this column there is no way back to
/// the pre-multiplier count, and the burn cannot be audited.
///
/// This covers the synchronous path, where the record is written from the groups
/// still in memory. See `a_frozen_multiplier_survives_the_pending_round_trip`
/// for the other one.
#[sqlx::test]
async fn the_burned_record_carries_the_multiplier(pool: PgPool) -> anyhow::Result<()> {
    use mobile_packet_verifier::{burner::Burner, iceberg::burned_session};

    let harness = common::setup_iceberg().await?;
    let burn_writer = harness.get_table_writer(burned_session::TABLE_NAME).await?;

    let gw = gateway(1);
    let now = Utc::now();

    grant(&pool, &gw, dec!(1.5), now - Duration::hours(1)).await?;
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", now)],
        now,
    )
    .await?;

    let solana = solana::burn::TestSolanaClientMap::default();
    solana.insert(&payer(), 1_000_000).await;

    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let burner = Burner::new(
        FileSinkClient::new(tx, "test"),
        solana,
        0,
        std::time::Duration::default(),
        Some(burn_writer),
    );
    burner.burn(&pool).await?;

    let burned = burned_session::get_all(harness.trino()).await?;
    assert_eq!(burned.len(), 1);

    // 200,000 bytes -> 2 DC -> x1.5 -> 3, and the record says how.
    assert_eq!(burned[0].num_dcs, 3);
    // Compared numerically, not as a string: a decimal(9,6) column returns
    // everything scale-padded, so 1.5 comes back as "1.500000".
    let stored: rust_decimal::Decimal = burned[0]
        .multiplier
        .as_ref()
        .expect("the burned record must state the multiplier it was priced at")
        .as_string()
        .parse()?;
    assert_eq!(stored, dec!(1.5));

    Ok(())
}

/// A burn that is not confirmed straight away leaves its rows in
/// `pending_data_transfer_sessions`, and the records are written later from
/// those rows, grouped on the multiplier stored with them.
///
/// Nothing else covers that path with a multiplier other than 1. The synchronous
/// burn writes from groups still in memory, so it would pass even if the stored
/// multiplier were dropped on the way in.
#[sqlx::test]
async fn a_frozen_multiplier_survives_the_pending_round_trip(pool: PgPool) -> anyhow::Result<()> {
    use mobile_packet_verifier::{burner::Burner, iceberg::burned_session, pending_txns};
    use solana::Signature;

    let harness = common::setup_iceberg().await?;
    let burn_writer = harness.get_table_writer(burned_session::TABLE_NAME).await?;

    let gw = gateway(1);
    let now = Utc::now();

    grant(&pool, &gw, dec!(1.5), now - Duration::hours(1)).await?;
    accumulate(
        &pool,
        std::slice::from_ref(&gw),
        vec![report(&gw, 200_000, "a", now)],
        now,
    )
    .await?;

    // Price it, then park it as an in-flight burn — the rows carry their
    // multiplier in, the way the burner does it.
    let burns = pending_burns::get_all_payer_burns(&pool).await?;
    assert_eq!(burns[0].total_dcs, 3, "2 DC at 1.5x");

    let signature = Signature::new_unique();
    pending_txns::do_add_pending_txn(
        &pool,
        &payer(),
        burns[0].total_dcs,
        &signature,
        &burns[0].rows,
        // Backdated so the confirm path does not sleep waiting for finality.
        Utc::now() - Duration::minutes(2),
    )
    .await?;

    let solana = solana::burn::TestSolanaClientMap::default();
    solana.insert(&payer(), 1_000_000).await;
    solana.add_confirmed(signature).await;

    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    Burner::new(
        FileSinkClient::new(tx, "test"),
        solana,
        0,
        std::time::Duration::default(),
        Some(burn_writer),
    )
    .confirm_pending_txns(&pool)
    .await?;

    let burned = burned_session::get_all(harness.trino()).await?;
    assert_eq!(burned.len(), 1);
    // 3, not 2: the frozen 1.5 was used. Dropping it on the way into the
    // pending table would leave this at 2.
    assert_eq!(burned[0].num_dcs, 3);

    let stored: rust_decimal::Decimal = burned[0]
        .multiplier
        .as_ref()
        .expect("the record must state the multiplier it was priced at")
        .as_string()
        .parse()?;
    assert_eq!(stored, dec!(1.5));

    Ok(())
}

/// Which grant is in force, tested through the burn's own read path.
///
/// These were inventory tests before the inventory was dropped. The guarantees
/// are the same; the table backing them is Postgres now.
///
/// A ticket is a signed message that never expires, so whoever captures one can
/// send it again later. Getting the ordering wrong is how a replayed ticket
/// restores a multiplier that was taken away.
mod which_grant_wins {
    use super::*;

    /// Price 200,000 bytes for `gw` right now, which is 2 DC before any
    /// multiplier, so the answer is 2 x whatever grant applies.
    ///
    /// `probe` has to differ between calls: repeated event ids are rejected as
    /// duplicates. The sessions are cleared afterwards so each probe stands
    /// alone rather than adding to the last one.
    async fn dc_for(pool: &PgPool, gw: &PublicKeyBinary, probe: &str) -> anyhow::Result<u64> {
        accumulate(
            pool,
            std::slice::from_ref(gw),
            vec![report(gw, 200_000, probe, Utc::now())],
            Utc::now(),
        )
        .await?;
        let dc = pending_dc(pool).await?;
        pending_burns::delete_for_payer(pool, &payer()).await?;
        Ok(dc)
    }

    /// Grants are ordered by when the issuer signed them, so a ticket that took
    /// a long time to arrive cannot override one issued after it.
    #[sqlx::test]
    async fn a_delayed_grant_does_not_supersede_a_newer_one(pool: PgPool) -> anyhow::Result<()> {
        let gw = gateway(1);
        let now = Utc::now();

        // Issued second, recorded first.
        grant(&pool, &gw, dec!(1), now - Duration::hours(1)).await?;
        // Issued first, recorded second, and must lose.
        grant(&pool, &gw, dec!(5), now - Duration::hours(3)).await?;

        assert_eq!(
            dc_for(&pool, &gw, "a").await?,
            2,
            "the newer grant still wins"
        );

        Ok(())
    }

    /// Replaying a captured ticket writes the same row again rather than a newer
    /// one, so it cannot bring back a multiplier that was superseded.
    #[sqlx::test]
    async fn replaying_a_superseded_grant_does_not_restore_it(pool: PgPool) -> anyhow::Result<()> {
        let gw = gateway(1);
        let now = Utc::now();
        let issued = now - Duration::hours(3);

        grant(&pool, &gw, dec!(5), issued).await?;
        grant(&pool, &gw, dec!(1), now - Duration::hours(1)).await?;
        assert_eq!(dc_for(&pool, &gw, "before").await?, 2, "revoked");

        // The attacker resends the original, byte for byte.
        grant(&pool, &gw, dec!(5), issued).await?;

        assert_eq!(
            dc_for(&pool, &gw, "after").await?,
            2,
            "a replay must not restore the old multiplier"
        );

        Ok(())
    }

    /// `1.50` and `1.5` are the same number, so they are the same grant rather
    /// than two rows competing.
    #[sqlx::test]
    async fn equivalent_spellings_are_one_grant(pool: PgPool) -> anyhow::Result<()> {
        let gw = gateway(1);
        let issued = Utc::now() - Duration::hours(1);

        grant(&pool, &gw, dec!(1.5), issued).await?;
        grant(&pool, &gw, dec!(1.50), issued).await?;

        // 2 DC x1.5 = 3. Two rows racing would still give 3, so also check
        // there is only one.
        assert_eq!(dc_for(&pool, &gw, "a").await?, 3);

        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM data_transfer_multipliers WHERE hotspot_pubkey = $1",
        )
        .bind(gw.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(rows, 1, "same value, same instant, one row");

        Ok(())
    }

    /// Values survive Postgres `NUMERIC` unchanged, including one that a float
    /// would mangle. The multiplier decides what a payer is charged, so an
    /// approximation here is a wrong bill.
    #[sqlx::test]
    async fn grants_round_trip_exactly(pool: PgPool) -> anyhow::Result<()> {
        let issued = Utc::now() - Duration::hours(1);
        let values = [dec!(1), dec!(1.3), dec!(1.5), dec!(2.25), dec!(5)];

        for (i, value) in values.iter().enumerate() {
            grant(&pool, &gateway(i as u8 + 1), *value, issued).await?;
        }

        let mut stored: Vec<rust_decimal::Decimal> = sqlx::query_scalar(
            "SELECT multiplier FROM data_transfer_multipliers ORDER BY multiplier",
        )
        .fetch_all(&pool)
        .await?;
        stored.sort();

        assert_eq!(stored, values.to_vec());

        Ok(())
    }
}
