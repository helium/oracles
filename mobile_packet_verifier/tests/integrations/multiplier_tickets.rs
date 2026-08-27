//! HIP-150 data transfer multiplier tickets.
//!
//! Two things are tested here, and the first is the one that matters.
//!
//! **Which ticket is in force.** The history table is append-only and holds
//! every ticket a hotspot has ever been issued, so "the multiplier" is whichever
//! row wins an ordering — decided by the merge that builds the inventory. A
//! ticket is a correctly signed message that never expires, so getting that
//! ordering wrong is not cosmetic: it is a way to restore a revoked multiplier
//! by replaying a captured message.
//!
//! These go through the whole path — seed the history, merge, read the
//! inventory — because that is what the burn will do. There is deliberately no
//! test that a ticket applies only to data that post-dates it: multipliers take
//! effect from the next refresh, not from the instant the data moved.
//!
//! **Whether a ticket counts at all** — the `verdict` module at the bottom.

use chrono::{DateTime, Duration, Utc};
use file_store_oracles::mobile::data_transfer_multiplier::{
    DataTransferMultiplier, DataTransferMultiplierTicket, DataTransferMultiplierTicketReport,
    VerifiedDataTransferMultiplierTicketReport,
    VerifiedDataTransferMultiplierTicketStatus as Status,
};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::data_transfer::multiplier_ticket_history::{
    IcebergMultiplierTicket, NAMESPACE, TABLE_NAME,
};
use mobile_packet_verifier::multiplier::{
    inventory::InventoryRefresher, trino::get_multipliers_from, Multipliers,
};
use rust_decimal::dec;

use crate::common;

/// Two-part `schema.table` name, resolved against the per-test catalog the
/// harness registers.
const HISTORY_TABLE: &str = "data_transfer.multiplier_ticket_history";
const INVENTORY_TABLE: &str = "data_transfer.multiplier_ticket_inventory";

fn hotspot(byte: u8) -> PublicKeyBinary {
    PublicKeyBinary::from(vec![byte])
}

fn multiplier(value: rust_decimal::Decimal) -> DataTransferMultiplier {
    DataTransferMultiplier::new(value).expect("valid multiplier")
}

/// A ticket carrying whatever multiplier is given, valid or not — the range is
/// judged by `ticket_status`, not by construction.
fn ticket_with(
    hotspot_pubkey: &PublicKeyBinary,
    value: Option<rust_decimal::Decimal>,
    signed: DateTime<Utc>,
    received: DateTime<Utc>,
) -> DataTransferMultiplierTicketReport {
    let mut report = ticket(hotspot_pubkey, dec!(1), signed, received);
    report.report.multiplier = value;
    report
}

/// A ticket signed at `signed` and received at `received`.
fn ticket(
    hotspot_pubkey: &PublicKeyBinary,
    value: rust_decimal::Decimal,
    signed: DateTime<Utc>,
    received: DateTime<Utc>,
) -> DataTransferMultiplierTicketReport {
    DataTransferMultiplierTicketReport {
        received_timestamp: received,
        report: DataTransferMultiplierTicket {
            hotspot_pubkey: hotspot_pubkey.clone(),
            multiplier: Some(value),
            timestamp: signed,
            message: "test ticket".to_string(),
            signer_pubkey: hotspot(200),
            signature: vec![],
        },
    }
}

/// A history row as the ingestor would have written it.
fn history_row(
    report: DataTransferMultiplierTicketReport,
    status: Status,
) -> IcebergMultiplierTicket {
    let verified = VerifiedDataTransferMultiplierTicketReport {
        verified_timestamp: report.received_timestamp,
        report,
        status,
    };
    IcebergMultiplierTicket::from(&verified)
}

fn valid_row(report: DataTransferMultiplierTicketReport) -> IcebergMultiplierTicket {
    history_row(report, Status::Valid)
}

async fn seed(
    harness: &IcebergTestHarness,
    rows: Vec<IcebergMultiplierTicket>,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    harness
        .get_table_writer_in::<IcebergMultiplierTicket>(NAMESPACE, TABLE_NAME)
        .await?
        .write(rows)
        .await?;
    Ok(())
}

/// Seed the history, run the merge, and read what the burn would see.
///
/// The whole path: a ticket lands in the history, the inventory is merged out of
/// it, and the burn reads the inventory. Ordering and refusal rules live in the
/// merge, so exercising them through this helper tests them where they run.
async fn multipliers_after_refresh(
    rows: Vec<IcebergMultiplierTicket>,
) -> anyhow::Result<Multipliers> {
    let harness = common::setup_iceberg().await?;
    seed(&harness, rows).await?;
    let trino = trino_client::Client::from_client(harness.owned_trino().await?);

    InventoryRefresher::new_with_tables(
        trino.clone(),
        std::time::Duration::from_secs(900),
        HISTORY_TABLE.to_string(),
        INVENTORY_TABLE.to_string(),
    )
    .refresh()
    .await?;

    get_multipliers_from(&trino, INVENTORY_TABLE).await
}

#[tokio::test]
async fn no_tickets_means_every_hotspot_is_unmultiplied() -> anyhow::Result<()> {
    let multipliers = multipliers_after_refresh(vec![]).await?;

    assert!(multipliers.is_empty());
    assert_eq!(
        multipliers.get(&hotspot(1)),
        DataTransferMultiplier::DEFAULT,
        "a hotspot with no ticket must be unmultiplied"
    );

    Ok(())
}

#[tokio::test]
async fn latest_ticket_by_issue_time_wins() -> anyhow::Result<()> {
    let hotspot = hotspot(1);
    let now = Utc::now();

    let multipliers = multipliers_after_refresh(vec![
        valid_row(ticket(
            &hotspot,
            dec!(5),
            now - Duration::hours(2),
            now - Duration::hours(2),
        )),
        valid_row(ticket(
            &hotspot,
            dec!(1.5),
            now - Duration::hours(1),
            now - Duration::hours(1),
        )),
    ])
    .await?;

    assert_eq!(multipliers.get(&hotspot), multiplier(dec!(1.5)));

    Ok(())
}

/// **The replay test.** A ticket granting 5x is superseded by one granting 1x.
/// An attacker resubmits the original 5x ticket — a genuine, correctly signed
/// message — hours after it was revoked.
///
/// With no database there is no primary key to reject the duplicate, so the
/// replay *does* become a row; the history honestly records that a resubmission
/// happened. It changes nothing because the ordering is on the issuer's signed
/// timestamp, which a replay cannot alter without the signing key. Ordering on
/// arrival would hand the attacker the 5x back.
#[tokio::test]
async fn replaying_a_superseded_ticket_does_not_restore_it() -> anyhow::Result<()> {
    let hotspot = hotspot(1);
    let now = Utc::now();

    let granted_at = now - Duration::hours(3);
    let revoked_at = now - Duration::hours(2);

    let granted = valid_row(ticket(&hotspot, dec!(5), granted_at, granted_at));
    let revoked = valid_row(ticket(&hotspot, dec!(1), revoked_at, revoked_at));
    // The captured 5x ticket, resubmitted now. Same signed timestamp — that is
    // what makes it a replay rather than a new grant — but it arrives after the
    // revocation.
    let replayed = valid_row(ticket(&hotspot, dec!(5), granted_at, now));

    // Control: before the revocation the grant really was in force. Without
    // this the assertion below would pass just as well if the query returned
    // nothing at all, since "no ticket" and "revoked to 1" are both the default.
    assert_eq!(
        multipliers_after_refresh(vec![granted.clone()])
            .await?
            .get(&hotspot),
        multiplier(dec!(5)),
        "the original grant should have been in force before revocation"
    );

    let multipliers = multipliers_after_refresh(vec![granted, revoked, replayed]).await?;

    assert_eq!(
        multipliers.get(&hotspot),
        DataTransferMultiplier::DEFAULT,
        "a replayed ticket must not restore a revoked multiplier"
    );

    Ok(())
}

/// A ticket held up in delivery must not leapfrog a newer one that overtook it.
/// Ordering by arrival gets this wrong with no attacker involved at all.
#[tokio::test]
async fn a_delayed_ticket_does_not_supersede_a_newer_one() -> anyhow::Result<()> {
    let hotspot = hotspot(1);
    let now = Utc::now();

    let multipliers = multipliers_after_refresh(vec![
        // Signed second, arrived first.
        valid_row(ticket(
            &hotspot,
            dec!(1.5),
            now - Duration::hours(1),
            now - Duration::minutes(30),
        )),
        // Signed first, arrived second.
        valid_row(ticket(
            &hotspot,
            dec!(5),
            now - Duration::hours(2),
            now - Duration::minutes(10),
        )),
    ])
    .await?;

    assert_eq!(
        multipliers.get(&hotspot),
        multiplier(dec!(1.5)),
        "the more recently *issued* ticket wins, not the more recently received"
    );

    Ok(())
}

/// The history table keeps refused tickets, so the read has to exclude them.
/// Without the status filter a rejected grant would take effect.
#[tokio::test]
async fn rejected_tickets_do_not_take_effect() -> anyhow::Result<()> {
    let hotspot = hotspot(1);
    let now = Utc::now();

    let multipliers = multipliers_after_refresh(vec![
        valid_row(ticket(
            &hotspot,
            dec!(1.5),
            now - Duration::hours(2),
            now - Duration::hours(2),
        )),
        // Newer, larger, and refused — it must not win despite sorting first.
        history_row(
            ticket(
                &hotspot,
                dec!(5),
                now - Duration::hours(1),
                now - Duration::hours(1),
            ),
            Status::InvalidSigner,
        ),
    ])
    .await?;

    assert_eq!(
        multipliers.get(&hotspot),
        multiplier(dec!(1.5)),
        "a refused ticket must not take effect"
    );

    Ok(())
}

#[tokio::test]
async fn hotspots_do_not_affect_each_other() -> anyhow::Result<()> {
    let now = Utc::now();
    let (a, b, c) = (hotspot(1), hotspot(2), hotspot(3));

    let multipliers = multipliers_after_refresh(vec![
        valid_row(ticket(&a, dec!(1.5), now, now)),
        valid_row(ticket(&b, dec!(5), now, now)),
    ])
    .await?;

    assert_eq!(multipliers.len(), 2, "only ticketed hotspots appear");
    assert_eq!(multipliers.get(&a), multiplier(dec!(1.5)));
    assert_eq!(multipliers.get(&b), multiplier(dec!(5)));
    assert_eq!(multipliers.get(&c), DataTransferMultiplier::DEFAULT);

    Ok(())
}

/// The exact decimal must survive the round trip through `decimal(9,6)`. 1.3 is
/// the value that would come back mangled from a float column.
#[tokio::test]
async fn multipliers_round_trip_exactly() -> anyhow::Result<()> {
    let now = Utc::now();
    let values = [dec!(1), dec!(1.5), dec!(1.3), dec!(2.718281), dec!(5)];

    let rows = values
        .iter()
        .enumerate()
        .map(|(i, value)| valid_row(ticket(&hotspot(i as u8 + 1), *value, now, now)))
        .collect();

    let multipliers = multipliers_after_refresh(rows).await?;

    for (i, value) in values.iter().enumerate() {
        assert_eq!(
            multipliers.get(&hotspot(i as u8 + 1)),
            multiplier(*value),
            "{value} did not survive storage"
        );
    }

    Ok(())
}

/// `1.5` and `1.50` are the same multiplier. Normalizing on parse is what keeps
/// a difference in spelling from becoming a difference in value — including
/// through the `decimal(9,6)` column, which returns everything scale-padded.
#[tokio::test]
async fn equivalent_spellings_are_one_multiplier() -> anyhow::Result<()> {
    let hotspot = hotspot(1);
    let now = Utc::now();

    let multipliers =
        multipliers_after_refresh(vec![valid_row(ticket(&hotspot, dec!(1.50), now, now))]).await?;

    assert_eq!(multipliers.get(&hotspot), multiplier(dec!(1.5)));

    Ok(())
}

// ── The inventory table ─────────────────────────────────────────────────────
//
// The history is the log; the inventory is what is currently in force. A
// periodic MERGE builds the second from the first.

mod inventory {
    use super::*;
    use helium_iceberg_oracles::data_transfer::multiplier_ticket_inventory::IcebergMultiplierInventory;
    use mobile_packet_verifier::multiplier::inventory::InventoryRefresher;
    use std::time::Duration as StdDuration;

    /// Compare by value, not by formatting. Trino returns a `decimal(9,6)`
    /// scale-padded — 1.5 comes back as "1.500000" — which says nothing about
    /// whether the stored value is right.
    fn stored(row: &IcebergMultiplierInventory) -> rust_decimal::Decimal {
        row.multiplier
            .as_string()
            .parse()
            .expect("stored multiplier parses")
    }

    /// Seed history, run the merge, and read the inventory back.
    async fn refresh_and_read(
        rows: Vec<IcebergMultiplierTicket>,
    ) -> anyhow::Result<Vec<IcebergMultiplierInventory>> {
        let harness = common::setup_iceberg().await?;
        seed(&harness, rows).await?;
        let trino = trino_client::Client::from_client(harness.owned_trino().await?);

        InventoryRefresher::new_with_tables(
            trino.clone(),
            StdDuration::from_secs(900),
            HISTORY_TABLE.to_string(),
            INVENTORY_TABLE.to_string(),
        )
        .refresh()
        .await?;

        let mut rows: Vec<IcebergMultiplierInventory> = trino
            .get_all_raw(format!("SELECT * FROM {INVENTORY_TABLE}"))
            .await?;
        rows.sort_by(|a, b| a.hotspot_pubkey.cmp(&b.hotspot_pubkey));
        Ok(rows)
    }

    #[tokio::test]
    async fn merges_the_latest_valid_ticket_per_hotspot() -> anyhow::Result<()> {
        let now = Utc::now();
        let (a, b) = (hotspot(1), hotspot(2));

        let rows = refresh_and_read(vec![
            valid_row(ticket(
                &a,
                dec!(5),
                now - Duration::hours(2),
                now - Duration::hours(2),
            )),
            valid_row(ticket(
                &a,
                dec!(1.5),
                now - Duration::hours(1),
                now - Duration::hours(1),
            )),
            valid_row(ticket(
                &b,
                dec!(2),
                now - Duration::hours(1),
                now - Duration::hours(1),
            )),
        ])
        .await?;

        assert_eq!(rows.len(), 2, "one row per hotspot, not one per ticket");
        assert_eq!(stored(&rows[0]), dec!(1.5), "superseded 5 must not win");
        assert_eq!(stored(&rows[1]), dec!(2));

        Ok(())
    }

    /// The inventory holds what is in force, so refusals are excluded. A refusal
    /// also must not revoke an earlier grant.
    #[tokio::test]
    async fn refused_tickets_are_excluded() -> anyhow::Result<()> {
        let hotspot = hotspot(1);
        let now = Utc::now();

        let rows = refresh_and_read(vec![
            valid_row(ticket(
                &hotspot,
                dec!(1.5),
                now - Duration::hours(2),
                now - Duration::hours(2),
            )),
            // Newer, larger, refused.
            history_row(
                ticket(
                    &hotspot,
                    dec!(5),
                    now - Duration::hours(1),
                    now - Duration::hours(1),
                ),
                Status::InvalidSigner,
            ),
        ])
        .await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(
            stored(&rows[0]),
            dec!(1.5),
            "a refusal must not take effect, nor revoke the last valid grant"
        );

        Ok(())
    }

    /// **The merge test.** Running twice must update in place, not append a
    /// second row per hotspot — that is the whole difference between a merge and
    /// the append-only writer.
    #[tokio::test]
    async fn refreshing_twice_updates_in_place() -> anyhow::Result<()> {
        let hotspot = hotspot(1);
        let now = Utc::now();

        let harness = common::setup_iceberg().await?;
        seed(
            &harness,
            vec![valid_row(ticket(
                &hotspot,
                dec!(1.5),
                now - Duration::hours(2),
                now - Duration::hours(2),
            ))],
        )
        .await?;
        let trino = trino_client::Client::from_client(harness.owned_trino().await?);
        let refresher = InventoryRefresher::new_with_tables(
            trino.clone(),
            StdDuration::from_secs(900),
            HISTORY_TABLE.to_string(),
            INVENTORY_TABLE.to_string(),
        );

        refresher.refresh().await?;

        // A newer grant arrives, then we refresh again.
        seed(
            &harness,
            vec![valid_row(ticket(
                &hotspot,
                dec!(5),
                now - Duration::hours(1),
                now - Duration::hours(1),
            ))],
        )
        .await?;
        refresher.refresh().await?;

        let rows: Vec<IcebergMultiplierInventory> = trino
            .get_all_raw(format!("SELECT * FROM {INVENTORY_TABLE}"))
            .await?;

        assert_eq!(
            rows.len(),
            1,
            "the hotspot must have one row, not one per refresh"
        );
        assert_eq!(stored(&rows[0]), dec!(5), "the row must have been updated");

        Ok(())
    }

    #[tokio::test]
    async fn no_tickets_leaves_the_inventory_empty() -> anyhow::Result<()> {
        assert!(refresh_and_read(vec![]).await?.is_empty());
        Ok(())
    }
}

// ── The verdict rule ────────────────────────────────────────────────────────
//
// Storage above decides which ticket wins. These decide whether a ticket counts
// at all — the second line of defence behind ingest, exercised here because
// ingest and this verifier are configured separately and either could be wrong.

mod verdict {
    use super::*;
    use file_store_oracles::mobile::data_transfer_multiplier::MAX_CLOCK_DRIFT;
    use mobile_packet_verifier::multiplier::{ingestor::ticket_status, TicketSigners};
    use std::time::Duration as StdDuration;

    const MAX_AGE: StdDuration = StdDuration::from_secs(600);

    fn signer() -> PublicKeyBinary {
        hotspot(200)
    }

    async fn status_of(
        report: &DataTransferMultiplierTicketReport,
        signers: TicketSigners,
    ) -> anyhow::Result<Status> {
        let harness = common::setup_iceberg().await?;
        // Seed the ticket's hotspot as known on chain, well before the ticket.
        common::hotspot_inventory::seed(
            &harness,
            vec![common::hotspot_inventory::MobileHotspotInventory::known(
                &report.report.hotspot_pubkey,
                report.received_timestamp - Duration::days(1),
            )],
        )
        .await?;
        let resolver = common::gateway_resolver(&harness).await?;

        Ok(ticket_status(report, &signers, MAX_AGE, &resolver).await)
    }

    #[tokio::test]
    async fn accepts_a_fresh_ticket_from_a_known_signer() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket(&hotspot(1), dec!(1.5), now, now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::Valid);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_an_unauthorized_signer() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket(&hotspot(1), dec!(1.5), now, now);

        // A different key is authorized — the ticket's signer is not.
        let status = status_of(&report, TicketSigners::from_iter([hotspot(201)])).await?;
        assert_eq!(status, Status::InvalidSigner);

        Ok(())
    }

    /// The empty allow-list, which is how this ships. Fails closed.
    #[tokio::test]
    async fn rejects_every_ticket_when_no_signers_are_configured() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket(&hotspot(1), dec!(1.5), now, now);

        let status = status_of(&report, TicketSigners::default()).await?;
        assert_eq!(status, Status::InvalidSigner);

        Ok(())
    }

    /// Signed well before ingest received it — the shape a replayed ticket has.
    #[tokio::test]
    async fn rejects_a_stale_ticket() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket(&hotspot(1), dec!(1.5), now - Duration::hours(2), now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::InvalidTimestamp);

        Ok(())
    }

    /// Post-dating must not buy an arbitrarily long window.
    #[tokio::test]
    async fn rejects_a_future_dated_ticket() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket(&hotspot(1), dec!(1.5), now + Duration::hours(1), now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::InvalidTimestamp);

        Ok(())
    }

    /// Ingest tolerates a client whose clock runs slightly fast, so this must
    /// too. If it did not, a ticket ingest accepted would be refused here and
    /// the two services would disagree about the same ticket.
    #[tokio::test]
    async fn tolerates_the_same_clock_drift_ingest_does() -> anyhow::Result<()> {
        let now = Utc::now();
        // Signed ahead of the timestamp ingest stamped on it.
        let signed = now + Duration::from_std(MAX_CLOCK_DRIFT)? - Duration::seconds(5);
        let report = ticket(&hotspot(1), dec!(1.5), signed, now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::Valid);

        Ok(())
    }

    /// The far side of the allowance, so the test above cannot pass by the
    /// tolerance being unbounded.
    #[tokio::test]
    async fn rejects_drift_beyond_the_allowance() -> anyhow::Result<()> {
        let now = Utc::now();
        let signed = now + Duration::from_std(MAX_CLOCK_DRIFT)? + Duration::minutes(1);
        let report = ticket(&hotspot(1), dec!(1.5), signed, now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::InvalidTimestamp);

        Ok(())
    }

    /// Inside the window is still accepted — without this the rejection tests
    /// would pass just as well if the window were zero.
    #[tokio::test]
    async fn accepts_a_ticket_inside_the_window() -> anyhow::Result<()> {
        let now = Utc::now();
        let signed = now - Duration::from_std(MAX_AGE)? + Duration::minutes(1);
        let report = ticket(&hotspot(1), dec!(1.5), signed, now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::Valid);

        Ok(())
    }

    /// HIP-150 fixes the range at 1 to 5 inclusive, enforced by the oracles.
    /// Enforced *here* rather than at ingest so the refusal lands on the record
    /// instead of vanishing at the gRPC boundary.
    #[tokio::test]
    async fn rejects_a_multiplier_outside_the_hip_range() -> anyhow::Result<()> {
        let now = Utc::now();

        for value in [dec!(0), dec!(0.999999), dec!(5.000001), dec!(7), dec!(-1)] {
            let report = ticket_with(&hotspot(1), Some(value), now, now);
            let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
            assert_eq!(
                status,
                Status::InvalidMultiplier,
                "{value} is outside 1..=5 and must be refused"
            );
        }

        Ok(())
    }

    /// The bounds themselves are accepted, so the test above cannot pass by the
    /// range being empty.
    #[tokio::test]
    async fn accepts_the_bounds_of_the_hip_range() -> anyhow::Result<()> {
        let now = Utc::now();

        for value in [dec!(1), dec!(5), dec!(1.5), dec!(4.999999)] {
            let report = ticket_with(&hotspot(1), Some(value), now, now);
            let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
            assert_eq!(status, Status::Valid, "{value} is within 1..=5");
        }

        Ok(())
    }

    /// More precision than the column stores is refused too — same outcome to a
    /// submitter, and it keeps the stored value exact.
    #[tokio::test]
    async fn rejects_a_multiplier_with_too_much_precision() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket_with(&hotspot(1), Some(dec!(1.5000001)), now, now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::InvalidMultiplier);

        Ok(())
    }

    /// An absent or unparseable multiplier is a refusal on the record, not a
    /// record that never existed. The file poller silently drops anything that
    /// fails to decode, so the ticket has to survive decoding to be refused.
    #[tokio::test]
    async fn rejects_an_absent_multiplier() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket_with(&hotspot(1), None, now, now);

        let status = status_of(&report, TicketSigners::from_iter([signer()])).await?;
        assert_eq!(status, Status::InvalidMultiplier);

        Ok(())
    }

    /// A multiplier can only attach to an on-chain hotspot.
    #[tokio::test]
    async fn rejects_an_unknown_hotspot() -> anyhow::Result<()> {
        let now = Utc::now();
        let report = ticket(&hotspot(1), dec!(1.5), now, now);

        let harness = common::setup_iceberg().await?;
        // Nothing seeded: the hotspot is not on chain.
        let resolver = common::gateway_resolver(&harness).await?;

        let status = ticket_status(
            &report,
            &TicketSigners::from_iter([signer()]),
            MAX_AGE,
            &resolver,
        )
        .await;
        assert_eq!(status, Status::InvalidHotspotKey);

        Ok(())
    }
}
