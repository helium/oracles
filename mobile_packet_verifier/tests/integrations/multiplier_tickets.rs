//! HIP-150 data transfer multiplier tickets: whether a ticket counts, and what
//! it grants.
//!
//! `ticket_status` decides whether a ticket is valid. `granted_multiplier` turns
//! a verified ticket into a grant, or into nothing. Both are pure functions, so
//! they are tested directly rather than through a file poller.
//!
//! Which grant is in force for a given hotspot at a given time is a property of
//! the Postgres table and the burn's join, so those tests live in
//! `apply_multiplier.rs` where the real read path is.

use chrono::{DateTime, Duration, Utc};
use file_store_oracles::mobile::data_transfer_multiplier::{
    DataTransferMultiplier, DataTransferMultiplierTicket, DataTransferMultiplierTicketReport,
    VerifiedDataTransferMultiplierTicketReport,
    VerifiedDataTransferMultiplierTicketStatus as Status,
};
use helium_crypto::PublicKeyBinary;
use rust_decimal::dec;

use crate::common;

fn hotspot(byte: u8) -> PublicKeyBinary {
    PublicKeyBinary::from(vec![byte])
}

/// A ticket carrying whatever multiplier is given, valid or not. The range is
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

/// What the ingestor would hold after ruling on a ticket.
fn verified(
    report: DataTransferMultiplierTicketReport,
    status: Status,
) -> VerifiedDataTransferMultiplierTicketReport {
    VerifiedDataTransferMultiplierTicketReport {
        verified_timestamp: report.received_timestamp,
        report,
        status,
    }
}

/// What a verified ticket grants, and to whom.
///
/// This is the gate between "we ruled on a ticket" and "a hotspot's data credits
/// are worth more". Only valid tickets pass it.
mod grants {
    use super::*;
    use mobile_packet_verifier::multiplier::ingestor::granted_multiplier;

    #[test]
    fn a_valid_ticket_grants_its_multiplier() {
        let now = Utc::now();
        let signed = now - Duration::minutes(1);
        let grant = granted_multiplier(&verified(
            ticket(&hotspot(1), dec!(1.5), signed, now),
            Status::Valid,
        ))
        .expect("a valid ticket grants something");

        assert_eq!(grant.hotspot_pubkey, hotspot(1));
        assert_eq!(
            grant.multiplier,
            DataTransferMultiplier::new(dec!(1.5)).unwrap()
        );
        // The signed time, not the received one: the issuer says when the grant
        // starts, and the history is ordered on the same value.
        assert_eq!(grant.effective_timestamp, signed);
    }

    /// A refused ticket grants nothing, whatever it was refused for. It still
    /// gets a verified report and a history row; it just never reaches the burn.
    #[test]
    fn a_refused_ticket_grants_nothing() {
        let now = Utc::now();
        for status in [
            Status::InvalidSigner,
            Status::InvalidMultiplier,
            Status::InvalidHotspotKey,
            Status::InvalidTimestamp,
        ] {
            let report = ticket(&hotspot(1), dec!(5), now - Duration::minutes(1), now);
            assert!(
                granted_multiplier(&verified(report, status)).is_none(),
                "{} must not grant anything",
                status.as_str_name()
            );
        }
    }

    /// Belt and braces. `ticket_status` refuses an absent multiplier, so a
    /// ticket cannot be both valid and empty unless the two checks have drifted
    /// apart. If they ever do, grant nothing rather than guess.
    #[test]
    fn a_valid_ticket_with_no_multiplier_grants_nothing() {
        let now = Utc::now();
        let report = ticket_with(&hotspot(1), None, now - Duration::minutes(1), now);
        assert!(granted_multiplier(&verified(report, Status::Valid)).is_none());
    }
}

/// Whether a ticket counts at all.
///
/// `ticket_status` is the gate. Everything it refuses still gets a verified
/// report and a history row, so a refusal is as visible as a grant.
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
