//! HIP-150 data transfer multipliers, and the tickets that grant them.
//!
//! A multiplier applies to the data credits derived from a hotspot's rewardable
//! bytes, not to the bytes themselves — a rewardable-byte count is a
//! measurement and does not change. It raises what a payer burns for that
//! hotspot's data and what its deployer earns, in the same proportion.
//!
//! A hotspot with no ticket is at [`DataTransferMultiplier::DEFAULT`] (1), and
//! there is no ticket meaning "no multiplier": returning a hotspot to 1 means
//! issuing a ticket that grants exactly 1.
//!
//! # Parsing and validating are separate steps
//!
//! The wire type is `helium.Decimal`, a string. [`parse_multiplier`] turns it
//! into a plain `Decimal` without judging it, and that is what a decoded
//! [`DataTransferMultiplierTicket`] carries — deliberately, because the file
//! poller discards records that fail to decode, and a ticket has to survive
//! decoding in order to be refused on the record.
//!
//! Validation is [`DataTransferMultiplier::new`], applied by the packet
//! verifier when it rules on a ticket. Everything downstream of that point —
//! the burn, the reward record — holds a [`DataTransferMultiplier`], so "is
//! this multiplier in range" is answered by the type rather than re-checked at
//! each call site.

use chrono::{DateTime, Utc};
use file_store::traits::{MsgDecode, TimestampDecode, TimestampDecodeError, TimestampEncode};
use helium_crypto::PublicKeyBinary;
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};

use crate::prost_enum;

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        DataTransferMultiplierTicketIngestReportV1, DataTransferMultiplierTicketReqV1,
        VerifiedDataTransferMultiplierTicketReportV1, VerifiedDataTransferMultiplierTicketStatus,
    };
    pub use helium_proto::Decimal;
}

pub use proto::VerifiedDataTransferMultiplierTicketStatus;

/// The verdict as a short string, for the stored record and for metric labels.
///
/// prost's own `as_str_name()` gives the full proto enum name —
/// `verified_data_transfer_multiplier_ticket_status_valid` — which is 44
/// characters restating the column the value sits in. Trino has no enum type,
/// so this string *is* what a person queries against, and
/// `status = 'valid'` beats the alternative.
///
/// Same reasoning as `carrier_id_string` in `helium_iceberg_oracles`, which
/// already does this for `CarrierIdV2` for the same reason.
///
/// Spelled out rather than derived by stripping a prefix, so a new variant is a
/// compile error here rather than a surprise in the data. Nothing parses these
/// back into the enum; if that changes, this needs an inverse.
pub fn ticket_status_string(status: VerifiedDataTransferMultiplierTicketStatus) -> &'static str {
    use VerifiedDataTransferMultiplierTicketStatus as Status;

    match status {
        Status::Valid => "valid",
        Status::InvalidSigner => "invalid_signer",
        Status::InvalidMultiplier => "invalid_multiplier",
        Status::InvalidHotspotKey => "invalid_hotspot_key",
        Status::InvalidTimestamp => "invalid_timestamp",
    }
}

/// Smallest multiplier the oracles accept.
///
/// Operating policy, not wire format: HIP-150's 1-to-5 figures are starting
/// values, and the proto deliberately does not encode a range. Widening the
/// range later — a sub-1 multiplier, or a ceiling above 5 — is editing these
/// constants, with no schema change and no migration.
pub const MIN_MULTIPLIER: Decimal = Decimal::ONE;
/// Largest multiplier the oracles accept. See [`MIN_MULTIPLIER`].
pub const MAX_MULTIPLIER: Decimal = Decimal::from_parts(5, 0, 0, false, 0);
/// Most fractional digits a multiplier may carry.
///
/// Bounds how much precision reaches the stored record, whose column is
/// `decimal(9,6)`. Values are negotiated per venue, so this is generous rather
/// than tight.
pub const MAX_SCALE: u32 = 6;

/// How far ahead of the receiving oracle's clock a ticket may be stamped.
///
/// Clients do not share a clock with ingest, so a ticket signed at what the
/// client believes is "now" can arrive stamped slightly in the future. Without
/// some tolerance those are rejected as post-dated, which is a confusing failure
/// for an honest client with a drifting clock.
///
/// One minute is enough for ordinary NTP-less drift and short enough that it
/// buys an attacker nothing: a post-dated ticket still ages out of the freshness
/// window at the same rate, it just starts a minute earlier.
///
/// **Shared deliberately.** Ingest and the packet verifier both check freshness,
/// and the verifier measures a ticket's age against the timestamp *ingest*
/// stamped on it. If ingest tolerated drift the verifier did not, every ticket
/// ingest accepted from a fast client would then be refused downstream — so the
/// two must use one value, not two settings that can be configured apart.
pub const MAX_CLOCK_DRIFT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum MultiplierError {
    #[error("unparseable multiplier: {0}")]
    Unparseable(String),
    #[error("multiplier {0} out of range, must be between {MIN_MULTIPLIER} and {MAX_MULTIPLIER}")]
    OutOfRange(Decimal),
    #[error("multiplier {0} has more than {MAX_SCALE} fractional digits")]
    TooPrecise(Decimal),
    /// Applying the multiplier overflowed.
    ///
    /// Unreachable for any real burn — a multiplier is bounded by
    /// [`MAX_MULTIPLIER`], so this needs a data credit count within a few
    /// multiples of `u64::MAX`. It is an error rather than a saturating value
    /// because the only saturating value available is `u64::MAX`, and the
    /// number this produces is what a payer is charged.
    #[error("multiplier {multiplier} applied to {data_credits} data credits overflows")]
    Overflow {
        data_credits: u64,
        multiplier: Decimal,
    },
}

/// A validated HIP-150 data transfer multiplier.
///
/// The inner value is private and every constructor validates, so holding one
/// *is* the proof it is in range — callers never re-check.
///
/// Serializes as the underlying decimal, so a record carrying one reads as the
/// number rather than a wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct DataTransferMultiplier(Decimal);

impl DataTransferMultiplier {
    /// The multiplier every hotspot without a ticket is at.
    ///
    /// Where "no ticket means 1" is written down: a lookup that misses returns
    /// this, and an absent multiplier on a *burned session* resolves to it.
    ///
    /// An absent multiplier on a *ticket* is a different thing — a grant that
    /// grants nothing — and is refused rather than defaulted. See
    /// `mobile_packet_verifier::multiplier::ingestor::ticket_status`.
    pub const DEFAULT: Self = Self(Decimal::ONE);

    /// Validate and normalize a multiplier.
    ///
    /// Normalizing means `1.5`, `1.50` and `1.5e0` all become the same value, so
    /// a difference in spelling cannot become a difference in multiplier.
    pub fn new(value: Decimal) -> Result<Self, MultiplierError> {
        if value.scale() > MAX_SCALE {
            return Err(MultiplierError::TooPrecise(value));
        }
        if value < MIN_MULTIPLIER || value > MAX_MULTIPLIER {
            return Err(MultiplierError::OutOfRange(value));
        }
        Ok(Self(value.normalize()))
    }

    /// Data credits after the multiplier, rounded **down**.
    ///
    /// HIP-150: "Applied to a data credit count it is rounded down, so a payer
    /// never burns more than the multiplier earns." This is the only place the
    /// multiplication happens.
    ///
    /// # Why this returns an error
    ///
    /// Overflow is unreachable for any real burn: a multiplier is bounded by
    /// [`MAX_MULTIPLIER`], so reaching it needs a data credit count within a few
    /// multiples of `u64::MAX`. It is still an error rather than a fallback
    /// value, because the number returned here is what a payer is charged, and
    /// the only fallback `u64` offers is `u64::MAX` — burn everything. A default
    /// that fails towards charging the most possible is the wrong default for
    /// money, however unreachable. Stopping forces someone to look at why an
    /// impossible thing happened before any DC moves.
    pub fn apply(&self, data_credits: u64) -> Result<u64, MultiplierError> {
        Decimal::from(data_credits)
            .checked_mul(self.0)
            .and_then(|scaled| {
                scaled
                    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                    .to_u64()
            })
            .ok_or(MultiplierError::Overflow {
                data_credits,
                multiplier: self.0,
            })
    }

    /// True when this is the default, i.e. the hotspot is effectively unmultiplied.
    pub fn is_default(&self) -> bool {
        *self == Self::DEFAULT
    }

    pub fn as_decimal(&self) -> Decimal {
        self.0
    }
}

impl std::fmt::Display for DataTransferMultiplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<proto::Decimal> for DataTransferMultiplier {
    type Error = MultiplierError;

    fn try_from(value: proto::Decimal) -> Result<Self, Self::Error> {
        // The Decimal proto permits an exponent ("2.5e8") as well as plain
        // notation, and says services should normalize rather than reject.
        // `rust_decimal`'s `FromStr` handles only the plain form, so fall back
        // to the scientific parser rather than calling a legal value garbage.
        let parsed = value
            .value
            .parse::<Decimal>()
            .or_else(|_| Decimal::from_scientific(&value.value))
            .map_err(|_| MultiplierError::Unparseable(value.value))?;
        Self::new(parsed)
    }
}

impl From<DataTransferMultiplier> for proto::Decimal {
    fn from(value: DataTransferMultiplier) -> Self {
        Self {
            value: value.0.to_string(),
        }
    }
}

// ── Ticket reports ──────────────────────────────────────────────────────────

#[derive(thiserror::Error, Debug)]
pub enum TicketReportError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("invalid multiplier: {0}")]
    Multiplier(#[from] MultiplierError),
    #[error("unsupported status: {0}")]
    Status(prost::UnknownEnumValue),
}

/// A ticket as submitted, after validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataTransferMultiplierTicket {
    pub hotspot_pubkey: PublicKeyBinary,
    /// The multiplier as submitted: parsed, but **not** range-checked.
    ///
    /// `None` means absent or unparseable. Deliberately not a
    /// [`DataTransferMultiplier`]: validating at decode would drop a bad ticket
    /// on the floor, because the file poller silently discards records that fail
    /// to decode. HIP-150 wants refusals on the record, so the ticket has to
    /// survive decoding in order to be refused — see
    /// `mobile_packet_verifier::multiplier::ingestor::ticket_status`.
    pub multiplier: Option<Decimal>,
    /// When the issuer signed it. Authoritative for which ticket is current,
    /// and the key that makes a replayed ticket a no-op.
    pub timestamp: DateTime<Utc>,
    pub message: String,
    pub signer_pubkey: PublicKeyBinary,
    pub signature: Vec<u8>,
}

/// A ticket as ingest recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataTransferMultiplierTicketReport {
    /// When ingest received it. Bounds which files the ticket may apply to; it
    /// is deliberately *not* what decides which ticket is current.
    pub received_timestamp: DateTime<Utc>,
    pub report: DataTransferMultiplierTicket,
}

/// A ticket after the verifier ruled on it. Rejections are written too, so a
/// refused grant is as auditable as an accepted one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDataTransferMultiplierTicketReport {
    pub verified_timestamp: DateTime<Utc>,
    pub report: DataTransferMultiplierTicketReport,
    pub status: VerifiedDataTransferMultiplierTicketStatus,
}

impl DataTransferMultiplierTicketReport {
    pub fn hotspot_pubkey(&self) -> &PublicKeyBinary {
        &self.report.hotspot_pubkey
    }
}

impl VerifiedDataTransferMultiplierTicketReport {
    pub fn is_valid(&self) -> bool {
        matches!(
            self.status,
            VerifiedDataTransferMultiplierTicketStatus::Valid
        )
    }

    pub fn hotspot_pubkey(&self) -> &PublicKeyBinary {
        self.report.hotspot_pubkey()
    }
}

impl MsgDecode for DataTransferMultiplierTicketReport {
    type Msg = proto::DataTransferMultiplierTicketIngestReportV1;
}

impl MsgDecode for VerifiedDataTransferMultiplierTicketReport {
    type Msg = proto::VerifiedDataTransferMultiplierTicketReportV1;
}

// === Conversion :: proto -> struct

/// Parse without judging. `None` for absent or unparseable, so the caller can
/// record a refusal rather than the record vanishing at decode.
pub fn parse_multiplier(value: Option<proto::Decimal>) -> Option<Decimal> {
    let value = value?;
    // The Decimal proto permits an exponent ("2.5e8") as well as plain notation,
    // and says services should normalize rather than reject.
    value
        .value
        .parse::<Decimal>()
        .or_else(|_| Decimal::from_scientific(&value.value))
        .ok()
}

impl TryFrom<proto::DataTransferMultiplierTicketReqV1> for DataTransferMultiplierTicket {
    type Error = TicketReportError;

    fn try_from(value: proto::DataTransferMultiplierTicketReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            hotspot_pubkey: value.hotspot_pubkey.into(),
            multiplier: parse_multiplier(value.multiplier),
            timestamp: value.timestamp_ms.to_timestamp_millis()?,
            message: value.message,
            signer_pubkey: value.signer_pubkey.into(),
            signature: value.signature,
        })
    }
}

impl TryFrom<proto::DataTransferMultiplierTicketIngestReportV1>
    for DataTransferMultiplierTicketReport
{
    type Error = TicketReportError;

    fn try_from(
        value: proto::DataTransferMultiplierTicketIngestReportV1,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: value.received_timestamp_ms.to_timestamp_millis()?,
            report: value
                .report
                .ok_or(TicketReportError::MissingField("ticket_report.report"))?
                .try_into()?,
        })
    }
}

impl TryFrom<proto::VerifiedDataTransferMultiplierTicketReportV1>
    for VerifiedDataTransferMultiplierTicketReport
{
    type Error = TicketReportError;

    fn try_from(
        value: proto::VerifiedDataTransferMultiplierTicketReportV1,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            verified_timestamp: value.verified_timestamp_ms.to_timestamp_millis()?,
            report: value
                .report
                .ok_or(TicketReportError::MissingField(
                    "verified_ticket_report.report",
                ))?
                .try_into()?,
            status: prost_enum(value.status, TicketReportError::Status)?,
        })
    }
}

// === Conversion :: struct -> proto

impl From<DataTransferMultiplierTicket> for proto::DataTransferMultiplierTicketReqV1 {
    fn from(value: DataTransferMultiplierTicket) -> Self {
        Self {
            hotspot_pubkey: value.hotspot_pubkey.into(),
            multiplier: value.multiplier.map(|m| proto::Decimal {
                value: m.to_string(),
            }),
            timestamp_ms: value.timestamp.encode_timestamp_millis(),
            message: value.message,
            signer_pubkey: value.signer_pubkey.into(),
            signature: value.signature,
        }
    }
}

impl From<DataTransferMultiplierTicketReport>
    for proto::DataTransferMultiplierTicketIngestReportV1
{
    fn from(value: DataTransferMultiplierTicketReport) -> Self {
        Self {
            received_timestamp_ms: value.received_timestamp.encode_timestamp_millis(),
            report: Some(value.report.into()),
        }
    }
}

impl From<VerifiedDataTransferMultiplierTicketReport>
    for proto::VerifiedDataTransferMultiplierTicketReportV1
{
    fn from(value: VerifiedDataTransferMultiplierTicketReport) -> Self {
        Self {
            verified_timestamp_ms: value.verified_timestamp.encode_timestamp_millis(),
            report: Some(value.report.into()),
            status: value.status.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    /// These strings are a stored data format, not a display detail: they land
    /// in `data_transfer.multiplier_ticket_history.status` and in a metric
    /// label. Changing one silently reclassifies history, so pin them exactly
    /// rather than asserting a shape.
    #[test]
    fn status_strings_are_stable() {
        use VerifiedDataTransferMultiplierTicketStatus as Status;

        for (status, expected) in [
            (Status::Valid, "valid"),
            (Status::InvalidSigner, "invalid_signer"),
            (Status::InvalidMultiplier, "invalid_multiplier"),
            (Status::InvalidHotspotKey, "invalid_hotspot_key"),
            (Status::InvalidTimestamp, "invalid_timestamp"),
        ] {
            assert_eq!(ticket_status_string(status), expected);
            // The point of the mapping: prost's own name restates the column.
            assert_ne!(ticket_status_string(status), status.as_str_name());
        }
    }

    fn dec_proto(s: &str) -> proto::Decimal {
        proto::Decimal {
            value: s.to_string(),
        }
    }

    fn parse(s: &str) -> Result<DataTransferMultiplier, MultiplierError> {
        DataTransferMultiplier::try_from(dec_proto(s))
    }

    #[test]
    fn accepts_and_normalizes_valid_multipliers() {
        // The Decimal spec permits a leading sign and an exponent, so these are
        // legal input and are canonicalized rather than rejected.
        for input in [
            "1", "1.0", "1.00", "1.5", "1.50", "1.5e0", "+1.5", "5", "5.0",
        ] {
            assert!(parse(input).is_ok(), "{input} should parse");
        }

        // Every spelling of the same value must produce the same value, so a
        // difference in formatting cannot become a difference in multiplier.
        assert_eq!(parse("1.5").unwrap(), parse("1.50").unwrap());
        assert_eq!(parse("1.5").unwrap(), parse("1.5e0").unwrap());
        assert_eq!(parse("1.5").unwrap(), parse("+1.5").unwrap());
        assert_eq!(parse("1").unwrap(), DataTransferMultiplier::DEFAULT);
        assert_eq!(parse("1.000").unwrap(), DataTransferMultiplier::DEFAULT);
    }

    #[test]
    fn rejects_invalid_multipliers() {
        assert_eq!(
            parse("").unwrap_err(),
            MultiplierError::Unparseable("".into())
        );
        assert_eq!(
            parse("abc").unwrap_err(),
            MultiplierError::Unparseable("abc".into())
        );
        assert_eq!(
            parse("NaN").unwrap_err(),
            MultiplierError::Unparseable("NaN".into())
        );

        // Below the floor: negative, zero, and just under 1.
        assert!(matches!(
            parse("-1").unwrap_err(),
            MultiplierError::OutOfRange(_)
        ));
        assert!(matches!(
            parse("0").unwrap_err(),
            MultiplierError::OutOfRange(_)
        ));
        assert!(matches!(
            parse("0.999999").unwrap_err(),
            MultiplierError::OutOfRange(_)
        ));

        // Above the ceiling, by the smallest representable step.
        assert!(matches!(
            parse("5.000001").unwrap_err(),
            MultiplierError::OutOfRange(_)
        ));

        // More precision than we will carry.
        assert!(matches!(
            parse("1.5000001").unwrap_err(),
            MultiplierError::TooPrecise(_)
        ));

        // A value far outside `Decimal`'s range fails at the parse step.
        assert!(matches!(
            parse("1e400").unwrap_err(),
            MultiplierError::Unparseable(_)
        ));
    }

    #[test]
    fn bounds_are_inclusive() {
        assert!(parse("1").is_ok());
        assert!(parse("5").is_ok());
    }

    #[test]
    fn default_is_the_identity() {
        let default = DataTransferMultiplier::DEFAULT;
        assert!(default.is_default());
        for dc in [0, 1, 2, 7, 100_000, u32::MAX as u64] {
            assert_eq!(
                default.apply(dc).unwrap(),
                dc,
                "default must not change {dc}"
            );
        }
    }

    #[test]
    fn apply_rounds_down() {
        let one_and_a_half = DataTransferMultiplier::new(dec!(1.5)).unwrap();

        // Exact.
        assert_eq!(one_and_a_half.apply(2).unwrap(), 3);
        assert_eq!(one_and_a_half.apply(10).unwrap(), 15);
        // 3 * 1.5 = 4.5 -> 4, not 5. A payer never burns more than the
        // multiplier earns.
        assert_eq!(one_and_a_half.apply(3).unwrap(), 4);
        assert_eq!(one_and_a_half.apply(1).unwrap(), 1);
        assert_eq!(one_and_a_half.apply(0).unwrap(), 0);

        let five = DataTransferMultiplier::new(dec!(5)).unwrap();
        assert_eq!(five.apply(2).unwrap(), 10);
    }

    /// The reason `apply` returns a `Result`. Saturating would hand back
    /// `u64::MAX` — charge the payer everything — for an arithmetic failure.
    #[test]
    fn overflow_is_an_error_not_a_maximum_charge() {
        let five = DataTransferMultiplier::new(dec!(5)).unwrap();

        let err = five.apply(u64::MAX).unwrap_err();
        assert!(
            matches!(err, MultiplierError::Overflow { .. }),
            "expected Overflow, got {err:?}"
        );

        // Specifically: it must not come back as the largest possible burn.
        assert_ne!(five.apply(u64::MAX).ok(), Some(u64::MAX));
    }

    /// The largest count that still fits, so the error above is a real boundary
    /// rather than the multiplier refusing everything large.
    #[test]
    fn applies_right_up_to_the_boundary() {
        let five = DataTransferMultiplier::new(dec!(5)).unwrap();
        let fits = u64::MAX / 5;

        assert_eq!(five.apply(fits).unwrap(), fits * 5);
    }

    #[test]
    fn apply_never_exceeds_the_exact_value() {
        let m = DataTransferMultiplier::new(dec!(1.333333)).unwrap();
        for dc in 0..500u64 {
            let exact = Decimal::from(dc) * m.as_decimal();
            assert!(
                Decimal::from(m.apply(dc).unwrap()) <= exact,
                "apply({dc}) exceeded {exact}"
            );
        }
    }

    #[test]
    fn ticket_round_trips_through_proto() {
        let ticket = DataTransferMultiplierTicket {
            hotspot_pubkey: PublicKeyBinary::from(vec![1, 2, 3]),
            multiplier: Some(dec!(1.5)),
            timestamp: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            message: "venue agreement 42".to_string(),
            signer_pubkey: PublicKeyBinary::from(vec![4, 5, 6]),
            signature: vec![7, 8, 9],
        };

        let proto: proto::DataTransferMultiplierTicketReqV1 = ticket.clone().into();
        let back = DataTransferMultiplierTicket::try_from(proto).unwrap();

        assert_eq!(ticket, back);
    }
}
