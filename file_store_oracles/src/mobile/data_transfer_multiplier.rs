//! HIP-150 data transfer multipliers, and the tickets that grant them.

use std::time::Duration;

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
pub const MAX_CLOCK_DRIFT: Duration = Duration::from_secs(60);
