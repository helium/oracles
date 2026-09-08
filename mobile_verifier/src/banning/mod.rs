//! Hotspot ban ingestion.
//!
//! This service verifies that a ban report was submitted by a key authorized for
//! [`NetworkKeyRole::Banning`](helium_proto::services::mobile_config::NetworkKeyRole)
//! and republishes it as a `VerifiedBanIngestReport` to the output bucket. It
//! does not act on bans itself.
//!
//! Enforcement lives in `mobile-packet-verifier`, which consumes those verified
//! reports and refuses to burn DC for a banned radio. Because HIP-149 rewards
//! data transfer only, and a reward follows a burn, declining the burn is what a
//! ban means now — there is nothing left here to gate.
//!
//! IMPORTANT
//!
//! There is deliberately no ban check applied on the reward side of this
//! service. `DataTransferSession`s written by mobile-packet-verifier must appear
//! in the rewards file: by the time we see one, its DC has already been burnt.
//! Suppressing it here would burn a hotspot's DC without paying for it.

pub mod ingestor;
