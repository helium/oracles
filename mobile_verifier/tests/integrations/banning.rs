//! Ban verification.
//!
//! This service's whole contribution to banning is the verdict it stamps on a
//! report before republishing it — mobile-packet-verifier consumes those verified
//! reports and is where a ban actually takes effect. Nothing is stored locally,
//! so these tests are about the verdict and nothing else.

use chrono::Utc;
use file_store_oracles::mobile_ban::{
    proto::VerifiedBanIngestReportStatus, BanAction, BanDetails, BanReason, BanReport, BanRequest,
    BanType, UnbanDetails,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_verifier::authorization::AuthorizationVerifier;
use mobile_verifier::banning::ingestor::process_ban_report;

struct AllVerified;

impl AuthorizationVerifier for AllVerified {
    fn is_authorized(&self, _address: &PublicKeyBinary, _role: NetworkKeyRole) -> bool {
        true
    }
}

struct NoneVerified;

impl AuthorizationVerifier for NoneVerified {
    fn is_authorized(&self, _address: &PublicKeyBinary, _role: NetworkKeyRole) -> bool {
        false
    }
}

/// Records which role was asked about, to pin that bans are checked against the
/// banning role rather than whatever role happens to be authorized.
struct RecordingVerifier(std::sync::Mutex<Vec<NetworkKeyRole>>);

impl AuthorizationVerifier for RecordingVerifier {
    fn is_authorized(&self, _address: &PublicKeyBinary, role: NetworkKeyRole) -> bool {
        self.0.lock().unwrap().push(role);
        true
    }
}

fn ban_report(hotspot: &PublicKeyBinary, ban_pubkey: &PublicKeyBinary) -> BanReport {
    report(
        hotspot,
        ban_pubkey,
        BanAction::Ban(BanDetails {
            hotspot_serial: "test-serial".to_string(),
            message: "test-ban".to_string(),
            reason: BanReason::LocationGaming,
            ban_type: BanType::All,
            expiration_timestamp: None,
        }),
    )
}

fn unban_report(hotspot: &PublicKeyBinary, ban_pubkey: &PublicKeyBinary) -> BanReport {
    report(
        hotspot,
        ban_pubkey,
        BanAction::Unban(UnbanDetails {
            hotspot_serial: "test-serial".to_string(),
            message: "test-unban".to_string(),
        }),
    )
}

fn report(
    hotspot: &PublicKeyBinary,
    ban_pubkey: &PublicKeyBinary,
    ban_action: BanAction,
) -> BanReport {
    BanReport {
        received_timestamp: Utc::now(),
        report: BanRequest {
            hotspot_pubkey: hotspot.clone(),
            timestamp: Utc::now(),
            ban_pubkey: ban_pubkey.clone(),
            signature: vec![],
            ban_action,
        },
    }
}

#[test]
fn a_ban_from_an_authorized_key_verifies() {
    let hotspot = PublicKeyBinary::from(vec![1]);
    let ban_pubkey = PublicKeyBinary::from(vec![2]);

    let verified = process_ban_report(&AllVerified, ban_report(&hotspot, &ban_pubkey));

    assert!(verified.is_valid());
    assert_eq!(VerifiedBanIngestReportStatus::Valid, verified.status);
    // The report is republished intact for mobile-packet-verifier to act on.
    assert_eq!(hotspot, verified.report.report.hotspot_pubkey);
}

#[test]
fn a_ban_from_an_unauthorized_key_is_rejected() {
    let hotspot = PublicKeyBinary::from(vec![1]);
    let ban_pubkey = PublicKeyBinary::from(vec![2]);

    let verified = process_ban_report(&NoneVerified, ban_report(&hotspot, &ban_pubkey));

    assert!(!verified.is_valid());
    assert_eq!(
        VerifiedBanIngestReportStatus::InvalidBanKey,
        verified.status
    );
}

#[test]
fn an_unban_is_verified_the_same_way_as_a_ban() {
    let hotspot = PublicKeyBinary::from(vec![1]);
    let ban_pubkey = PublicKeyBinary::from(vec![2]);

    let authorized = process_ban_report(&AllVerified, unban_report(&hotspot, &ban_pubkey));
    assert_eq!(VerifiedBanIngestReportStatus::Valid, authorized.status);

    let unauthorized = process_ban_report(&NoneVerified, unban_report(&hotspot, &ban_pubkey));
    assert_eq!(
        VerifiedBanIngestReportStatus::InvalidBanKey,
        unauthorized.status
    );
}

/// The submitter's authority is what is checked — not the banned hotspot's.
#[test]
fn authorization_is_checked_against_the_submitting_key_and_banning_role() {
    let hotspot = PublicKeyBinary::from(vec![1]);
    let ban_pubkey = PublicKeyBinary::from(vec![2]);
    let verifier = RecordingVerifier(std::sync::Mutex::new(Vec::new()));

    process_ban_report(&verifier, ban_report(&hotspot, &ban_pubkey));

    assert_eq!(
        vec![NetworkKeyRole::Banning],
        *verifier.0.lock().unwrap(),
        "bans must be authorized against the banning role"
    );
}
