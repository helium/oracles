//! Settings that reach ingest through the environment rather than the settings
//! file.
//!
//! Its own test binary because `Settings::new` merges the process environment
//! on every call: setting `INGEST__` variables inside the unit-test process
//! would leak into the file-based settings tests running beside it.
//!
//! What this pins down is that an additional output bucket's credentials can be
//! injected as environment variables. That is the whole reason
//! `additional_output_buckets` is a map keyed by name and not a list — `config`
//! builds nested maps from environment variables but cannot build sequences, so
//! a list would strand every mirror's secret in the settings file.

use ingest::Settings;
use std::path::Path;

#[test]
fn an_additional_bucket_can_be_configured_entirely_from_the_environment() {
    // Note the `INGEST__` prefix: the separator is `__`, not a single
    // underscore, so `INGEST_MODE` would be ignored.
    for (key, value) in [
        ("INGEST__MODE", "mobile"),
        ("INGEST__OUTPUT_BUCKET", "ingest-bucket"),
        ("INGEST__FILE_STORE__REGION", "us-west-2"),
        (
            "INGEST__ADDITIONAL_OUTPUT_BUCKETS__R2__BUCKET",
            "ingest-mirror",
        ),
        (
            "INGEST__ADDITIONAL_OUTPUT_BUCKETS__R2__ENDPOINT",
            "https://accountid.r2.cloudflarestorage.com",
        ),
        ("INGEST__ADDITIONAL_OUTPUT_BUCKETS__R2__REGION", "auto"),
        (
            "INGEST__ADDITIONAL_OUTPUT_BUCKETS__R2__ACCESS_KEY_ID",
            "r2-key-id",
        ),
        (
            "INGEST__ADDITIONAL_OUTPUT_BUCKETS__R2__SECRET_ACCESS_KEY",
            "r2-secret",
        ),
    ] {
        std::env::set_var(key, value);
    }

    let settings = Settings::new(None::<&Path>).expect("settings from env");

    assert_eq!("ingest-bucket", settings.output_bucket);
    assert_eq!(Some("us-west-2".to_string()), settings.file_store.region);

    let mirror = settings
        .additional_output_buckets
        .get("r2")
        .expect("r2 mirror");
    assert_eq!("ingest-mirror", mirror.bucket);
    assert_eq!(
        Some("https://accountid.r2.cloudflarestorage.com".to_string()),
        mirror.settings.endpoint
    );
    assert_eq!(Some("auto".to_string()), mirror.settings.region);
    assert_eq!(Some("r2-key-id".to_string()), mirror.settings.access_key_id);
    // The half that must never end up in a settings file or a config dump.
    assert_eq!(
        Some("r2-secret".to_string()),
        mirror.settings.secret_access_key
    );
}
