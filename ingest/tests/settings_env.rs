//! Settings that reach ingest through the environment rather than the settings
//! file.
//!
//! Its own test binary because `Settings::new` merges the process environment
//! on every call: setting `INGEST__` variables inside the unit-test process
//! would leak into the file-based settings tests running beside it.
//!
//! What this pins down is that the mirror bucket's credentials can be injected
//! as environment variables rather than having to live in the settings file.

use ingest::Settings;
use std::path::Path;

#[test]
fn the_mirror_can_be_configured_entirely_from_the_environment() {
    // Note the `INGEST__` prefix: the separator is `__`, not a single
    // underscore, so `INGEST_MODE` would be ignored.
    for (key, value) in [
        ("INGEST__MODE", "mobile"),
        ("INGEST__OUTPUT_BUCKET", "ingest-bucket"),
        ("INGEST__FILE_STORE__REGION", "us-west-2"),
        ("INGEST__OUTPUT_BUCKET_MIRROR", "ingest-mirror"),
        (
            "INGEST__FILE_STORE_MIRROR__ENDPOINT",
            "https://accountid.r2.cloudflarestorage.com",
        ),
        ("INGEST__FILE_STORE_MIRROR__REGION", "auto"),
        ("INGEST__FILE_STORE_MIRROR__ACCESS_KEY_ID", "r2-key-id"),
        ("INGEST__FILE_STORE_MIRROR__SECRET_ACCESS_KEY", "r2-secret"),
    ] {
        std::env::set_var(key, value);
    }

    let settings = Settings::new(None::<&Path>).expect("settings from env");

    assert_eq!("ingest-bucket", settings.output_bucket);
    assert_eq!(Some("us-west-2".to_string()), settings.file_store.region);

    assert_eq!(
        Some("ingest-mirror".to_string()),
        settings.output_bucket_mirror
    );
    let mirror = settings.file_store_mirror.expect("mirror file store");
    assert_eq!(
        Some("https://accountid.r2.cloudflarestorage.com".to_string()),
        mirror.endpoint
    );
    assert_eq!(Some("auto".to_string()), mirror.region);
    assert_eq!(Some("r2-key-id".to_string()), mirror.access_key_id);
    // The half that must never end up in a settings file or a config dump.
    assert_eq!(Some("r2-secret".to_string()), mirror.secret_access_key);
}
