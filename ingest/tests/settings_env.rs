//! Settings that reach ingest through the environment rather than the settings
//! file.
//!
//! Its own test binary because `Settings::new` merges the process environment
//! on every call: setting `INGEST__` variables inside the unit-test process
//! would leak into the file-based settings tests running beside it.
//!
//! What this pins down is that a bucket's credentials can be injected as
//! environment variables rather than having to live in the settings file. It is
//! why `file_upload.buckets` is keyed by a label rather than by bucket name: bucket
//! names are hyphenated and hyphens cannot appear in an environment variable.

use ingest::Settings;
use std::path::Path;

#[test]
fn the_mirror_can_be_configured_entirely_from_the_environment() {
    // Note the `INGEST__` prefix: the separator is `__`, not a single
    // underscore, so `INGEST_MODE` would be ignored.
    for (key, value) in [
        ("INGEST__MODE", "mobile"),
        ("INGEST__FILE_UPLOAD__ROOT", "/opt/ingest/data"),
        (
            "INGEST__FILE_UPLOAD__BUCKETS__PRIMARY__BUCKET",
            "ingest-bucket",
        ),
        ("INGEST__FILE_UPLOAD__BUCKETS__PRIMARY__REGION", "us-west-2"),
        ("INGEST__FILE_UPLOAD__BUCKETS__R2__BUCKET", "ingest-mirror"),
        (
            "INGEST__FILE_UPLOAD__BUCKETS__R2__ENDPOINT",
            "https://accountid.r2.cloudflarestorage.com",
        ),
        ("INGEST__FILE_UPLOAD__BUCKETS__R2__REGION", "auto"),
        (
            "INGEST__FILE_UPLOAD__BUCKETS__R2__ACCESS_KEY_ID",
            "r2-key-id",
        ),
        (
            "INGEST__FILE_UPLOAD__BUCKETS__R2__SECRET_ACCESS_KEY",
            "r2-secret",
        ),
    ] {
        std::env::set_var(key, value);
    }

    let settings = Settings::new(None::<&Path>).expect("settings from env");

    assert_eq!(
        std::path::PathBuf::from("/opt/ingest/data"),
        settings.file_upload.root
    );
    assert_eq!(
        "ingest-bucket",
        settings.file_upload.buckets["primary"].bucket
    );

    let r2 = &settings.file_upload.buckets["r2"];
    assert_eq!("ingest-mirror", r2.bucket);
    assert_eq!(
        Some("https://accountid.r2.cloudflarestorage.com".to_string()),
        r2.settings.endpoint
    );
    assert_eq!(Some("auto".to_string()), r2.settings.region);
    assert_eq!(Some("r2-key-id".to_string()), r2.settings.access_key_id);
    // The half that must never end up in a settings file or a config dump.
    assert_eq!(Some("r2-secret".to_string()), r2.settings.secret_access_key);
}
