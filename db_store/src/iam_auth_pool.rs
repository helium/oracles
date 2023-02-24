use crate::{Error, Result, Settings};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, Postgres},
    Pool,
};

use aws_credential_types::Credentials;
use aws_sig_auth::signer::{
    self, HttpSignatureType, OperationSigningConfig, RequestConfig, SigningError,
};
use aws_smithy_http::body::SdkBody;
use aws_types::region::{Region, SigningRegion};
use aws_types::SigningService;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub async fn connect(
    settings: &Settings,
    default_max_connections: usize,
    shutdown: triggered::Listener,
) -> Result<(Pool<Postgres>, futures::future::BoxFuture<'static, Result>)> {
    let aws_config = aws_config::load_from_env().await;
    let client = aws_sdk_sts::Client::new(&aws_config);
    let connect_options = connect_options(&client, settings).await?;

    let pool = PgPoolOptions::new()
        .max_connections(
            settings
                .max_connections
                .unwrap_or(default_max_connections as u32),
        )
        .connect_with(connect_options)
        .await?;

    let cloned_settings = settings.clone();
    let cloned_pool = pool.clone();
    let join_handle =
        tokio::spawn(async move { run(client, cloned_settings, cloned_pool, shutdown).await });

    Ok((
        pool,
        Box::pin(async move {
            match join_handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(Error::from(err)),
            }
        }),
    ))
}

async fn run(
    client: aws_sdk_sts::Client,
    settings: Settings,
    pool: Pool<Postgres>,
    shutdown: triggered::Listener,
) -> Result {
    let duration = std::time::Duration::from_secs(settings.iam_duration_seconds.unwrap() as u64)
        - Duration::from_secs(120);

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => break,
            _ = tokio::time::sleep(duration) => {
                let connect_options = connect_options(&client, &settings).await?;
                pool.set_connect_options(connect_options);
            }
        }
    }

    Ok(())
}

async fn connect_options(
    client: &aws_sdk_sts::Client,
    settings: &Settings,
) -> Result<PgConnectOptions> {
    let auth_token = auth_token(client, settings).await?;

    Ok(PgConnectOptions::new()
        .host(&settings.host)
        .port(settings.port)
        .database(&settings.database)
        .username(&settings.username)
        .password(&auth_token))
}

async fn auth_token(client: &aws_sdk_sts::Client, settings: &Settings) -> Result<String> {
    let credentials = credentials(client, settings).await?;

    generate_rds_iam_token(
        &settings.host,
        region(settings)?,
        settings.port,
        &settings.username,
        &credentials,
        std::time::SystemTime::now(),
    )
    .map_err(Error::from)
}

async fn credentials(client: &aws_sdk_sts::Client, settings: &Settings) -> Result<Credentials> {
    assume_role(client, settings)
        .await?
        .credentials()
        .ok_or_else(|| Error::InvalidAssumedCredentials("No Credientials available".to_string()))
        .and_then(|creds| {
            Ok(Credentials::new(
                creds.access_key_id().ok_or_else(|| {
                    Error::InvalidAssumedCredentials("no access_key_id".to_string())
                })?,
                creds.secret_access_key().ok_or_else(|| {
                    Error::InvalidAssumedCredentials("no secret_access_key".to_string())
                })?,
                creds.session_token().map(|s| s.to_string()),
                creds
                    .expiration()
                    .map(|e| UNIX_EPOCH + Duration::from_secs(e.secs() as u64)),
                "Helium Foundation",
            ))
        })
}

fn region(settings: &Settings) -> Result<Region> {
    settings
        .iam_region
        .as_ref()
        .map(|r| Region::new(r.clone()))
        .ok_or_else(|| Error::InvalidConfiguration("iam_region is required".to_string()))
}

async fn assume_role(
    client: &aws_sdk_sts::Client,
    settings: &Settings,
) -> Result<aws_sdk_sts::output::AssumeRoleOutput> {
    client
        .assume_role()
        .role_arn(
            settings.iam_role_arn.as_ref().ok_or_else(|| {
                Error::InvalidConfiguration("iam_role_arn is required".to_string())
            })?,
        )
        .role_session_name(settings.iam_role_session_name.as_ref().ok_or_else(|| {
            Error::InvalidConfiguration("iam_role_session_name is required".to_string())
        })?)
        .duration_seconds(settings.iam_duration_seconds.ok_or_else(|| {
            Error::InvalidConfiguration("iam_duration_seconds is required".to_string())
        })?)
        .send()
        .await
        .map_err(Error::from)
}

fn generate_rds_iam_token(
    db_hostname: &str,
    region: Region,
    port: u16,
    db_username: &str,
    credentials: &Credentials,
    timestamp: SystemTime,
) -> std::result::Result<String, SigningError> {
    let signer = signer::SigV4Signer::new();
    let mut operation_config = OperationSigningConfig::default_config();
    operation_config.signature_type = HttpSignatureType::HttpRequestQueryParams;
    operation_config.expires_in = Some(Duration::from_secs(15 * 60));
    let request_config = RequestConfig {
        request_ts: timestamp,
        region: &SigningRegion::from(region),
        service: &SigningService::from_static("rds-db"),
        payload_override: None,
    };
    let mut request = http::Request::builder()
        .uri(format!(
            "http://{db_hostname}:{port}/?Action=connect&DBUser={db_user}",
            db_hostname = db_hostname,
            port = port,
            db_user = db_username
        ))
        .body(SdkBody::empty())
        .expect("valid request");
    let _signature = signer.sign(
        &operation_config,
        &request_config,
        credentials,
        &mut request,
    )?;
    let mut uri = request.uri().to_string();
    assert!(uri.starts_with("http://"));
    let uri = uri.split_off("http://".len());
    Ok(uri)
}
