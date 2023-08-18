use crate::{error::invalid_configuration, Error, Result, Settings};
use sqlx::{
    postgres::{PgConnectOptions, Postgres},
    Pool,
};

use aws_credential_types::Credentials;
use aws_sig_auth::signer::{self, HttpSignatureType, OperationSigningConfig, RequestConfig};
use aws_smithy_http::body::SdkBody;
use aws_types::{
    region::{Region, SigningRegion},
    SigningService,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub async fn connect(settings: &Settings) -> Result<Pool<Postgres>> {
    let aws_config = aws_config::load_from_env().await;
    let client = aws_sdk_sts::Client::new(&aws_config);
    let connect_parameters = ConnectParameters::try_from(settings)?;
    let connect_options = connect_parameters.connect_options(&client).await?;

    let pool = settings
        .pool_options()
        .connect_with(connect_options)
        .await?;

    let cloned_pool = pool.clone();
    tokio::spawn(async move { run(client, connect_parameters, cloned_pool).await });

    Ok(pool)
}

async fn run(
    client: aws_sdk_sts::Client,
    connect_parameters: ConnectParameters,
    pool: Pool<Postgres>,
) -> Result {
    let duration = std::time::Duration::from_secs(connect_parameters.iam_duration_seconds as u64)
        - Duration::from_secs(120);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                let connect_options = connect_parameters.connect_options(&client).await?;
                pool.set_connect_options(connect_options);
            }
        }
    }
}

struct ConnectParameters {
    host: String,
    port: u16,
    database: String,
    username: String,
    iam_role_arn: String,
    iam_role_session_name: String,
    iam_duration_seconds: i32,
    iam_region: Region,
}

impl TryFrom<&Settings> for ConnectParameters {
    type Error = Error;

    fn try_from(settings: &Settings) -> Result<Self> {
        Ok(Self {
            host: settings
                .host
                .clone()
                .ok_or_else(|| invalid_configuration("host is required"))?,
            port: settings
                .port
                .ok_or_else(|| invalid_configuration("port is required"))?,
            database: settings
                .database
                .clone()
                .ok_or_else(|| invalid_configuration("database is required"))?,
            username: settings
                .username
                .clone()
                .ok_or_else(|| invalid_configuration("username is required"))?,
            iam_role_arn: settings
                .iam_role_arn
                .clone()
                .ok_or_else(|| invalid_configuration("iam_role_arn is required"))?,
            iam_role_session_name: settings
                .iam_role_session_name
                .clone()
                .ok_or_else(|| invalid_configuration("iam_role_session_name is required"))?,
            iam_duration_seconds: settings
                .iam_duration_seconds
                .ok_or_else(|| invalid_configuration("iam_duration_seconds is required"))?,
            iam_region: region(settings)?,
        })
    }
}

impl ConnectParameters {
    async fn connect_options(&self, client: &aws_sdk_sts::Client) -> Result<PgConnectOptions> {
        let auth_token = self.auth_token(client).await?;

        Ok(PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .database(&self.database)
            .username(&self.username)
            .password(&auth_token))
    }

    async fn auth_token(&self, client: &aws_sdk_sts::Client) -> Result<String> {
        let credentials = self.credentials(client).await?;

        generate_rds_iam_token(
            &self.host,
            self.iam_region.clone(),
            self.port,
            &self.username,
            &credentials,
            std::time::SystemTime::now(),
        )
    }

    async fn credentials(&self, client: &aws_sdk_sts::Client) -> Result<Credentials> {
        self.assume_role(client)
            .await?
            .credentials()
            .ok_or_else(|| {
                Error::InvalidAssumedCredentials("No Credientials available".to_string())
            })
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

    async fn assume_role(
        &self,
        client: &aws_sdk_sts::Client,
    ) -> Result<aws_sdk_sts::output::AssumeRoleOutput> {
        client
            .assume_role()
            .role_arn(self.iam_role_arn.clone())
            .role_session_name(self.iam_role_session_name.clone())
            .duration_seconds(self.iam_duration_seconds)
            .send()
            .await
            .map_err(Error::from)
    }
}

fn region(settings: &Settings) -> Result<Region> {
    settings
        .iam_region
        .as_ref()
        .cloned()
        .map(Region::new)
        .ok_or_else(|| invalid_configuration("iam_region is required"))
}

fn generate_rds_iam_token(
    db_hostname: &str,
    region: Region,
    port: u16,
    db_username: &str,
    credentials: &Credentials,
    timestamp: SystemTime,
) -> Result<String> {
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
    if uri.starts_with("http://") {
        Ok(uri.split_off("http://".len()))
    } else {
        Err(Error::InvalidAuthToken())
    }
}
