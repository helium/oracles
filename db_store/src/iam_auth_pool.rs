use crate::{error::invalid_configuration, Error, Result, Settings};
use sqlx::{
    postgres::{PgConnectOptions, PgSslMode, Postgres},
    Pool,
};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sigv4::sign::v4;
use std::time::{Duration, SystemTime};

pub async fn connect(settings: &Settings) -> Result<Pool<Postgres>> {
    let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let client = aws_sdk_sts::Client::new(&aws_config);
    let connect_parameters = ConnectParameters::try_from(settings)?;
    let connect_options = connect_parameters.connect_options(&client).await?;
    let connect_options = if let Some(ref ca_path) = settings.ca_path {
        connect_options
            .ssl_mode(PgSslMode::VerifyCa)
            .ssl_root_cert(ca_path)
    } else {
        connect_options
    };

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
            credentials,
            std::time::SystemTime::now(),
        )
    }

    async fn credentials(&self, client: &aws_sdk_sts::Client) -> Result<Credentials> {
        self.assume_role(client)
            .await?
            .credentials()
            .ok_or_else(|| Error::InvalidAssumedCredentials("No Credentials available".to_string()))
            .and_then(|creds| {
                Ok(Credentials::new(
                    creds.access_key_id.clone(),
                    creds.secret_access_key.clone(),
                    Some(creds.session_token.clone()),
                    Some(
                        creds
                            .expiration
                            .try_into()
                            .map_err(|e| Error::AwsDateTimeConversionError(Box::new(e)))?,
                    ),
                    "Helium Foundation",
                ))
            })
    }

    async fn assume_role(
        &self,
        client: &aws_sdk_sts::Client,
    ) -> Result<aws_sdk_sts::operation::assume_role::AssumeRoleOutput> {
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

// Reference: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_rds_code_examples.html
pub fn generate_rds_iam_token(
    db_hostname: &str,
    region: aws_config::Region,
    port: u16,
    db_username: &str,
    credentials: Credentials,
    timestamp: SystemTime,
) -> Result<String> {
    use aws_sigv4::http_request::{
        self, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
    };

    let mut signing_settings = SigningSettings::default();
    signing_settings.expires_in = Some(Duration::from_secs(15 * 60));
    signing_settings.signature_location = SignatureLocation::QueryParams;

    let identity = credentials.into();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region.as_ref())
        .name("rds-db")
        .time(timestamp)
        .settings(signing_settings)
        .build()
        .map_err(|err| Error::SigningError(format!("SigningParams build: {err:?}")))?;

    let url = format!("https://{db_hostname}:{port}/?Action=connect&DBUser={db_username}",);

    let signable_request =
        SignableRequest::new("GET", &url, std::iter::empty(), SignableBody::Bytes(&[]))
            .expect("signable request");

    let (signing_instructions, _signature) =
        http_request::sign(signable_request, &signing_params.into())
            .map_err(|err| Error::SigningError(format!("Signing: {err:?}")))?
            .into_parts();

    let mut url = url::Url::parse(&url).expect("valid url");
    for (name, value) in signing_instructions.params() {
        url.query_pairs_mut().append_pair(name, value);
    }

    let response = url.to_string().split_off("https://".len());

    Ok(response)
}
