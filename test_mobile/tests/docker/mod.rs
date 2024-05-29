use anyhow::{bail, Result};
use std::time::Duration;
use tokio::process::Command;
use tonic::transport::{Channel, Uri};

pub async fn up() -> Result<String> {
    let up_output = Command::new("docker")
        .current_dir("../docker/mobile/")
        .arg("compose")
        .arg("up")
        .arg("-d")
        .output()
        .await?;

    if up_output.status.success() {
        let stdout = String::from_utf8(up_output.stdout)?;
        Ok(stdout)
    } else {
        let stderr = String::from_utf8(up_output.stderr)?;
        bail!(stderr)
    }
}

pub async fn down() -> Result<String> {
    let up_output = Command::new("docker")
        .current_dir("../docker/mobile/")
        .arg("compose")
        .arg("down")
        .output()
        .await?;

    if up_output.status.success() {
        let stdout = String::from_utf8(up_output.stdout)?;
        Ok(stdout)
    } else {
        let stderr = String::from_utf8(up_output.stderr)?;
        bail!(stderr)
    }
}

pub async fn check_ingest_up(
    endpoint: &str,
    max_retries: u32,
    retry_delay: Duration,
) -> Result<()> {
    let mut retries = 0;

    loop {
        let uri = endpoint.parse::<Uri>()?;

        match Channel::builder(uri)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(5))
            .connect()
            .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                retries += 1;
                if retries >= max_retries {
                    return Err(anyhow::anyhow!(format!(
                        "Failed to connect to server after {} retries: {:?}",
                        max_retries, e
                    )));
                } else {
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }
}
