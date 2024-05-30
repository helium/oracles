use anyhow::{bail, Result};
use std::process::Command;
use std::time::Duration;
use tonic::transport::{Channel, Uri};

pub struct Docker;

impl Docker {
    pub fn new() -> Self {
        Self
    }

    pub fn up(&self) -> Result<String> {
        let up_output = Command::new("docker")
            .current_dir("../docker/mobile/")
            .arg("compose")
            .arg("up")
            .arg("-d")
            .output()?;

        if up_output.status.success() {
            let stdout = String::from_utf8(up_output.stdout)?;
            Ok(stdout)
        } else {
            let stderr = String::from_utf8(up_output.stderr)?;
            bail!(stderr)
        }
    }

    fn down(&self) -> Result<String> {
        let up_output = Command::new("docker")
            .current_dir("../docker/mobile/")
            .arg("compose")
            .arg("down")
            .output()?;

        if up_output.status.success() {
            let stdout = String::from_utf8(up_output.stdout)?;
            Ok(stdout)
        } else {
            let stderr = String::from_utf8(up_output.stderr)?;
            bail!(stderr)
        }
    }
}

impl Drop for Docker {
    fn drop(&mut self) {
        // This code runs when the scope exits, including if the test fails.
        tracing::info!("Test finished. Performing cleanup.");
        match self.down() {
            Ok(_) => tracing::info!("stack went down"),
            Err(e) => tracing::error!("docker compose down failed: {:?}", e),
        }
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
