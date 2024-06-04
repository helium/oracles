use anyhow::{bail, Result};
use std::process::Command;

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

    fn stop(&self) -> Result<String> {
        let up_output = Command::new("docker")
            .current_dir("../docker/mobile/")
            .arg("compose")
            .arg("stop")
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
        tracing::debug!("Docker dropped");
        match self.stop() {
            Ok(_) => tracing::info!("docker stack stopped"),
            Err(e) => tracing::error!("docker compose stopped failed: {:?}", e),
        }
    }
}
