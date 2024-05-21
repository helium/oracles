use anyhow::{bail, Result};
use tokio::process::Command;

pub async fn setup() -> Result<String> {
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
