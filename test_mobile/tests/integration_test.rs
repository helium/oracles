use anyhow::Result;

mod common;

#[tokio::test]
async fn main() -> Result<()> {
    tokio::spawn(async move {
        match common::setup().await {
            Ok(_) => {}
            Err(e) => panic!("{:?}", e),
        }
    });

    Ok(())
}
