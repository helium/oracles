use backon::{ExponentialBuilder, Retryable};
use helium_lib::solana_sdk::pubkey::Pubkey;
use helium_proto::services::iot_config::{self as proto, config_org_client::OrgClient};
use sqlx::{Pool, Postgres};
use std::net::SocketAddr;

pub async fn create_solana_org(
    pool: &Pool<Postgres>,
    authority: &String,
    escrow_key: &String,
    net_id: &String,
    oui: Option<i64>,
) -> anyhow::Result<(String, u64)> {
    let address = Pubkey::new_unique().to_string();
    let oui = oui.unwrap_or(1);

    sqlx::query(
        r#"
    INSERT INTO solana_organizations (
        address,
        net_id,
        authority,
        oui,
        escrow_key,
        approved
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    "#,
    )
    .bind(address.clone())
    .bind(net_id)
    .bind(authority)
    .bind(oui)
    .bind(escrow_key)
    .bind(true)
    .execute(pool)
    .await?;

    Ok((address, oui as u64))
}

pub async fn create_solana_org_devaddr_constraint(
    pool: &Pool<Postgres>,
    net_id: &String,
    organization: &String,
    current_addr_offset: Option<i64>,
    num_blocks: i64,
) -> anyhow::Result<String> {
    let address = Pubkey::new_unique().to_string();
    let end_addr = current_addr_offset.unwrap_or(0) + num_blocks * 8;

    sqlx::query(
        r#"
    INSERT INTO solana_organization_devaddr_constraints (
        address,
        net_id,
        organization,
        start_addr,
        end_addr
    )
    VALUES ($1, $2, $3, $4, $5)
    "#,
    )
    .bind(address.clone())
    .bind(net_id)
    .bind(organization)
    .bind(current_addr_offset.unwrap_or(0))
    .bind(end_addr)
    .execute(pool)
    .await?;

    Ok(address)
}

pub async fn create_solana_org_delegate_key(
    pool: &Pool<Postgres>,
    organization: &String,
    delegate: &String,
) -> anyhow::Result<String> {
    let address = Pubkey::new_unique().to_string();

    sqlx::query(
        r#"
        INSERT INTO solana_organization_delegate_keys (
            address,
            organization,
            delegate
        )
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(address.clone())
    .bind(organization)
    .bind(delegate)
    .execute(pool)
    .await?;

    Ok(address)
}

pub async fn create_solana_net_id(
    pool: &Pool<Postgres>,
    authority: &String,
    id: Option<i32>,
    current_addr_offset: Option<i64>,
) -> anyhow::Result<String> {
    let address = Pubkey::new_unique().to_string();

    sqlx::query(
        r#"
        INSERT INTO solana_net_ids (
            address,
            id,
            authority,
            current_addr_offset
        )
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(address.clone())
    .bind(id.unwrap_or(6))
    .bind(authority)
    .bind(current_addr_offset.unwrap_or(0))
    .execute(pool)
    .await?;

    Ok(address)
}

pub async fn create_org(socket_addr: SocketAddr, pool: &Pool<Postgres>) -> proto::OrgResV2 {
    let mut client = (|| OrgClient::connect(format!("http://{socket_addr}")))
        .retry(&ExponentialBuilder::default())
        .await
        .expect("org client");

    let payer = Pubkey::new_unique().to_string();
    let net_id_res = create_solana_net_id(pool, &payer, None, None).await;
    let net_id = net_id_res.unwrap();

    let org_res = create_solana_org(pool, &payer, &payer, &net_id, None).await;
    let (org_id, oui) = org_res.unwrap();

    let devaddr_res = create_solana_org_devaddr_constraint(pool, &net_id, &org_id, None, 8).await;
    let _devaddr = devaddr_res.unwrap();

    let response = match client.get_v2(proto::OrgGetReqV2 { oui }).await {
        Ok(resp) => resp,
        Err(e) => {
            panic!("Failed to get the org: {:?}", e);
        }
    };

    response.into_inner()
}
