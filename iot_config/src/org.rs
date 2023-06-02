use crate::{
    helium_netids::{self, is_helium_netid, AddressStore, HeliumNetId},
    lora_field::{DevAddrConstraint, DevAddrField, NetIdField},
    org_service::UpdateAuthorizer,
};
use futures::stream::StreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use serde::Serialize;
use sqlx::{postgres::PgRow, types::Uuid, FromRow, Row};
use std::collections::HashSet;
use tokio::sync::watch;

pub mod proto {
    pub use helium_proto::services::iot_config::{
        org_update_req_v1::update_v1::Update, org_update_req_v1::UpdateV1, ActionV1, OrgResV1,
        OrgV1,
    };
}

#[derive(Clone, Debug, Serialize)]
pub struct Org {
    pub oui: u64,
    pub owner: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub locked: bool,
    pub delegate_keys: Option<Vec<PublicKeyBinary>>,
    pub constraints: Option<Vec<DevAddrConstraint>>,
}

#[derive(Debug, Serialize)]
pub struct OrgList {
    orgs: Vec<Org>,
}

impl FromRow<'_, PgRow> for Org {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let delegate_keys = row
            .get::<Vec<PublicKeyBinary>, &str>("delegate_keys")
            .into_iter()
            .map(Some)
            .collect();
        let constraints = row
            .get::<Vec<(i32, i32)>, &str>("constraints")
            .into_iter()
            .map(|(start, end)| {
                Some(DevAddrConstraint {
                    start_addr: start.into(),
                    end_addr: end.into(),
                })
            })
            .collect();
        Ok(Self {
            oui: row.get::<i64, &str>("oui") as u64,
            owner: row.get("owner_pubkey"),
            payer: row.get("payer_pubkey"),
            locked: row.get("locked"),
            delegate_keys,
            constraints,
        })
    }
}

pub type DelegateCache = HashSet<PublicKeyBinary>;

pub async fn delegate_keys_cache(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(watch::Sender<DelegateCache>, watch::Receiver<DelegateCache>), sqlx::Error> {
    let key_set = sqlx::query(r#" select delegate_pubkey from organization_delegate_keys "#)
        .fetch(db)
        .filter_map(|row| async move { row.ok() })
        .map(|row| row.get("delegate_pubkey"))
        .collect::<HashSet<PublicKeyBinary>>()
        .await;

    Ok(watch::channel(key_set))
}

pub async fn create_org(
    owner: PublicKeyBinary,
    payer: PublicKeyBinary,
    delegate_keys: Vec<PublicKeyBinary>,
    net_id: NetIdField,
    devaddr_ranges: &[DevAddrConstraint],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres>,
) -> Result<Org, OrgStoreError> {
    let mut txn = db.begin().await?;

    let oui = sqlx::query(
        r#"
        insert into organizations (owner_pubkey, payer_pubkey)
        values ($1, $2)
        returning oui
        "#,
    )
    .bind(&owner)
    .bind(&payer)
    .fetch_one(&mut txn)
    .await
    .map_err(|_| {
        OrgStoreError::SaveOrg(format!("owner: {owner}, payer: {payer}, net_id: {net_id}"))
    })?
    .get::<i64, &str>("oui");

    if !delegate_keys.is_empty() {
        let delegate_keys = delegate_keys
            .into_iter()
            .map(|key| (key, oui))
            .collect::<Vec<(PublicKeyBinary, i64)>>();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            " insert into organization_delegate_keys (delegate_pubkey, oui) ",
        );
        query_builder.push_values(delegate_keys, |mut builder, (key, oui)| {
            builder.push_bind(key).push_bind(oui);
        });
        query_builder
            .build()
            .execute(&mut txn)
            .await
            .map_err(|_| {
                OrgStoreError::SaveDelegates(format!(
                    "owner: {owner}, payer: {payer}, net_id: {net_id}"
                ))
            })
            .map(|_| ())?
    };

    if is_helium_netid(&net_id) {
        insert_helium_constraints(oui as u64, net_id, devaddr_ranges, &mut txn).await
    } else {
        let constraint = devaddr_ranges
            .first()
            .ok_or(OrgStoreError::SaveConstraints(
                "no devaddr constraints supplied".to_string(),
            ))?;
        if check_roamer_constraint_count(net_id, &mut txn).await? == 0 {
            insert_roamer_constraint(oui as u64, net_id, constraint, &mut txn).await
        } else {
            return Err(OrgStoreError::SaveConstraints(format!(
                "constraint already in use {constraint:?}"
            )));
        }
    }
    .map_err(|_| OrgStoreError::SaveConstraints(format!("{devaddr_ranges:?}")))?;

    let org = get(oui as u64, &mut txn).await?;

    txn.commit().await?;

    Ok(org)
}

pub async fn update_org(
    oui: u64,
    authorizer: UpdateAuthorizer,
    updates: Vec<proto::UpdateV1>,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres>,
) -> Result<Org, OrgStoreError> {
    let mut txn = db.begin().await?;

    let net_id = get_org_netid(oui, &mut txn).await?;
    let is_helium_org = is_helium_netid(&net_id);
    let current_org = get(oui, &mut txn).await?;

    for update in updates {
        match update.update {
            Some(proto::Update::Owner(pubkeybin)) if authorizer == UpdateAuthorizer::Admin => {
                update_owner(oui, pubkeybin.into(), &mut txn).await?
            }
            Some(proto::Update::Devaddrs(addr_count))
                if authorizer == UpdateAuthorizer::Admin && is_helium_org =>
            {
                add_devaddr_slab(oui, net_id, addr_count, &mut txn).await?
            }
            Some(proto::Update::Constraint(constraint_update))
                if authorizer == UpdateAuthorizer::Admin && is_helium_org =>
            {
                match (constraint_update.action(), &constraint_update.constraint) {
                    (proto::ActionV1::Add, Some(ref constraint)) => add_constraint_update(oui, net_id, constraint.into(), &mut txn).await?,
                    (proto::ActionV1::Remove, Some(ref constraint)) => remove_constraint_update(oui, net_id, current_org.constraints.as_ref(), constraint.into(), &mut txn).await?,
                    _ => return Err(OrgStoreError::InvalidUpdate(format!("invalid action or missing devaddr constraint update: {constraint_update:?}")))
                }
            }
            Some(proto::Update::Payer(pubkeybin)) => {
                update_payer(oui, pubkeybin.into(), &mut txn).await?
            }
            Some(proto::Update::DelegateKey(delegate_key_update)) => {
                match delegate_key_update.action() {
                    proto::ActionV1::Add => {
                        add_delegate_key(oui, delegate_key_update.delegate_key.into(), &mut txn)
                            .await?
                    }
                    proto::ActionV1::Remove => {
                        remove_delegate_key(oui, delegate_key_update.delegate_key.into(), &mut txn)
                            .await?
                    }
                }
            }
            _ => {
                return Err(OrgStoreError::InvalidUpdate(format!(
                    "update: {update:?}, authorizer: {authorizer:?}"
                )))
            }
        };
    }

    let updated_org = get(oui, &mut txn).await?;

    txn.commit().await?;

    Ok(updated_org)
}

pub async fn get_org_netid(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<NetIdField, sqlx::Error> {
    let netid = sqlx::query_scalar::<_, i32>(
        " select net_id from organization_devaddr_constraints where oui = $1 limit 1 ",
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await?;
    Ok(netid.into())
}

async fn update_owner(
    oui: u64,
    owner_pubkey: PublicKeyBinary,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(" update organizations set owner_pubkey = $1 where oui = $2 ")
        .bind(owner_pubkey)
        .bind(oui as i64)
        .execute(db)
        .await
        .map(|_| ())
}

async fn update_payer(
    oui: u64,
    payer_pubkey: PublicKeyBinary,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(" update organizations set payer_pubkey = $1 where oui = $2 ")
        .bind(payer_pubkey)
        .bind(oui as i64)
        .execute(db)
        .await
        .map(|_| ())
}

async fn add_delegate_key(
    oui: u64,
    delegate_pubkey: PublicKeyBinary,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(" insert into organization_delegate_keys (delegate_pubkey, oui) values ($1, $2) ")
        .bind(delegate_pubkey)
        .bind(oui as i64)
        .execute(db)
        .await
        .map(|_| ())
}

async fn remove_delegate_key(
    oui: u64,
    delegate_pubkey: PublicKeyBinary,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(" delete from organization_delegate_keys where delegate_pubkey = $1 and oui = $2 ")
        .bind(delegate_pubkey)
        .bind(oui as i64)
        .execute(db)
        .await
        .map(|_| ())
}

async fn add_constraint_update(
    oui: u64,
    net_id: NetIdField,
    added_constraint: DevAddrConstraint,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), OrgStoreError> {
    let helium_net_id: HeliumNetId = net_id
        .try_into()
        .map_err(|err: &'static str| OrgStoreError::InvalidUpdate(err.to_string()))?;
    helium_netids::checkout_specified_devaddr_constraint(db, helium_net_id, &added_constraint)
        .await
        .map_err(|err| OrgStoreError::InvalidUpdate(format!("{err:?}")))?;
    insert_helium_constraints(oui, net_id, &[added_constraint], db).await?;
    Ok(())
}

async fn remove_constraint_update(
    oui: u64,
    net_id: NetIdField,
    org_constraints: Option<&Vec<DevAddrConstraint>>,
    removed_constraint: DevAddrConstraint,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), OrgStoreError> {
    let helium_net_id: HeliumNetId = net_id
        .try_into()
        .map_err(|err: &'static str| OrgStoreError::InvalidUpdate(err.to_string()))?;
    if let Some(org_constraints) = org_constraints {
        if org_constraints.contains(&removed_constraint) && org_constraints.len() > 1 {
            let remove_range = (u32::from(removed_constraint.start_addr)
                ..=u32::from(removed_constraint.end_addr))
                .collect::<Vec<u32>>();
            db.release_addrs(helium_net_id, &remove_range).await?;
            remove_helium_constraints(oui, &[removed_constraint], db).await?;
            Ok(())
        } else if org_constraints.len() == 1 {
            return Err(OrgStoreError::InvalidUpdate(
                "org must have at least one constraint range".to_string(),
            ));
        } else {
            return Err(OrgStoreError::InvalidUpdate(
                "cannot remove constraint leased by other org".to_string(),
            ));
        }
    } else {
        Err(OrgStoreError::InvalidUpdate(
            "no org constraints defined".to_string(),
        ))
    }
}

async fn add_devaddr_slab(
    oui: u64,
    net_id: NetIdField,
    addr_count: u64,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), OrgStoreError> {
    let helium_net_id: HeliumNetId = net_id
        .try_into()
        .map_err(|err: &'static str| OrgStoreError::InvalidUpdate(err.to_string()))?;
    let constraints = helium_netids::checkout_devaddr_constraints(txn, addr_count, helium_net_id)
        .await
        .map_err(|err| OrgStoreError::SaveConstraints(format!("{err:?}")))?;
    insert_helium_constraints(oui, net_id, &constraints, txn).await?;
    Ok(())
}

async fn insert_helium_constraints(
    oui: u64,
    net_id: NetIdField,
    devaddr_ranges: &[DevAddrConstraint],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        r#"
        insert into organization_devaddr_constraints (oui, net_id, start_addr, end_addr)
        "#,
    );
    query_builder.push_values(devaddr_ranges, |mut builder, range| {
        builder
            .push_bind(oui as i64)
            .push_bind(i32::from(net_id))
            .push_bind(i32::from(range.start_addr))
            .push_bind(i32::from(range.end_addr));
    });

    query_builder.build().execute(db).await.map(|_| ())
}

async fn remove_helium_constraints(
    oui: u64,
    devaddr_ranges: &[DevAddrConstraint],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    let constraints = devaddr_ranges
        .iter()
        .map(|constraint| (oui, constraint.start_addr, constraint.end_addr))
        .collect::<Vec<(u64, DevAddrField, DevAddrField)>>();
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "delete from organization_devaddr_constraints where (oui, start_addr, end_addr) in ",
    );
    query_builder.push_tuples(constraints, |mut builder, (oui, start_addr, end_addr)| {
        builder
            .push_bind(oui as i64)
            .push_bind(i32::from(start_addr))
            .push_bind(i32::from(end_addr));
    });

    query_builder.build().execute(db).await.map(|_| ())
}

async fn check_roamer_constraint_count(
    net_id: NetIdField,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        " select count(net_id) from organization_devaddr_constraints where net_id = $1 ",
    )
    .bind(i32::from(net_id))
    .fetch_one(db)
    .await
}

async fn insert_roamer_constraint(
    oui: u64,
    net_id: NetIdField,
    devaddr_range: &DevAddrConstraint,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into organization_devaddr_constraints (oui, net_id, start_addr, end_addr)
        values ($1, $2, $3, $4)
        "#,
    )
    .bind(oui as i64)
    .bind(i32::from(net_id))
    .bind(i32::from(devaddr_range.start_addr))
    .bind(i32::from(devaddr_range.end_addr))
    .execute(db)
    .await
    .map(|_| ())
}

const GET_ORG_SQL: &str = r#"
        select org.oui, org.owner_pubkey, org.payer_pubkey, org.locked,
            array(select (start_addr, end_addr) from organization_devaddr_constraints org_const where org_const.oui = org.oui) as constraints,
            array(select delegate_pubkey from organization_delegate_keys org_delegates where org_delegates.oui = org.oui) as delegate_keys
        from organizations org
        "#;

pub async fn list(db: impl sqlx::PgExecutor<'_>) -> Result<Vec<Org>, sqlx::Error> {
    Ok(sqlx::query_as::<_, Org>(GET_ORG_SQL)
        .fetch(db)
        .filter_map(|row| async move { row.ok() })
        .collect::<Vec<Org>>()
        .await)
}

pub async fn get(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<Org, sqlx::Error> {
    let mut query: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(GET_ORG_SQL);
    query.push(" where org.oui = $1 ");
    query
        .build_query_as::<Org>()
        .bind(oui as i64)
        .fetch_one(db)
        .await
}

pub async fn get_constraints_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<DevAddrConstraint>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let constraints = sqlx::query(
        r#"
        select consts.start_addr, consts.end_addr from organization_devaddr_constraints consts
        join routes on routes.oui = consts.oui
        where routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| DevAddrConstraint {
        start_addr: row.get::<i32, &str>("start_addr").into(),
        end_addr: row.get::<i32, &str>("end_addr").into(),
    })
    .collect();

    Ok(constraints)
}

pub async fn get_route_ids_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<String>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let route_ids = sqlx::query(
        r#"
        select routes.id from routes
        where oui = (
            select organizations.oui from organizations
            join routes on organizations.oui = routes.oui
            where routes.id = $1
        )
        "#,
    )
    .bind(uuid)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| row.get::<Uuid, &str>("id").to_string())
    .collect();

    Ok(route_ids)
}

pub async fn is_locked(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        r#"
        select locked from organizations where oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await
}

pub async fn toggle_locked(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        update organizations
        set locked = not locked
        where oui = $1
        "#,
    )
    .bind(oui as i64)
    .execute(db)
    .await?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum OrgStoreError {
    #[error("error retrieving saved org row: {0}")]
    FetchOrg(#[from] sqlx::Error),
    #[error("error saving org: {0}")]
    SaveOrg(String),
    #[error("error saving delegate keys: {0}")]
    SaveDelegates(String),
    #[error("error saving devaddr constraints: {0}")]
    SaveConstraints(String),
    #[error("unable to deserialize pubkey: {0}")]
    DecodeKey(#[from] helium_crypto::Error),
    #[error("Route Id parse error: {0}")]
    RouteIdParse(#[from] sqlx::types::uuid::Error),
    #[error("Invalid update: {0}")]
    InvalidUpdate(String),
}

pub async fn get_org_pubkeys(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, OrgStoreError> {
    let org = get(oui, db).await?;

    let mut pubkeys: Vec<PublicKey> = vec![
        PublicKey::try_from(org.owner)?,
        PublicKey::try_from(org.payer)?,
    ];

    if let Some(ref mut delegate_pubkeys) = org
        .delegate_keys
        .map(|keys| {
            keys.into_iter()
                .map(PublicKey::try_from)
                .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()
        })
        .transpose()?
    {
        pubkeys.append(delegate_pubkeys);
    }

    Ok(pubkeys)
}

pub async fn get_org_pubkeys_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let org = sqlx::query_as::<_, Org>(
        r#"
        select org.oui, org.owner_pubkey, org.payer_pubkey, org.locked,
            array(select (start_addr, end_addr) from organization_devaddr_constraints org_const where org_const.oui = org.oui) as constraints,
            array(select delegate_pubkey from organization_delegate_keys org_delegates where org_delegates.oui = org.oui) as delegate_keys
        from organizations org
        join routes on org.oui = routes.oui
        where routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await?;

    let mut pubkeys: Vec<PublicKey> = vec![
        PublicKey::try_from(org.owner)?,
        PublicKey::try_from(org.payer)?,
    ];

    if let Some(ref mut delegate_keys) = org
        .delegate_keys
        .map(|keys| {
            keys.into_iter()
                .map(PublicKey::try_from)
                .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()
        })
        .transpose()?
    {
        pubkeys.append(delegate_keys);
    }

    Ok(pubkeys)
}

impl From<Org> for proto::OrgV1 {
    fn from(org: Org) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            locked: org.locked,
            delegate_keys: org.delegate_keys.map_or_else(Vec::new, |keys| {
                keys.iter().map(|key| key.as_ref().into()).collect()
            }),
        }
    }
}
