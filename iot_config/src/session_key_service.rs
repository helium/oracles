use crate::{
    admin::{AuthCache, KeyType},
    lora_field::DevAddrConstraint,
    org::{self, DbOrgError},
    session_key::{self, SessionKeyFilter},
    update_channel, GrpcResult, GrpcStreamRequest, GrpcStreamResult, Settings,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use file_store::traits::{MsgVerify, TimestampEncode};
use futures::{
    future::TryFutureExt,
    stream::{StreamExt, TryStreamExt},
};
use helium_crypto::{Keypair, PublicKey, Sign};
use helium_proto::{
    services::iot_config::{
        self, ActionV1, SessionKeyFilterGetReqV1, SessionKeyFilterListReqV1,
        SessionKeyFilterStreamReqV1, SessionKeyFilterStreamResV1, SessionKeyFilterUpdateReqV1,
        SessionKeyFilterUpdateResV1, SessionKeyFilterV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tonic::{Request, Response, Status};

const UPDATE_BATCH_LIMIT: usize = 5_000;

pub struct SessionKeyFilterService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    update_channel: broadcast::Sender<SessionKeyFilterStreamResV1>,
    shutdown: triggered::Listener,
    signing_key: Arc<Keypair>,
}

impl SessionKeyFilterService {
    pub fn new(
        settings: &Settings,
        auth_cache: AuthCache,
        pool: Pool<Postgres>,
        shutdown: triggered::Listener,
    ) -> Result<Self> {
        Ok(Self {
            auth_cache,
            pool,
            update_channel: update_channel(),
            shutdown,
            signing_key: Arc::new(settings.signing_keypair()?),
        })
    }

    fn subscribe_to_session_keys(&self) -> broadcast::Receiver<SessionKeyFilterStreamResV1> {
        self.update_channel.subscribe()
    }

    fn clone_update_channel(&self) -> broadcast::Sender<SessionKeyFilterStreamResV1> {
        self.update_channel.clone()
    }

    async fn verify_request_signature<'a, R>(
        &self,
        signer: &PublicKey,
        request: &R,
        id: u64,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .auth_cache
            .verify_signature_with_type(KeyType::Administrator, signer, request)
            .is_ok()
        {
            tracing::debug!(signer = signer.to_string(), "request authorized by admin");
            return Ok(());
        }

        let org_keys = org::get_org_pubkeys(id, &self.pool)
            .await
            .map_err(|_| Status::internal("auth verification error"))?;

        if org_keys.as_slice().contains(signer) && request.verify(signer).is_ok() {
            tracing::debug!(
                signer = signer.to_string(),
                "request authorized by delegate"
            );
            return Ok(());
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    fn verify_stream_request_signature<R>(
        &self,
        signer: &PublicKey,
        request: &R,
    ) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self.auth_cache.verify_signature(signer, request).is_ok() {
            tracing::debug!(signer = signer.to_string(), "request authorized");
            Ok(())
        } else {
            Err(Status::permission_denied("unauthorized request signature"))
        }
    }

    fn verify_public_key(&self, bytes: &[u8]) -> Result<PublicKey, Status> {
        PublicKey::try_from(bytes)
            .map_err(|_| Status::invalid_argument(format!("invalid public key: {bytes:?}")))
    }

    fn sign_response<R>(&self, response: &R) -> Result<Vec<u8>, Status>
    where
        R: Message,
    {
        self.signing_key
            .sign(&response.encode_to_vec())
            .map_err(|_| Status::internal("response signing error"))
    }

    async fn update_validator(&self, oui: u64) -> Result<SkfValidator, DbOrgError> {
        let admin_keys = self.auth_cache.get_keys_by_type(KeyType::Administrator);

        SkfValidator::new(oui, admin_keys, &self.pool).await
    }
}

#[tonic::async_trait]
impl iot_config::SessionKeyFilter for SessionKeyFilterService {
    type listStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn list(
        &self,
        request: Request<SessionKeyFilterListReqV1>,
    ) -> GrpcResult<Self::listStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, request.oui)
            .await?;

        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tokio::spawn(async move {
            let mut filters = session_key::list_for_oui(request.oui, &pool);

            while let Some(filter) = filters.next().await {
                let message = match filter {
                    Ok(filter) => Ok(filter.into()),
                    Err(bad_filter) => Err(Status::internal(format!(
                        "invalid session key filter {bad_filter:?}"
                    ))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type getStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn get(&self, request: Request<SessionKeyFilterGetReqV1>) -> GrpcResult<Self::getStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_request_signature(&signer, &request, request.oui)
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let pool = self.pool.clone();

        tokio::spawn(async move {
            let mut filters =
                session_key::list_for_oui_and_devaddr(request.oui, request.devaddr.into(), &pool);

            while let Some(filter) = filters.next().await {
                let message = match filter {
                    Ok(filter) => Ok(filter.into()),
                    Err(bad_filter) => Err(Status::internal(format!(
                        "invalid session key filter {bad_filter:?}"
                    ))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    async fn update(
        &self,
        request: GrpcStreamRequest<SessionKeyFilterUpdateReqV1>,
    ) -> GrpcResult<SessionKeyFilterUpdateResV1> {
        let mut request = request.into_inner();

        let mut to_add: Vec<SessionKeyFilter> = vec![];
        let mut to_remove: Vec<SessionKeyFilter> = vec![];
        let mut pending_updates: usize = 0;

        let mut validator: SkfValidator = if let Ok(Some(first_update)) = request.message().await {
            if let Some(filter) = &first_update.filter {
                let mut validator = self
                    .update_validator(filter.oui)
                    .await
                    .map_err(|_| Status::internal("unable to verify updates"))?;
                validator.validate_update(&first_update)?;
                match first_update.action() {
                    ActionV1::Add => to_add.push(filter.into()),
                    ActionV1::Remove => to_remove.push(filter.into()),
                };
                pending_updates += 1;
                validator
            } else {
                return Err(Status::invalid_argument(
                    "no valid session key filter for update",
                ));
            }
        } else {
            return Err(Status::invalid_argument("no session key filter provided"));
        };

        while let Ok(Some(update)) = request.message().await {
            validator.validate_update(&update)?;
            match (update.action(), update.filter) {
                (ActionV1::Add, Some(filter)) => to_add.push(filter.into()),
                (ActionV1::Remove, Some(filter)) => to_remove.push(filter.into()),
                _ => return Err(Status::invalid_argument("no filter provided")),
            };
            pending_updates += 1;
            if pending_updates >= UPDATE_BATCH_LIMIT {
                tracing::debug!(
                    adding = to_add.len(),
                    removing = to_remove.len(),
                    "update session key filters",
                );
                session_key::update_session_keys(
                    &to_add,
                    &to_remove,
                    &self.pool,
                    self.signing_key.clone(),
                    self.clone_update_channel(),
                )
                .await
                .map_err(|err| {
                    tracing::error!("session key update failed: {err:?}");
                    Status::internal("session key update failed")
                })?;
                to_add = vec![];
                to_remove = vec![];
                pending_updates = 0;
            }
        }

        if pending_updates > 0 {
            tracing::debug!(
                adding = to_add.len(),
                removing = to_remove.len(),
                "updating session key filters",
            );

            session_key::update_session_keys(
                &to_add,
                &to_remove,
                &self.pool,
                self.signing_key.clone(),
                self.clone_update_channel(),
            )
            .await
            .map_err(|err| {
                tracing::error!("session key update failed: {err:?}");
                Status::internal("session key update failed")
            })?;
        }

        let mut resp = SessionKeyFilterUpdateResV1 {
            timestamp: Utc::now().encode_timestamp(),
            signer: self.signing_key.public_key().into(),
            signature: vec![],
        };
        resp.signature = self.sign_response(&resp.encode_to_vec())?;

        Ok(Response::new(resp))
    }

    type streamStream = GrpcStreamResult<SessionKeyFilterStreamResV1>;
    async fn stream(
        &self,
        request: Request<SessionKeyFilterStreamReqV1>,
    ) -> GrpcResult<Self::streamStream> {
        let request = request.into_inner();

        let signer = self.verify_public_key(&request.signer)?;
        self.verify_stream_request_signature(&signer, &request)?;

        tracing::info!("client subscribed to session key stream");

        let pool = self.pool.clone();
        let shutdown_listener = self.shutdown.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let signing_key = self.signing_key.clone();

        let mut session_key_updates = self.subscribe_to_session_keys();

        tokio::spawn(async move {
            if stream_existing_skfs(&pool, signing_key, tx.clone())
                .await
                .is_err()
            {
                return;
            }

            tracing::info!("existing session keys sent; streaming updates as available");
            loop {
                let shutdown = shutdown_listener.clone();

                tokio::select! {
                    _ = shutdown => return,
                    msg = session_key_updates.recv() => if let Ok(update) = msg {
                        if tx.send(Ok(update)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}

async fn stream_existing_skfs(
    pool: &Pool<Postgres>,
    signing_key: Arc<Keypair>,
    tx: mpsc::Sender<Result<SessionKeyFilterStreamResV1, Status>>,
) -> Result<()> {
    let timestamp = Utc::now().encode_timestamp();
    let signer: Vec<u8> = signing_key.public_key().into();
    session_key::list_stream(pool)
        .then(|session_key_filter| {
            let mut skf_resp = SessionKeyFilterStreamResV1 {
                action: ActionV1::Add.into(),
                filter: Some(session_key_filter.into()),
                timestamp,
                signer: signer.clone(),
                signature: vec![],
            };

            futures::future::ready(signing_key.sign(&skf_resp.encode_to_vec()))
                .map_err(|_| anyhow!("failed signing session key filter"))
                .and_then(|signature| {
                    skf_resp.signature = signature;
                    tx.send(Ok(skf_resp))
                        .map_err(|_| anyhow!("failed sending session key filter"))
                })
        })
        .map_err(|err| anyhow!(err))
        .try_fold((), |acc, _| async move { Ok(acc) })
        .await
}

struct SkfValidator {
    oui: u64,
    constraints: Vec<DevAddrConstraint>,
    signing_keys: Vec<PublicKey>,
}

#[derive(thiserror::Error, Debug)]
enum SkfValidatorError {
    #[error("devaddr outside of constraint bounds {0}")]
    AddrOutOfBounds(String),
    #[error("wrong oui for session key filter {0}")]
    WrongOui(String),
    #[error("unauthorized signature {0}")]
    UnauthorizedSignature(String),
    #[error("invalid update {0}")]
    InvalidUpdate(String),
}

impl SkfValidator {
    async fn new(
        oui: u64,
        mut admin_keys: Vec<PublicKey>,
        db: impl sqlx::PgExecutor<'_> + Copy,
    ) -> Result<SkfValidator, DbOrgError> {
        let org = org::get_with_constraints(oui, db).await?;
        let mut org_keys = org::get_org_pubkeys(oui, db).await?;
        org_keys.append(&mut admin_keys);

        Ok(Self {
            oui,
            constraints: org.constraints,
            signing_keys: org_keys,
        })
    }

    fn validate_update<'a>(
        &'a mut self,
        request: &'a SessionKeyFilterUpdateReqV1,
    ) -> Result<(), Status> {
        validate_oui(request, self.oui)
            .and_then(|update| validate_constraint_bounds(update, self.constraints.as_ref()))
            .and_then(|update| validate_signature(update, &mut self.signing_keys))
            .map_err(|err| Status::invalid_argument(format!("{err:?}")))?;
        Ok(())
    }
}

fn validate_oui(
    update: &SessionKeyFilterUpdateReqV1,
    oui: u64,
) -> Result<&SessionKeyFilterUpdateReqV1, SkfValidatorError> {
    let filter_oui = if let Some(ref filter) = update.filter {
        filter.oui
    } else {
        return Err(SkfValidatorError::InvalidUpdate(format!("{update:?}")));
    };

    if oui == filter_oui {
        Ok(update)
    } else {
        Err(SkfValidatorError::WrongOui(format!(
            "authorized oui: {oui}, update: {filter_oui}"
        )))
    }
}

fn validate_constraint_bounds<'a>(
    update: &'a SessionKeyFilterUpdateReqV1,
    constraints: &'a Vec<DevAddrConstraint>,
) -> Result<&'a SessionKeyFilterUpdateReqV1, SkfValidatorError> {
    let filter_addr = if let Some(ref filter) = update.filter {
        filter.devaddr
    } else {
        return Err(SkfValidatorError::InvalidUpdate(format!("{update:?}")));
    };

    for constraint in constraints {
        if constraint.contains_addr(filter_addr.into()) {
            return Ok(update);
        }
    }
    Err(SkfValidatorError::AddrOutOfBounds(format!("{update:?}")))
}

fn validate_signature<'a, R>(
    request: &'a R,
    signing_keys: &mut [PublicKey],
) -> Result<&'a R, SkfValidatorError>
where
    R: MsgVerify + std::fmt::Debug,
{
    for (idx, pubkey) in signing_keys.iter().enumerate() {
        if request.verify(pubkey).is_ok() {
            signing_keys.swap(idx, 0);
            return Ok(request);
        }
    }
    Err(SkfValidatorError::UnauthorizedSignature(format!(
        "{request:?}"
    )))
}
