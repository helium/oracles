use std::{net::SocketAddr, str::FromStr, sync::Arc};

use backon::{ExponentialBuilder, Retryable};
use chrono::Utc;
use futures::{Future, StreamExt, TryFutureExt};
use helium_crypto::{KeyTag, KeyType as CryptoKeyType, Keypair, Network, PublicKey, Sign};
use helium_proto::services::iot_config::{
    self as proto, config_org_client::OrgClient, config_route_client::RouteClient, RouteStreamReqV1,
};
use iot_config::{
    admin::{AuthCache, KeyType},
    org::{self},
    OrgService, RouteService,
};
use prost::Message;
use rand::{rngs::OsRng, Rng};
use sqlx::{Pool, Postgres};
use tokio::task::JoinHandle;
use tonic::{
    transport::{self, Channel},
    Streaming,
};

#[sqlx::test]
async fn stream_sends_all_data_when_since_is_0(pool: Pool<Postgres>) {
    let signing_keypair = Arc::new(generate_keypair());
    let admin_keypair = generate_keypair();
    let client_keypair = generate_keypair();

    let port = get_port();

    let auth_cache = create_auth_cache(
        admin_keypair.public_key().clone(),
        client_keypair.public_key().clone(),
        &pool,
    )
    .await;

    let _handle = start_server(port, signing_keypair, auth_cache, pool.clone()).await;
    let mut client = connect_client(port).await;

    let org = create_org(port, &admin_keypair).await;
    let route = create_route(&mut client, &org.org.unwrap(), &admin_keypair).await;

    create_euis(
        &mut client,
        &route,
        vec![(200, 201), (202, 203)],
        &admin_keypair,
    )
    .await;

    let constraint = org.devaddr_constraints.first().unwrap();

    create_devaddr_ranges(
        &mut client,
        &route,
        vec![
            (constraint.start_addr, constraint.start_addr + 1),
            (constraint.start_addr + 2, constraint.start_addr + 3),
        ],
        &admin_keypair,
    )
    .await;

    create_skf(
        &mut client,
        &route,
        vec![(constraint.start_addr, "key-1")],
        &admin_keypair,
    )
    .await;

    let response = client
        .stream(route_stream_req_v1(&client_keypair, 0))
        .await
        .expect("stream request");
    let mut response_stream = response.into_inner();

    assert_route_received(&mut response_stream, proto::ActionV1::Add, &route.id).await;
    assert_eui_pair(
        &mut response_stream,
        proto::ActionV1::Add,
        &route.id,
        200,
        201,
    )
    .await;
    assert_eui_pair(
        &mut response_stream,
        proto::ActionV1::Add,
        &route.id,
        202,
        203,
    )
    .await;

    assert_devaddr_range(
        &mut response_stream,
        proto::ActionV1::Add,
        &route.id,
        constraint.start_addr,
        constraint.start_addr + 1,
    )
    .await;

    assert_devaddr_range(
        &mut response_stream,
        proto::ActionV1::Add,
        &route.id,
        constraint.start_addr + 2,
        constraint.start_addr + 3,
    )
    .await;

    assert_skf(
        &mut response_stream,
        proto::ActionV1::Add,
        &route.id,
        constraint.start_addr,
        "key-1",
    )
    .await;
}

#[sqlx::test]
async fn stream_only_sends_data_modified_since(pool: Pool<Postgres>) {
    let signing_keypair = Arc::new(generate_keypair());
    let admin_keypair = generate_keypair();
    let client_keypair = generate_keypair();

    let port = get_port();

    let auth_cache = create_auth_cache(
        admin_keypair.public_key().clone(),
        client_keypair.public_key().clone(),
        &pool,
    )
    .await;

    let _handle = start_server(port, signing_keypair, auth_cache, pool.clone()).await;
    let mut client = connect_client(port).await;

    let org_res_v1 = create_org(port, &admin_keypair).await;

    let proto::OrgResV1 { org: Some(org), .. } = org_res_v1 else {
        panic!("invalid OrgResV1")
    };

    let route1 = create_route(&mut client, &org, &admin_keypair).await;

    create_euis(&mut client, &route1, vec![(200, 201)], &admin_keypair).await;

    let constraint = org_res_v1.devaddr_constraints.first().unwrap();
    create_devaddr_ranges(
        &mut client,
        &route1,
        vec![(constraint.start_addr, constraint.start_addr + 1)],
        &admin_keypair,
    )
    .await;

    create_skf(
        &mut client,
        &route1,
        vec![(constraint.start_addr, "key-1")],
        &admin_keypair,
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let since = Utc::now();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let route2 = create_route(&mut client, &org, &admin_keypair).await;

    create_euis(&mut client, &route1, vec![(202, 203)], &admin_keypair).await;

    create_devaddr_ranges(
        &mut client,
        &route1,
        vec![(constraint.start_addr + 2, constraint.start_addr + 3)],
        &admin_keypair,
    )
    .await;

    create_skf(
        &mut client,
        &route1,
        vec![(constraint.start_addr + 2, "key-2")],
        &admin_keypair,
    )
    .await;

    let response = client
        .stream(route_stream_req_v1(
            &client_keypair,
            since.timestamp() as u64,
        ))
        .await
        .expect("stream request");
    let mut response_stream = response.into_inner();

    assert_route_received(&mut response_stream, proto::ActionV1::Add, &route2.id).await;

    assert_eui_pair(
        &mut response_stream,
        proto::ActionV1::Add,
        &route1.id,
        202,
        203,
    )
    .await;

    assert_devaddr_range(
        &mut response_stream,
        proto::ActionV1::Add,
        &route1.id,
        constraint.start_addr + 2,
        constraint.start_addr + 3,
    )
    .await;

    assert_skf(
        &mut response_stream,
        proto::ActionV1::Add,
        &route1.id,
        constraint.start_addr + 2,
        "key-2",
    )
    .await;
}

async fn assert_route_received(
    stream: &mut Streaming<proto::RouteStreamResV1>,
    expected_action: proto::ActionV1,
    expected_id: &str,
) {
    let Ok(proto::RouteStreamResV1 {
        action,
        data: Some(proto::route_stream_res_v1::Data::Route(streamed_route)),
        ..
    }) = receive(stream.next()).await
    else {
        panic!("message not correct format")
    };

    assert_eq!(action, expected_action as i32);
    assert_eq!(&streamed_route.id, expected_id);
}

async fn assert_eui_pair(
    stream: &mut Streaming<proto::RouteStreamResV1>,
    expected_action: proto::ActionV1,
    expected_id: &str,
    expected_app_eui: u64,
    expected_dev_eui: u64,
) {
    let Ok(proto::RouteStreamResV1 {
        action,
        data: Some(proto::route_stream_res_v1::Data::EuiPair(streamed_pair)),
        ..
    }) = receive(stream.next()).await
    else {
        panic!("message not correct format")
    };

    assert_eq!(action, expected_action as i32);
    assert_eq!(streamed_pair.route_id, expected_id);
    assert_eq!(streamed_pair.app_eui, expected_app_eui);
    assert_eq!(streamed_pair.dev_eui, expected_dev_eui);
}

async fn assert_devaddr_range(
    stream: &mut Streaming<proto::RouteStreamResV1>,
    expected_action: proto::ActionV1,
    expected_route_id: &str,
    expected_start: u32,
    expected_end: u32,
) {
    let Ok(proto::RouteStreamResV1 {
        action,
        data: Some(proto::route_stream_res_v1::Data::DevaddrRange(range)),
        ..
    }) = receive(stream.next()).await
    else {
        panic!("message not in correct format")
    };

    assert_eq!(action, expected_action as i32);
    assert_eq!(range.route_id, expected_route_id);
    assert_eq!(range.start_addr, expected_start);
    assert_eq!(range.end_addr, expected_end);
}

async fn assert_skf(
    stream: &mut Streaming<proto::RouteStreamResV1>,
    expected_action: proto::ActionV1,
    expected_route_id: &str,
    expected_devaddr: u32,
    expected_session_key: &str,
) {
    let Ok(proto::RouteStreamResV1 {
        action,
        data: Some(proto::route_stream_res_v1::Data::Skf(skf)),
        ..
    }) = receive(stream.next()).await
    else {
        panic!("message not in received")
    };

    assert_eq!(action, expected_action as i32);
    assert_eq!(skf.route_id, expected_route_id);
    assert_eq!(skf.devaddr, expected_devaddr);
    assert_eq!(skf.session_key, expected_session_key);
}

async fn receive<F, T>(future: F) -> T
where
    F: Future<Output = Option<T>>,
    T: std::fmt::Debug,
{
    match tokio::time::timeout(std::time::Duration::from_secs(5), future).await {
        Ok(Some(t)) => {
            dbg!(&t);
            t
        }
        _other => panic!("message was not received within 5 seconds"),
    }
}

fn route_stream_req_v1(signer: &Keypair, since: u64) -> RouteStreamReqV1 {
    let mut request = RouteStreamReqV1 {
        timestamp: Utc::now().timestamp() as u64,
        signer: signer.public_key().to_vec(),
        since,
        signature: vec![],
    };

    request.signature = signer.sign(&request.encode_to_vec()).expect("sign");

    request
}

async fn connect_client(port: u64) -> RouteClient<Channel> {
    (|| RouteClient::connect(format!("http://127.0.0.1:{port}")))
        .retry(&ExponentialBuilder::default())
        .await
        .expect("grpc client")
}

async fn start_server(
    port: u64,
    signing_keypair: Arc<Keypair>,
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
) -> JoinHandle<anyhow::Result<()>> {
    let (delegate_key_updater, _delegate_key_cache) = org::delegate_keys_cache(&pool)
        .await
        .expect("delete keys cache");

    let route_service =
        RouteService::new(signing_keypair.clone(), auth_cache.clone(), pool.clone());

    let org_service = OrgService::new(
        signing_keypair.clone(),
        auth_cache.clone(),
        pool.clone(),
        route_service.clone_update_channel(),
        delegate_key_updater,
    )
    .expect("org service");

    tokio::spawn(
        transport::Server::builder()
            .add_service(proto::OrgServer::new(org_service))
            .add_service(proto::RouteServer::new(route_service))
            .serve(socket_addr(port).expect("socket addr"))
            .map_err(anyhow::Error::from),
    )
}

fn socket_addr(port: u64) -> anyhow::Result<SocketAddr> {
    SocketAddr::from_str(&format!("127.0.0.1:{port}")).map_err(anyhow::Error::from)
}

fn generate_keypair() -> Keypair {
    Keypair::generate(
        KeyTag {
            network: Network::MainNet,
            key_type: CryptoKeyType::Ed25519,
        },
        &mut OsRng,
    )
}

fn get_port() -> u64 {
    rand::thread_rng().gen_range(6000..10000)
}

async fn create_org(port: u64, admin_keypair: &Keypair) -> proto::OrgResV1 {
    let mut client = (|| OrgClient::connect(format!("http://127.0.0.1:{port}")))
        .retry(&ExponentialBuilder::default())
        .await
        .expect("org client");

    let mut request = proto::OrgCreateHeliumReqV1 {
        owner: generate_keypair().public_key().to_vec(),
        payer: generate_keypair().public_key().to_vec(),
        devaddrs: 8,
        timestamp: Utc::now().timestamp() as u64,
        signature: vec![],
        delegate_keys: vec![],
        signer: admin_keypair.public_key().into(),
        net_id: 6,
    };

    request.signature = admin_keypair
        .sign(&request.encode_to_vec())
        .expect("sign create org");

    let response = client.create_helium(request).await;

    let proto::OrgResV1 { org: Some(org), .. } = response.unwrap().into_inner() else {
        panic!("org response is incorrect")
    };

    let Ok(response) = client.get(proto::OrgGetReqV1 { oui: org.oui }).await else {
        panic!("could not get the org")
    };

    response.into_inner()
}

async fn create_route(
    client: &mut RouteClient<Channel>,
    org: &proto::OrgV1,
    signing_keypair: &Keypair,
) -> proto::RouteV1 {
    let mut request = proto::RouteCreateReqV1 {
        oui: org.oui,
        route: Some(proto::RouteV1 {
            id: "".to_string(),
            net_id: 11,
            oui: org.oui,
            server: Some(proto::ServerV1 {
                host: "hostname".to_string(),
                port: 8080,
                protocol: Some(proto::server_v1::Protocol::PacketRouter(
                    proto::ProtocolPacketRouterV1 {},
                )),
            }),
            max_copies: 1,
            active: true,
            locked: false,
            ignore_empty_skf: true,
        }),
        timestamp: Utc::now().timestamp() as u64,
        signature: vec![],
        signer: signing_keypair.public_key().into(),
    };

    request.signature = signing_keypair
        .sign(&request.encode_to_vec())
        .expect("sign create route");

    let response = client.create(request).await;

    let proto::RouteResV1 {
        route: Some(route), ..
    } = response.unwrap().into_inner()
    else {
        panic!("incorrect route response")
    };

    route
}

async fn create_euis(
    client: &mut RouteClient<Channel>,
    route: &proto::RouteV1,
    pairs: Vec<(u64, u64)>,
    signing_keypair: &Keypair,
) {
    let requests = pairs
        .into_iter()
        .map(|(a, d)| proto::EuiPairV1 {
            route_id: route.id.clone(),
            app_eui: a,
            dev_eui: d,
        })
        .map(|pair| {
            let mut request = proto::RouteUpdateEuisReqV1 {
                action: proto::ActionV1::Add as i32,
                eui_pair: Some(pair),
                timestamp: Utc::now().timestamp() as u64,
                signature: vec![],
                signer: signing_keypair.public_key().into(),
            };

            request.signature = signing_keypair
                .sign(&request.encode_to_vec())
                .expect("sign");

            request
        })
        .collect::<Vec<_>>();

    let Ok(_) = client.update_euis(futures::stream::iter(requests)).await else {
        panic!("unable to create eui pairs")
    };
}

async fn create_devaddr_ranges(
    client: &mut RouteClient<Channel>,
    route: &proto::RouteV1,
    ranges: Vec<(u32, u32)>,
    signing_keypair: &Keypair,
) {
    let requests = ranges
        .into_iter()
        .map(|(s, e)| proto::DevaddrRangeV1 {
            route_id: route.id.clone(),
            start_addr: s,
            end_addr: e,
        })
        .map(|range| {
            let mut request = proto::RouteUpdateDevaddrRangesReqV1 {
                action: proto::ActionV1::Add as i32,
                devaddr_range: Some(range),
                timestamp: Utc::now().timestamp() as u64,
                signature: vec![],
                signer: signing_keypair.public_key().into(),
            };

            request.signature = signing_keypair
                .sign(&request.encode_to_vec())
                .expect("sign");

            request
        })
        .collect::<Vec<_>>();

    let Ok(_) = client
        .update_devaddr_ranges(futures::stream::iter(requests))
        .await
    else {
        panic!("unable to create devaddr ranges")
    };
}

async fn create_skf(
    client: &mut RouteClient<Channel>,
    route: &proto::RouteV1,
    updates: Vec<(u32, &str)>,
    signing_keypair: &Keypair,
) {
    let updates = updates
        .into_iter()
        .map(
            |(devaddr, session_key)| proto::route_skf_update_req_v1::RouteSkfUpdateV1 {
                devaddr,
                session_key: session_key.to_string(),
                action: proto::ActionV1::Add as i32,
                max_copies: 1,
            },
        )
        .collect::<Vec<_>>();

    let mut request = proto::RouteSkfUpdateReqV1 {
        route_id: route.id.clone(),
        updates,
        timestamp: Utc::now().timestamp() as u64,
        signature: vec![],
        signer: signing_keypair.public_key().into(),
    };

    request.signature = signing_keypair
        .sign(&request.encode_to_vec())
        .expect("sign");

    let Ok(_) = client.update_skfs(request).await else {
        panic!("unable to create skf")
    };
}

async fn create_auth_cache(
    admin_public_key: PublicKey,
    client_public_key: PublicKey,
    pool: &Pool<Postgres>,
) -> AuthCache {
    let (auth_updater, auth_cache) = AuthCache::new(admin_public_key, pool)
        .await
        .expect("new auth cache");

    auth_updater.send_modify(|cache| {
        cache.insert(client_public_key, KeyType::PacketRouter);
    });

    auth_cache
}
