// model representing the JSON payload returned from:
// https://api.github.com/repos/helium/denylist/releases/latest

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DenyListMetaData {
    url: String,
    assets_url: String,
    upload_url: String,
    html_url: String,
    id: i64,
    author: Item,
    pnode_id: String,
    pub tag_name: String,
    target_commitish: String,
    name: String,
    draft: bool,
    prerelease: bool,
    created_at: String,
    published_at: String,
    pub assets: Vec<Asset>,
    tarball_url: String,
    zipball_url: String,
    body: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Asset {
    url: String,
    id: i64,
    node_id: String,
    pub name: String,
    label: String,
    uploader: Item,
    content_type: String,
    state: String,
    size: i64,
    download_count: i64,
    created_at: String,
    updated_at: String,
    pub browser_download_url: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Item {
    login: String,
    id: i64,
    node_id: String,
    avatar_url: String,
    gravatar_id: String,
    url: String,
    html_url: String,
    followers_url: String,
    following_url: String,
    gists_url: String,
    starred_url: String,
    subscriptions_url: String,
    organizations_url: String,
    repos_url: String,
    events_url: String,
    received_events_url: String,
    #[serde(rename = "type")]
    r#type: String,
    site_admin: bool,
}
