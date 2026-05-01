use serde::Deserialize;

use super::Metadata;

#[derive(Debug, Clone, Deserialize)]
pub struct PublicAccount {
    pub id: u64,
    pub organization_name: String,
    pub partner_description: Option<String>,
    pub logo_blob: Option<u64>,
    pub partner_url: String,
    pub custom_crawl_schedules_visible: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PublicCollection {
    pub id: u64,
    pub account: u64,
    pub name: String,
    pub state: String,
    pub publicly_visible: bool,
    pub deleted: bool,
    pub oai_exported: bool,
    pub topics: Option<String>,
    pub image: Option<u64>,
    pub created_date: String,
    pub last_updated_date: String,
    pub last_crawl_date: Option<String>,
    pub num_active_seeds: u64,
    pub num_inactive_seeds: u64,
    pub total_warc_bytes: u64,
    pub metadata: Option<Metadata>,
}
