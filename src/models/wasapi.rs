use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Page<T> {
    pub count: u64,
    pub next: Option<String>,
    pub previous: Option<String>,
    pub files: Vec<T>,
    #[serde(rename = "includes-extra")]
    pub includes_extra: Option<bool>,
    #[serde(rename = "request-url")]
    pub request_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WasapiFile {
    pub filename: String,
    pub filetype: String,
    pub checksums: Checksums,
    pub account: u64,
    pub size: u64,
    pub collection: u64,
    pub crawl: u64,
    pub crawl_time: String,
    pub crawl_start: String,
    pub store_time: String,
    pub locations: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Checksums {
    pub sha1: String,
    pub md5: String,
}
