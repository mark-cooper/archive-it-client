#![doc = include_str!("../README.md")]

use std::time::Duration;

mod error;
mod http;
pub mod models;
pub mod partner;
pub mod public;
pub mod wasapi;

pub use error::Error;
pub use partner::PartnerClient;
pub use public::PublicClient;
pub use wasapi::{DownloadOutcome, WasapiClient, WebdataQuery};

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct PageOpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base_url: String,
    pub timeout: Duration,
    pub download_timeout: Duration,
    pub max_attempts: u32,
    pub backoff: Duration,
}

impl Config {
    pub fn api() -> Self {
        Self {
            base_url: "https://partner.archive-it.org/api/".into(),
            timeout: Duration::from_secs(30),
            download_timeout: Duration::from_secs(30),
            max_attempts: 3,
            backoff: Duration::from_millis(250),
        }
    }

    pub fn wasapi() -> Self {
        Self {
            base_url: "https://partner.archive-it.org/wasapi/v1/".into(),
            timeout: Duration::from_secs(60),
            download_timeout: Duration::from_secs(300),
            ..Self::api()
        }
    }
}
