use futures_core::Stream;
use serde::Serialize;

use crate::http::Transport;
use crate::models::wasapi::{Page, WasapiFile};
use crate::{Config, Error};

pub struct WasapiClient {
    transport: Transport,
}

impl WasapiClient {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Result<Self, Error> {
        Self::with_config(username, password, Config::wasapi())
    }

    pub fn with_config(
        username: impl Into<String>,
        password: impl Into<String>,
        cfg: Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            transport: Transport::new(cfg, Some((username.into(), password.into())))?,
        })
    }

    pub async fn list_webdata(&self, query: &WebdataQuery) -> Result<Page<WasapiFile>, Error> {
        self.transport.get_json("webdata", query).await
    }

    pub async fn list_webdata_next(
        &self,
        prev: &Page<WasapiFile>,
    ) -> Result<Option<Page<WasapiFile>>, Error> {
        match prev.next.as_deref() {
            None => Ok(None),
            Some(url) => self.transport.get_json(url, &()).await.map(Some),
        }
    }

    pub fn webdata(
        &self,
        query: WebdataQuery,
    ) -> impl Stream<Item = Result<WasapiFile, Error>> + Send + '_ {
        async_stream::try_stream! {
            let mut page = self.list_webdata(&query).await?;
            loop {
                let files = std::mem::take(&mut page.files);
                for f in files { yield f; }
                match self.list_webdata_next(&page).await? {
                    Some(next) => page = next,
                    None => break,
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct WebdataQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filetype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crawl: Option<u64>,
    #[serde(rename = "crawl-time-after", skip_serializing_if = "Option::is_none")]
    pub crawl_time_after: Option<String>,
    #[serde(rename = "crawl-time-before", skip_serializing_if = "Option::is_none")]
    pub crawl_time_before: Option<String>,
    #[serde(rename = "crawl-start-after", skip_serializing_if = "Option::is_none")]
    pub crawl_start_after: Option<String>,
    #[serde(rename = "crawl-start-before", skip_serializing_if = "Option::is_none")]
    pub crawl_start_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
}
