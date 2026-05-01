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

    pub async fn list_webdata(&self, collection_id: u64) -> Result<Page<WasapiFile>, Error> {
        #[derive(Serialize)]
        struct Q {
            collection: u64,
        }
        self.transport
            .get_json(
                "webdata",
                &Q {
                    collection: collection_id,
                },
            )
            .await
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
        collection_id: u64,
    ) -> impl Stream<Item = Result<WasapiFile, Error>> + Send + '_ {
        async_stream::try_stream! {
            let mut page = self.list_webdata(collection_id).await?;
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
