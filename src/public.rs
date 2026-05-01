use futures_core::Stream;
use serde::Serialize;

use crate::http::{Transport, paginated};
use crate::models::public::{PublicAccount, PublicCollection};
use crate::{Config, Error, PageOpts};

pub struct PublicClient {
    transport: Transport,
}

impl PublicClient {
    pub fn new() -> Result<Self, Error> {
        Self::with_config(Config::api())
    }

    pub fn with_config(cfg: Config) -> Result<Self, Error> {
        Ok(Self {
            transport: Transport::new(cfg, None)?,
        })
    }

    pub async fn list_accounts(&self, opts: PageOpts) -> Result<Vec<PublicAccount>, Error> {
        self.transport.get_json("account", &opts).await
    }

    pub async fn get_account(&self, id: u64) -> Result<PublicAccount, Error> {
        self.transport.get_json(&format!("account/{id}"), &()).await
    }

    pub async fn list_collections(
        &self,
        account_id: Option<u64>,
        opts: PageOpts,
    ) -> Result<Vec<PublicCollection>, Error> {
        self.transport
            .get_json("collection", &CollectionsQuery::new(account_id, opts))
            .await
    }

    pub async fn get_collection(&self, id: u64) -> Result<PublicCollection, Error> {
        self.transport
            .get_json(&format!("collection/{id}"), &())
            .await
    }

    pub fn accounts(&self) -> impl Stream<Item = Result<PublicAccount, Error>> + Send + '_ {
        paginated(move |opts| self.list_accounts(opts))
    }

    pub fn collections(
        &self,
        account_id: Option<u64>,
    ) -> impl Stream<Item = Result<PublicCollection, Error>> + Send + '_ {
        paginated(move |opts| self.list_collections(account_id, opts))
    }
}

#[derive(Serialize)]
pub(crate) struct CollectionsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) account: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) offset: Option<u32>,
}

impl CollectionsQuery {
    pub(crate) fn new(account: Option<u64>, opts: PageOpts) -> Self {
        Self {
            account,
            limit: opts.limit,
            offset: opts.offset,
        }
    }
}
