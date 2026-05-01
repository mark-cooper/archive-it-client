use futures_core::Stream;
use tokio::sync::OnceCell;

use crate::http::{Transport, paginated};
use crate::models::partner::{Account, Collection};
use crate::public::CollectionsQuery;
use crate::{Config, Error, PageOpts};

pub struct PartnerClient {
    transport: Transport,
    self_id: OnceCell<u64>,
}

impl PartnerClient {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Result<Self, Error> {
        Self::with_config(username, password, Config::api())
    }

    pub fn with_config(
        username: impl Into<String>,
        password: impl Into<String>,
        cfg: Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            transport: Transport::new(cfg, Some((username.into(), password.into())))?,
            self_id: OnceCell::new(),
        })
    }

    pub async fn my_account(&self) -> Result<Account, Error> {
        let accounts: Vec<Account> = self.transport.get_json("account", &()).await?;
        accounts.into_iter().next().ok_or(Error::Empty)
    }

    pub async fn list_collections(&self, opts: PageOpts) -> Result<Vec<Collection>, Error> {
        let me = self.cached_self_id().await?;
        self.transport
            .get_json("collection", &CollectionsQuery::new(Some(me), opts))
            .await
    }

    pub fn collections(&self) -> impl Stream<Item = Result<Collection, Error>> + Send + '_ {
        paginated(move |opts| self.list_collections(opts))
    }

    pub async fn get_collection(&self, id: u64) -> Result<Collection, Error> {
        self.transport
            .get_json(&format!("collection/{id}"), &())
            .await
    }

    async fn cached_self_id(&self) -> Result<u64, Error> {
        self.self_id
            .get_or_try_init(|| async { self.my_account().await.map(|a| a.id) })
            .await
            .copied()
    }
}
