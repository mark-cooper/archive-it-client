use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::pin;

#[cfg(feature = "s3")]
use aws_sdk_s3::Client as AwsS3Client;
use futures_core::Stream;
use futures_util::TryStreamExt;
use serde::Serialize;
use url::Url;

use crate::downloads::local::LocalSink;
#[cfg(feature = "s3")]
use crate::downloads::s3::{S3Location, S3Sink};
use crate::downloads::{self, DownloadEvent, DownloadLocation};
use crate::http::Transport;
use crate::models::wasapi::{Page, WasapiFile};
use crate::{Config, Error};

pub const PRIMARY_LOCATION_SRC: &str = "https://warcs.archive-it.org";
pub const DEFAULT_WEBDATA_PAGE_SIZE: u32 = 50;

#[derive(Debug, Clone, Serialize)]
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

impl Default for WebdataQuery {
    fn default() -> Self {
        Self {
            filename: None,
            filetype: None,
            collection: None,
            crawl: None,
            crawl_time_after: None,
            crawl_time_before: None,
            crawl_start_after: None,
            crawl_start_before: None,
            page: None,
            page_size: Some(DEFAULT_WEBDATA_PAGE_SIZE),
        }
    }
}

pub struct WasapiClient {
    transport: Transport,
    primary_location_src: String,
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
            primary_location_src: PRIMARY_LOCATION_SRC.into(),
        })
    }

    pub fn with_primary_location_src(mut self, src: impl Into<String>) -> Self {
        self.primary_location_src = src.into();
        self
    }

    pub async fn download(&self, file: WasapiFile, path: impl AsRef<Path>) -> Result<(), Error> {
        let url = self.primary_location_url(&file)?;
        let sink = LocalSink::new(path.as_ref().to_path_buf())?;
        let mut events = pin!(downloads::run_download(&self.transport, url, file, sink));
        while events.try_next().await?.is_some() {}
        Ok(())
    }

    pub fn download_collection(
        &self,
        query: WebdataQuery,
        dir: impl Into<PathBuf>,
    ) -> impl Stream<Item = Result<DownloadOutcome, Error>> + Send + '_ {
        let dir = dir.into();
        async_stream::try_stream! {
            tokio::fs::create_dir_all(&dir).await?;
            let mut files = pin!(self.webdata(query));
            while let Some(file) = files.try_next().await? {
                let path = dir.join(&file.filename);
                let sink = match LocalSink::new(path) {
                    Ok(sink) => sink,
                    Err(error) => {
                        yield DownloadOutcome::Failed { file, error };
                        continue;
                    }
                };
                let url = match self.primary_location_url(&file) {
                    Ok(url) => url,
                    Err(error) => {
                        yield DownloadOutcome::Failed { file, error };
                        continue;
                    }
                };
                let file_for_error = file.clone();
                let mut events = pin!(downloads::run_download(&self.transport, url, file, sink));
                loop {
                    match events.try_next().await {
                        Ok(Some(event)) => yield outcome_from(event),
                        Ok(None) => break,
                        Err(error) => {
                            yield DownloadOutcome::Failed {
                                file: file_for_error,
                                error,
                            };
                            break;
                        }
                    }
                }
            }
        }
    }

    #[cfg(feature = "s3")]
    pub async fn download_to_s3(
        &self,
        file: WasapiFile,
        s3: AwsS3Client,
        target: S3Location,
    ) -> Result<(), Error> {
        let url = self.primary_location_url(&file)?;
        let sink = S3Sink::new(s3, target);
        let mut events = pin!(downloads::run_download(&self.transport, url, file, sink));
        while events.try_next().await?.is_some() {}
        Ok(())
    }

    #[cfg(feature = "s3")]
    pub fn download_collection_to_s3<K>(
        &self,
        query: WebdataQuery,
        s3: AwsS3Client,
        bucket: String,
        mut key_for: K,
    ) -> impl Stream<Item = Result<DownloadOutcome<S3Location>, Error>> + Send + '_
    where
        K: FnMut(&WasapiFile) -> String + Send + 'static,
    {
        async_stream::try_stream! {
            let mut files = pin!(self.webdata(query));
            while let Some(file) = files.try_next().await? {
                let target = S3Location {
                    bucket: bucket.clone(),
                    key: key_for(&file),
                };
                let sink = S3Sink::new(s3.clone(), target);
                let url = match self.primary_location_url(&file) {
                    Ok(url) => url,
                    Err(error) => {
                        yield DownloadOutcome::Failed { file, error };
                        continue;
                    }
                };
                let file_for_error = file.clone();
                let mut events = pin!(downloads::run_download(&self.transport, url, file, sink));
                loop {
                    match events.try_next().await {
                        Ok(Some(event)) => yield outcome_from(event),
                        Ok(None) => break,
                        Err(error) => {
                            yield DownloadOutcome::Failed {
                                file: file_for_error,
                                error,
                            };
                            break;
                        }
                    }
                }
            }
        }
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

    pub fn primary_location<'a>(&self, file: &'a WasapiFile) -> Option<&'a str> {
        file.locations
            .iter()
            .find(|location| location.starts_with(&self.primary_location_src))
            .map(String::as_str)
    }

    fn primary_location_url(&self, file: &WasapiFile) -> Result<Url, Error> {
        let location =
            self.primary_location(file)
                .ok_or_else(|| Error::PrimaryLocationMissing {
                    filename: file.filename.clone(),
                })?;
        Ok(Url::parse(location)?)
    }
}

#[derive(Debug)]
pub enum DownloadOutcome<L = PathBuf> {
    Downloaded {
        file: WasapiFile,
        location: L,
        verified: bool,
    },
    Failed {
        file: WasapiFile,
        error: Error,
    },
    Progress {
        file: WasapiFile,
        received: u64,
        total: u64,
    },
    Skipped {
        file: WasapiFile,
        location: L,
    },
}

impl<L: DownloadLocation> fmt::Display for DownloadOutcome<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Progress {
                file,
                received,
                total,
            } => {
                let pct = if *total == 0 {
                    100.0
                } else {
                    (*received as f64 / *total as f64) * 100.0
                };
                write!(
                    f,
                    "{}: {pct:.1}% ({received} / {total} bytes)",
                    file.filename
                )
            }
            Self::Downloaded {
                file,
                location,
                verified,
            } => {
                write!(f, "downloaded ")?;
                location.fmt_location(f)?;
                if *verified {
                    write!(f, " ({} bytes)", file.size)
                } else {
                    write!(f, " ({} bytes, unverified)", file.size)
                }
            }
            Self::Failed { file, error } => {
                write!(f, "failed {}: {error}", file.filename)
            }
            Self::Skipped { location, .. } => {
                write!(f, "skipped ")?;
                location.fmt_location(f)?;
                write!(f, " (already present)")
            }
        }
    }
}

fn outcome_from<L>(event: DownloadEvent<L>) -> DownloadOutcome<L> {
    match event {
        DownloadEvent::Progress {
            file,
            received,
            total,
        } => DownloadOutcome::Progress {
            file,
            received,
            total,
        },
        DownloadEvent::Skipped { file, location } => DownloadOutcome::Skipped { file, location },
        DownloadEvent::Downloaded {
            file,
            location,
            verified,
        } => DownloadOutcome::Downloaded {
            file,
            location,
            verified,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::wasapi::Checksums;

    #[test]
    fn primary_location_uses_configured_source_prefix() {
        let client = WasapiClient::new("u", "p")
            .unwrap()
            .with_primary_location_src("https://example.invalid");
        let file = WasapiFile {
            filename: "ARCHIVEIT-1.warc.gz".into(),
            filetype: "warc".into(),
            checksums: Checksums {
                sha1: Some("sha1".into()),
                md5: Some("md5".into()),
            },
            account: 1,
            size: 1,
            collection: 1,
            crawl: Some(1),
            crawl_time: Some("2020-01-01T00:00:00Z".into()),
            crawl_start: Some("2020-01-01T00:00:00Z".into()),
            store_time: "2020-01-01T00:00:00Z".into(),
            locations: vec![
                "https://other.example.com/warcs/foo.warc.gz".into(),
                "https://example.invalid/warcs/foo.warc.gz".into(),
            ],
        };

        assert_eq!(
            client.primary_location(&file),
            Some("https://example.invalid/warcs/foo.warc.gz")
        );
    }
}
