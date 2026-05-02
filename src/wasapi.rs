use std::path::{Path, PathBuf};
use std::pin::pin;

use bytes::Bytes;
use futures_core::Stream;
use futures_util::TryStreamExt;
use serde::Serialize;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::http::Transport;
use crate::models::wasapi::{Page, WasapiFile};
use crate::{Config, Error};

pub const PRIMARY_LOCATION_SRC: &str = "https://warcs.archive-it.org";

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
        let expected = file.size;
        let (url, mut response) = self.download_response(&file).await?;
        let path = path.as_ref();
        let partial_path = partial_path(path)?;
        let mut out = tokio::fs::File::create(&partial_path).await?;
        let mut hasher = Sha1::new();

        while let Some(chunk) = response.chunk().await? {
            hasher.update(&chunk);
            out.write_all(&chunk).await?;
        }

        out.sync_all().await?;
        let actual_size = out.metadata().await?.len();
        drop(out);

        if actual_size != expected {
            return Err(Error::SizeMismatch {
                url: url.into(),
                expected,
                actual: actual_size,
            });
        }
        let actual_sha1 = format!("{:x}", hasher.finalize());
        if actual_sha1 != file.checksums.sha1 {
            return Err(Error::ChecksumMismatch {
                url: url.into(),
                expected: file.checksums.sha1,
                actual: actual_sha1,
            });
        }

        tokio::fs::rename(partial_path, path).await?;
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
                if existing_sha1_matches(&path, &file.checksums.sha1).await? {
                    yield DownloadOutcome::Skipped { file, path };
                    continue;
                }
                self.download(file.clone(), &path).await?;
                yield DownloadOutcome::Downloaded { file, path };
            }
        }
    }

    pub async fn download_stream(
        &self,
        file: WasapiFile,
    ) -> Result<impl Stream<Item = Result<Bytes, Error>> + Send + 'static, Error> {
        let (_, response) = self.download_response(&file).await?;
        Ok(response.bytes_stream().map_err(Error::from))
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

    async fn download_response(
        &self,
        file: &WasapiFile,
    ) -> Result<(Url, reqwest::Response), Error> {
        let url = self.primary_location_url(file)?;
        let response = self.transport.get_response(url.clone()).await?;
        Ok((url, response))
    }

    fn primary_location_url(&self, file: &WasapiFile) -> Result<Url, Error> {
        let location = file
            .locations
            .iter()
            .find(|location| location.starts_with(&self.primary_location_src))
            .ok_or_else(|| Error::PrimaryLocationMissing {
                filename: file.filename.clone(),
            })?;
        Ok(Url::parse(location)?)
    }
}

#[derive(Debug)]
pub enum DownloadOutcome {
    Downloaded { file: WasapiFile, path: PathBuf },
    Skipped { file: WasapiFile, path: PathBuf },
}

async fn existing_sha1_matches(path: &Path, expected: &str) -> Result<bool, Error> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(false);
    }
    let mut f = tokio::fs::File::open(path).await?;
    let mut hasher = Sha1::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()) == expected)
}

fn partial_path(path: &Path) -> Result<PathBuf, Error> {
    let mut file_name = path
        .file_name()
        .ok_or_else(|| Error::InvalidDownloadPath { path: path.into() })?
        .to_os_string();
    file_name.push(".part");
    Ok(path.with_file_name(file_name))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_path_appends_part_suffix() {
        let result = partial_path(Path::new("/tmp/foo.warc.gz")).unwrap();
        assert_eq!(result, PathBuf::from("/tmp/foo.warc.gz.part"));
    }

    #[test]
    fn partial_path_rejects_path_with_no_filename() {
        let err = partial_path(Path::new("/")).unwrap_err();
        assert!(matches!(err, Error::InvalidDownloadPath { .. }));
    }
}
