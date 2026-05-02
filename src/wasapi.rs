use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::{Duration, Instant};

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

const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

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
        let mut stream = pin!(self.download_with_progress(file, path));
        while stream.try_next().await?.is_some() {}
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
                let mut inner = pin!(self.download_with_progress(file, path));
                while let Some(event) = inner.try_next().await? {
                    yield event;
                }
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

    pub fn download_with_progress(
        &self,
        file: WasapiFile,
        path: impl AsRef<Path>,
    ) -> impl Stream<Item = Result<DownloadOutcome, Error>> + Send + '_ {
        let path = path.as_ref().to_path_buf();
        async_stream::try_stream! {
            let url = self.primary_location_url(&file)?;
            let partial_path = partial_path(&path)?;

            let (mut resume_from, mut hasher) = examine_partial(&partial_path, file.size).await?;

            if resume_from == file.size {
                let actual_sha1 = format!("{:x}", hasher.finalize());
                if actual_sha1 != file.checksums.sha1 {
                    Err(Error::ChecksumMismatch {
                        url: url.to_string(),
                        expected: file.checksums.sha1.clone(),
                        actual: actual_sha1,
                    })?;
                }
                tokio::fs::rename(&partial_path, &path).await?;
                yield DownloadOutcome::Downloaded { file, path };
                return;
            }

            let mut response = self.transport.get_response_range(url.clone(), resume_from).await?;

            if resume_from > 0 && response.status() == reqwest::StatusCode::OK {
                tokio::fs::remove_file(&partial_path).await?;
                resume_from = 0;
                hasher = Sha1::new();
            }

            let mut out = if resume_from > 0 {
                tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&partial_path)
                    .await?
            } else {
                tokio::fs::File::create(&partial_path).await?
            };

            let mut received: u64 = resume_from;
            let mut last_progress: Option<Instant> = None;

            if resume_from > 0 {
                yield DownloadOutcome::Progress {
                    file: file.clone(),
                    received,
                    total: file.size,
                };
                last_progress = Some(Instant::now());
            }

            while let Some(chunk) = response.chunk().await? {
                hasher.update(&chunk);
                out.write_all(&chunk).await?;
                received += chunk.len() as u64;
                let emit = match last_progress {
                    None => true,
                    Some(t) => t.elapsed() >= PROGRESS_INTERVAL,
                };
                if emit {
                    yield DownloadOutcome::Progress {
                        file: file.clone(),
                        received,
                        total: file.size,
                    };
                    last_progress = Some(Instant::now());
                }
            }

            verify_and_finalize(
                out,
                &partial_path,
                &path,
                hasher,
                file.size,
                &file.checksums.sha1,
                &url,
            ).await?;
            yield DownloadOutcome::Downloaded { file, path };
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
    Downloaded {
        file: WasapiFile,
        path: PathBuf,
    },
    Progress {
        file: WasapiFile,
        received: u64,
        total: u64,
    },
    Skipped {
        file: WasapiFile,
        path: PathBuf,
    },
}

async fn existing_sha1_matches(path: &Path, expected: &str) -> Result<bool, Error> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(false);
    }
    let mut hasher = Sha1::new();
    seed_hasher_from_file(path, &mut hasher).await?;
    Ok(format!("{:x}", hasher.finalize()) == expected)
}

async fn seed_hasher_from_file(path: &Path, hasher: &mut Sha1) -> Result<(), Error> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(())
}

async fn examine_partial(partial_path: &Path, expected_size: u64) -> Result<(u64, Sha1), Error> {
    if !tokio::fs::try_exists(partial_path).await? {
        return Ok((0, Sha1::new()));
    }
    let m = tokio::fs::metadata(partial_path).await?;
    if m.len() > expected_size {
        tokio::fs::remove_file(partial_path).await?;
        return Ok((0, Sha1::new()));
    }
    let mut hasher = Sha1::new();
    if m.len() > 0 {
        seed_hasher_from_file(partial_path, &mut hasher).await?;
    }
    Ok((m.len(), hasher))
}

fn partial_path(path: &Path) -> Result<PathBuf, Error> {
    let mut file_name = path
        .file_name()
        .ok_or_else(|| Error::InvalidDownloadPath { path: path.into() })?
        .to_os_string();
    file_name.push(".part");
    Ok(path.with_file_name(file_name))
}

async fn verify_and_finalize(
    out: tokio::fs::File,
    partial_path: &Path,
    final_path: &Path,
    hasher: Sha1,
    expected_size: u64,
    expected_sha1: &str,
    url: &Url,
) -> Result<(), Error> {
    out.sync_all().await?;
    let actual_size = out.metadata().await?.len();
    drop(out);
    if actual_size != expected_size {
        return Err(Error::SizeMismatch {
            url: url.to_string(),
            expected: expected_size,
            actual: actual_size,
        });
    }
    let actual_sha1 = format!("{:x}", hasher.finalize());
    if actual_sha1 != expected_sha1 {
        return Err(Error::ChecksumMismatch {
            url: url.to_string(),
            expected: expected_sha1.to_string(),
            actual: actual_sha1,
        });
    }
    tokio::fs::rename(partial_path, final_path).await?;
    Ok(())
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
