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

use crate::http::{Transport, is_retryable};
use crate::models::wasapi::{Page, WasapiFile};
use crate::{Config, Error};

pub const PRIMARY_LOCATION_SRC: &str = "https://warcs.archive-it.org";
pub const DEFAULT_WEBDATA_PAGE_SIZE: u32 = 50;

const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

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
                let expected_sha1 = self.required_sha1(&file)?;
                if existing_sha1_matches(&path, expected_sha1).await? {
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
        let url = self.primary_location_url(&file)?;
        let response = self.transport.get_response(url).await?;
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
            let expected_sha1 = self.required_sha1(&file)?;

            let mut state = match prepare_partial_download(&partial_path, file.size).await? {
                DownloadState::Complete { hasher } => {
                    let actual_sha1 = format!("{:x}", hasher.finalize());
                    if actual_sha1 != expected_sha1 {
                        Err(Error::ChecksumMismatch {
                            url: url.to_string(),
                            expected: expected_sha1.to_owned(),
                            actual: actual_sha1,
                        })?;
                    }
                    tokio::fs::rename(&partial_path, &path).await?;
                    yield DownloadOutcome::Downloaded { file, path };
                    return;
                }
                DownloadState::InProgress(state) => state,
            };

            let mut last_progress: Option<Instant> = None;
            let mut attempts_left = self.transport.max_attempts();
            let mut delay = self.transport.backoff();

            if state.received > 0 {
                yield DownloadOutcome::Progress {
                    file: file.clone(),
                    received: state.received,
                    total: file.size,
                };
                last_progress = Some(Instant::now());
            }

            'download: loop {
                let mut response = self
                    .transport
                    .get_response_range(url.clone(), state.received)
                    .await?;

                if state.received > 0 {
                    match response.status() {
                        reqwest::StatusCode::OK => {
                            state.restart(&partial_path).await?;
                            attempts_left = self.transport.max_attempts();
                            delay = self.transport.backoff();
                        }
                        reqwest::StatusCode::PARTIAL_CONTENT => {
                            validate_content_range(&response, state.received, file.size, &url)?;
                        }
                        status => {
                            Err(Error::InvalidRangeResponse {
                                url: url.to_string(),
                                details: format!("expected 200 or 206 for resume, got {status}"),
                            })?;
                        }
                    }
                }

                loop {
                    let chunk = match response.chunk().await {
                        Ok(Some(chunk)) => chunk,
                        Ok(None) => break 'download,
                        Err(e) => {
                            let err = Error::from(e);
                            if attempts_left > 1 && is_retryable(&err) {
                                attempts_left -= 1;
                                tokio::time::sleep(delay).await;
                                delay = delay.saturating_mul(2);
                                continue 'download;
                            }
                            Err(err)?;
                            unreachable!();
                        }
                    };

                    state.write_chunk(&chunk).await?;
                    attempts_left = self.transport.max_attempts();
                    delay = self.transport.backoff();

                    let emit = match last_progress {
                        None => true,
                        Some(t) => t.elapsed() >= PROGRESS_INTERVAL,
                    };
                    if emit {
                        yield DownloadOutcome::Progress {
                            file: file.clone(),
                            received: state.received,
                            total: file.size,
                        };
                        last_progress = Some(Instant::now());
                    }
                }
            }

            state
                .finalize(
                    &partial_path,
                    &path,
                    file.size,
                    expected_sha1,
                    &url,
                )
                .await?;
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

    fn required_sha1<'a>(&self, file: &'a WasapiFile) -> Result<&'a str, Error> {
        file.checksums
            .sha1
            .as_deref()
            .ok_or_else(|| Error::MissingChecksum {
                filename: file.filename.clone(),
                algorithm: "sha1",
            })
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

enum DownloadState {
    Complete { hasher: Sha1 },
    InProgress(PartialDownload),
}

struct PartialDownload {
    out: tokio::fs::File,
    received: u64,
    hasher: Sha1,
}

impl PartialDownload {
    async fn restart(&mut self, partial_path: &Path) -> Result<(), Error> {
        let replacement = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(partial_path)
            .await?;
        let previous = std::mem::replace(&mut self.out, replacement);
        drop(previous);
        self.received = 0;
        self.hasher = Sha1::new();
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        self.hasher.update(chunk);
        self.out.write_all(chunk).await?;
        self.received += chunk.len() as u64;
        Ok(())
    }

    async fn finalize(
        self,
        partial_path: &Path,
        final_path: &Path,
        expected_size: u64,
        expected_sha1: &str,
        url: &Url,
    ) -> Result<(), Error> {
        verify_and_finalize(
            self.out,
            partial_path,
            final_path,
            self.hasher,
            expected_size,
            expected_sha1,
            url,
        )
        .await
    }
}

async fn existing_sha1_matches(path: &Path, expected: &str) -> Result<bool, Error> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(false);
    }
    let mut hasher = Sha1::new();
    seed_hasher_from_file(path, &mut hasher).await?;
    Ok(format!("{:x}", hasher.finalize()) == expected)
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

async fn prepare_partial_download(
    partial_path: &Path,
    expected_size: u64,
) -> Result<DownloadState, Error> {
    let (received, hasher) = examine_partial(partial_path, expected_size).await?;
    if received == expected_size {
        return Ok(DownloadState::Complete { hasher });
    }
    let out = if received > 0 {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(partial_path)
            .await?
    } else {
        tokio::fs::File::create(partial_path).await?
    };
    Ok(DownloadState::InProgress(PartialDownload {
        out,
        received,
        hasher,
    }))
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

fn partial_path(path: &Path) -> Result<PathBuf, Error> {
    let mut file_name = path
        .file_name()
        .ok_or_else(|| Error::InvalidDownloadPath { path: path.into() })?
        .to_os_string();
    file_name.push(".part");
    Ok(path.with_file_name(file_name))
}

fn validate_content_range(
    response: &reqwest::Response,
    expected_start: u64,
    expected_total: u64,
    url: &Url,
) -> Result<(), Error> {
    let header = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: "missing Content-Range header on 206 response".into(),
        })?;
    let value = header.to_str().map_err(|_| Error::InvalidRangeResponse {
        url: url.to_string(),
        details: "invalid Content-Range header encoding".into(),
    })?;
    let range = value
        .strip_prefix("bytes ")
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("unexpected Content-Range format: {value}"),
        })?;
    let (bounds, total) = range
        .split_once('/')
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("unexpected Content-Range format: {value}"),
        })?;
    let (start, _) = bounds
        .split_once('-')
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("unexpected Content-Range format: {value}"),
        })?;
    let start = start
        .parse::<u64>()
        .map_err(|_| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("invalid Content-Range start: {value}"),
        })?;
    let total = total
        .parse::<u64>()
        .map_err(|_| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("invalid Content-Range total: {value}"),
        })?;

    if start != expected_start {
        return Err(Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("Content-Range starts at {start}, expected {expected_start}"),
        });
    }
    if total != expected_total {
        return Err(Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("Content-Range total is {total}, expected {expected_total}"),
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::wasapi::Checksums;

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
