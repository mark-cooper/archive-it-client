use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use futures_core::Stream;
use sha1::{Digest, Sha1};
use url::Url;

use crate::Error;
use crate::http::{Transport, is_retryable};
use crate::models::wasapi::WasapiFile;

pub(crate) mod local;
mod range;
pub(crate) mod s3;

const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

/// Renders a download destination for log lines.
///
/// One impl per [`Sink::Location`] — keeps `DownloadOutcome` generic
/// over destination while sharing a single `Display` impl.
pub trait DownloadLocation: Send + 'static {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl DownloadLocation for PathBuf {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

pub(crate) trait Sink: Send {
    type Location: Send + 'static;

    fn prepare(
        &mut self,
        file: &WasapiFile,
    ) -> impl Future<Output = Result<Prepared<Self::Location>, Error>> + Send;

    fn write_chunk(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), Error>> + Send;

    fn restart(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    fn finalize(self) -> impl Future<Output = Result<Self::Location, Error>> + Send;
}

pub(crate) enum Prepared<L> {
    /// Destination already holds this file. Engine yields `Skipped` and stops.
    Skip { location: L },
    /// Begin or resume; engine should fetch starting at `received` and continue
    /// hashing from `partial_sha1`. `received == file.size` is valid and means
    /// the engine skips the byte fetch and goes straight to finalize.
    Resume { received: u64, partial_sha1: Sha1 },
}

#[derive(Debug)]
pub(crate) enum DownloadEvent<L> {
    Progress {
        file: WasapiFile,
        received: u64,
        total: u64,
    },
    Skipped {
        file: WasapiFile,
        location: L,
    },
    Downloaded {
        file: WasapiFile,
        location: L,
        verified: bool,
    },
}

pub(crate) fn run_download<S>(
    transport: &Transport,
    url: Url,
    file: WasapiFile,
    sink: S,
) -> impl Stream<Item = Result<DownloadEvent<S::Location>, Error>> + Send + '_
where
    S: Sink + Send + 'static,
{
    async_stream::try_stream! {
        let expected_sha1 = file.checksums.sha1.clone();
        let mut sink = sink;

        let (mut received, mut hasher) = match sink.prepare(&file).await? {
            Prepared::Skip { location } => {
                yield DownloadEvent::Skipped { file, location };
                return;
            }
            Prepared::Resume { received, partial_sha1 } => (received, partial_sha1),
        };

        let mut last_progress: Option<Instant> = None;
        let mut attempts_left = transport.max_attempts();
        let mut delay = transport.backoff();

        if received > 0 && received < file.size {
            yield DownloadEvent::Progress {
                file: file.clone(),
                received,
                total: file.size,
            };
            last_progress = Some(Instant::now());
        }

        'download: while received < file.size {
            let mut response = transport.get_response_range(url.clone(), received).await?;

            if received > 0 {
                match response.status() {
                    reqwest::StatusCode::OK => {
                        sink.restart().await?;
                        received = 0;
                        hasher = Sha1::new();
                        attempts_left = transport.max_attempts();
                        delay = transport.backoff();
                    }
                    reqwest::StatusCode::PARTIAL_CONTENT => {
                        range::validate_content_range(&response, received, file.size, &url)?;
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

                sink.write_chunk(&chunk).await?;
                hasher.update(&chunk);
                received += chunk.len() as u64;
                attempts_left = transport.max_attempts();
                delay = transport.backoff();

                let emit = match last_progress {
                    None => true,
                    Some(t) => t.elapsed() >= PROGRESS_INTERVAL,
                };
                if emit {
                    yield DownloadEvent::Progress {
                        file: file.clone(),
                        received,
                        total: file.size,
                    };
                    last_progress = Some(Instant::now());
                }
            }
        }

        if received != file.size {
            Err(Error::SizeMismatch {
                url: url.to_string(),
                expected: file.size,
                actual: received,
            })?;
        }

        let verified = if let Some(expected) = expected_sha1.as_deref() {
            let actual = format!("{:x}", hasher.finalize());
            if actual != expected {
                Err(Error::ChecksumMismatch {
                    url: url.to_string(),
                    expected: expected.to_owned(),
                    actual,
                })?;
            }
            true
        } else {
            false
        };

        let location = sink.finalize().await?;
        yield DownloadEvent::Downloaded { file, location, verified };
    }
}
