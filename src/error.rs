use std::io;
use std::path::PathBuf;

use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sha1 mismatch for {url}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        url: String,
        expected: String,
        actual: String,
    },
    #[error("authenticated account list was empty")]
    Empty,
    #[error("download path has no file name: {}", .path.display())]
    InvalidDownloadPath { path: PathBuf },
    #[error("invalid range response for {url}: {details}")]
    InvalidRangeResponse { url: String, details: String },
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("no primary WARC location for {filename}")]
    PrimaryLocationMissing { filename: String },
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("downloaded {actual} bytes from {url}; expected {expected}")]
    SizeMismatch {
        url: String,
        expected: u64,
        actual: u64,
    },
    #[error("unexpected status: {0}")]
    Status(StatusCode),
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),
}
