use std::io;
use std::path::PathBuf;

use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("no primary WARC location for {filename}")]
    PrimaryLocationMissing { filename: String },
    #[error("download path has no file name: {}", .path.display())]
    InvalidDownloadPath { path: PathBuf },
    #[error("downloaded {actual} bytes from {url}; expected {expected}")]
    SizeMismatch {
        url: String,
        expected: u64,
        actual: u64,
    },
    #[error("sha1 mismatch for {url}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        url: String,
        expected: String,
        actual: String,
    },
    #[error("unexpected status: {0}")]
    Status(StatusCode),
    #[error("authenticated account list was empty")]
    Empty,
}
