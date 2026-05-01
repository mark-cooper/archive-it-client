use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("resource not found")]
    NotFound,
    #[error("unexpected status: {0}")]
    Status(StatusCode),
    #[error("authenticated account list was empty")]
    Empty,
}
