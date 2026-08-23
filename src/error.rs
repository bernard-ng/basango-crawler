//! One error vocabulary for the crawler.
//!
//! `thiserror` removes repetitive `Display` and `From` implementations while
//! keeping errors typed. `anyhow` is only used at the executable boundary,
//! where reporting matters more than programmatic recovery.

use thiserror::Error;

/// Convenience alias used throughout the library.
pub type Result<T, E = CrawlError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum CrawlError {
    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("source '{0}' was not found")]
    SourceNotFound(String),

    #[error("invalid source selectors: {0}")]
    InvalidSourceSelectors(String),

    #[error("invalid article: {0}")]
    InvalidArticle(String),

    #[error("article at {url} is outside the requested date range")]
    ArticleOutOfDateRange { url: String },

    #[error("invalid range: {0}")]
    InvalidRange(String),

    #[error("HTTP request failed: {0}")]
    HttpTransport(#[from] reqwest::Error),

    #[error("HTTP {status} from {url}: {body}")]
    HttpStatus {
        status: u16,
        url: String,
        body: String,
    },

    #[error("SQLite outbox error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("SQLite outbox became unavailable after a task panicked")]
    OutboxLockPoisoned,

    #[error("BullMQ error: {0}")]
    BullMq(#[from] bullmq::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),

    #[error("queue error: {0}")]
    Queue(String),
}
