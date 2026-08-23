//! Basango's reusable crawler library.
//!
//! Think of this file as a map, not a storage room. It declares the top-level
//! modules and keeps implementation details in focused files. Most callers
//! only need [`Crawler`] and [`CrawlRequest`].

mod articles;
mod cli;
pub mod config;
mod crawler;
pub mod domain;
pub mod error;
mod execution;
mod http;
mod sources;
mod telemetry;

pub use articles::{DeliveryStatus, Outbox, OutboxEntry, normalize};
pub use crawler::Crawler;
pub use domain::{
    Article, ArticleDraft, ArticleMetadata, CrawlRequest, DateRange, PageRange, SourceId,
    UpdateDirection,
};
pub use error::{CrawlError, Result};
pub use execution::CrawlReport;

/// Run the bundled command-line interface.
pub async fn run_cli() -> anyhow::Result<()> {
    cli::run().await
}
