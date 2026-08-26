//! Domain types: the vocabulary of the crawler.
//!
//! These values describe what Basango works with and deliberately avoid HTTP,
//! Redis, SQLite, and CLI concerns.

mod article;
mod crawl;
mod delivery;
mod run;
mod source;

pub use article::{Article, ArticleDraft, ArticleHash, ArticleMetadata};
pub use crawl::{CrawlRequest, DateRange, PageRange, UpdateDirection};
pub use delivery::{DeliveryOutcome, DeliveryState, RetryDecision};
pub use run::{AgentId, RunId};
pub use source::{CategorySlug, SourceId};

#[cfg(test)]
#[path = "../../tests/unit/domain.rs"]
mod tests;
