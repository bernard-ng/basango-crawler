use std::str::FromStr;

use chrono::{DateTime, Utc};

use crate::{
    domain::{Article, ArticleHash, SourceId},
    error::{CrawlError, Result},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Forwarded,
    Failed,
}

impl FromStr for DeliveryStatus {
    type Err = CrawlError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "forwarded" => Ok(Self::Forwarded),
            "failed" => Ok(Self::Failed),
            other => Err(CrawlError::Configuration(format!(
                "unknown outbox status '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub article: Article,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub retryable: bool,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub forwarded_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub claimed_by: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutboxStats {
    pub total: usize,
    pub pending: usize,
    pub forwarded: usize,
    pub failed: usize,
    pub retryable_failed: usize,
    pub claimed: usize,
    pub delivery_intents_pending: usize,
    pub delivery_intents_failed: usize,
}

#[derive(Debug, Clone)]
pub struct DeliveryIntent {
    pub run_id: String,
    pub agent_id: String,
    pub source_id: SourceId,
    pub article_hash: ArticleHash,
    pub started_at: DateTime<Utc>,
}
