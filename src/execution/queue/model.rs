use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{ArticleHash, CrawlRequest, SourceId},
    sources::ArticleSeed,
    telemetry::RunMetrics,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuedRunContext {
    pub run_id: String,
    pub agent_id: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverJob {
    pub request: CrawlRequest,
    pub run: QueuedRunContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchJob {
    pub request: CrawlRequest,
    pub article: ArticleSeed,
    pub run: QueuedRunContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryJob {
    pub article_hash: ArticleHash,
    pub source_id: SourceId,
    pub run: QueuedRunContext,
}

#[derive(Debug, Clone, Copy)]
pub struct QueuedRunUpdate {
    pub metrics: RunMetrics,
    pub terminal: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueuedArticleResult {
    pub persisted: usize,
    pub delivered: usize,
    pub delivery_expected: usize,
    pub failed: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone)]
pub struct OpenQueuedRun {
    pub run: QueuedRunContext,
    pub source_id: SourceId,
    pub articles_processed: usize,
    pub deliveries_expected: usize,
    pub deliveries_processed: usize,
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedRunReconciliation {
    pub run_id: String,
    pub source_id: Option<String>,
    pub discovery_complete: Option<bool>,
    pub terminal: bool,
    pub discovered: usize,
    pub processed: Option<usize>,
    pub persisted: usize,
    pub skipped: Option<usize>,
    pub failed: usize,
    pub deliveries_expected: Option<usize>,
    pub deliveries_processed: Option<usize>,
    pub delivered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResetReport {
    pub agent_id: String,
    pub discovery_queue: String,
    pub articles_queue: String,
    pub delivery_queue: String,
    pub progress_trackers_removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueSnapshot {
    pub name: String,
    pub workers: usize,
    pub waiting: u64,
    pub active: u64,
    pub delayed: u64,
    pub prioritized: u64,
    pub completed: u64,
    pub failed: u64,
    pub waiting_children: u64,
    pub paused: u64,
}
