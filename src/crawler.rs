//! Small public facade for embedding the crawler in another Rust program.

use std::path::PathBuf;

use crate::{
    config::CrawlerConfig,
    domain::{CrawlRequest, SourceId},
    error::Result,
    execution::{
        CrawlReport, DiscoverJob, JobQueue, Runtime, crawl_now, forward_pending, run_worker,
    },
};

/// A configured crawler with reusable HTTP connections.
#[derive(Clone)]
pub struct Crawler {
    runtime: Runtime,
}

impl Crawler {
    pub fn new(config: CrawlerConfig) -> Result<Self> {
        Ok(Self {
            runtime: Runtime::new(config)?,
        })
    }

    /// Use an environment-selected config file or the bundled default, then
    /// apply environment overrides.
    pub fn from_environment() -> Result<Self> {
        Self::new(CrawlerConfig::load(None)?)
    }

    /// Load one explicit JSON config file, then apply environment overrides.
    pub fn from_config_file(path: impl Into<PathBuf>) -> Result<Self> {
        Self::new(CrawlerConfig::load(Some(path.into()))?)
    }

    pub fn config(&self) -> &CrawlerConfig {
        &self.runtime.config
    }

    /// Crawl now, streaming collected drafts into the durable outbox.
    pub async fn crawl(&self, request: CrawlRequest) -> Result<CrawlReport> {
        crawl_now(&self.runtime, request).await
    }

    /// Schedule source discovery in BullMQ.
    pub async fn schedule(&self, request: CrawlRequest) -> Result<String> {
        self.runtime.config.source(&request.source_id)?;
        JobQueue::connect(&self.runtime.config.queue)
            .await?
            .enqueue_discovery(DiscoverJob { request })
            .await
    }

    /// Deliver pending and retryable outbox entries.
    pub async fn deliver_pending(
        &self,
        source: Option<&SourceId>,
        limit: usize,
    ) -> Result<CrawlReport> {
        forward_pending(&self.runtime, source, limit).await
    }

    /// Run BullMQ consumers until the process receives Ctrl-C.
    pub async fn work(&self, queues: Vec<String>, concurrency: usize) -> Result<()> {
        run_worker(self.runtime.clone(), queues, concurrency).await
    }
}
