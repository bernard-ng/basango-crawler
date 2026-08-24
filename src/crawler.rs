//! Small public facade for embedding the crawler in another Rust program.

use std::path::PathBuf;

use crate::{
    articles::Outbox,
    config::CrawlerConfig,
    domain::{CrawlRequest, SourceId},
    error::Result,
    execution::{
        CrawlReport, DiscoverJob, JobQueue, QueueResetReport, QueuedRunContext, Runtime, crawl_now,
        forward_pending, run_worker,
    },
    telemetry::{AgentReporter, RunMetrics, RunReporter},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResetReport {
    pub agent_id: String,
    pub discovery_queue: String,
    pub articles_queue: String,
    pub progress_trackers_removed: usize,
    pub outbox_articles_removed: usize,
    pub legacy_queues_removed: bool,
}

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
        let reporter = RunReporter::new(
            &self.runtime.config.ingestion,
            self.runtime.http.clone(),
            request.source_id.as_str(),
            &self.runtime.agent_id,
        );
        let run = QueuedRunContext {
            run_id: reporter.run_id().to_owned(),
            agent_id: reporter.agent_id().to_owned(),
            started_at: chrono::Utc::now(),
        };
        reporter.preparing().await;
        let queue =
            match JobQueue::connect(&self.runtime.config.queue, &self.runtime.agent_id).await {
                Ok(queue) => queue,
                Err(error) => {
                    reporter
                        .failed(RunMetrics::default(), 0, error.to_string())
                        .await;
                    return Err(error);
                }
            };
        if let Err(error) = queue.prepare_run(&run, &request.source_id).await {
            reporter
                .failed(RunMetrics::default(), 0, error.to_string())
                .await;
            return Err(error);
        }
        match queue.enqueue_discovery(DiscoverJob { request, run }).await {
            Ok(job_id) => Ok(job_id),
            Err(error) => {
                let _ = queue.fail_run(reporter.run_id()).await;
                reporter
                    .failed(RunMetrics::default(), 0, error.to_string())
                    .await;
                Err(error)
            }
        }
    }

    /// Deliver pending and retryable outbox entries.
    pub async fn deliver_pending(
        &self,
        source: Option<&SourceId>,
        limit: usize,
        retry_all: bool,
    ) -> Result<CrawlReport> {
        forward_pending(&self.runtime, source, limit, retry_all).await
    }

    /// Run BullMQ consumers until the process receives Ctrl-C.
    pub async fn work(&self, queues: Vec<String>, concurrency: usize) -> Result<()> {
        run_worker(self.runtime.clone(), queues, concurrency).await
    }

    /// Clear this agent's BullMQ state and local SQLite outbox.
    pub async fn reset_agent(&self, include_legacy_queues: bool) -> Result<AgentResetReport> {
        let queue = JobQueue::connect(&self.runtime.config.queue, &self.runtime.agent_id).await?;
        let QueueResetReport {
            agent_id,
            discovery_queue,
            articles_queue,
            progress_trackers_removed,
            legacy_queues_removed,
        } = queue.reset_agent(include_legacy_queues).await?;
        let outbox = Outbox::open(&self.runtime.config.sqlite_path(), true)?;
        let outbox_articles_removed = outbox.clear()?;
        AgentReporter::new(
            &self.runtime.config.ingestion,
            self.runtime.http.clone(),
            &self.runtime.agent_id,
        )
        .reset()
        .await;
        Ok(AgentResetReport {
            agent_id,
            discovery_queue,
            articles_queue,
            progress_trackers_removed,
            outbox_articles_removed,
            legacy_queues_removed,
        })
    }
}
