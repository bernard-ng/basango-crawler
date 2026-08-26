//! Small public facade for embedding the crawler in another Rust program.

use std::path::PathBuf;

use chrono::{DateTime, Utc};

use crate::{
    articles::{Outbox, OutboxStats},
    config::CrawlerConfig,
    domain::{CrawlRequest, SourceId},
    error::Result,
    execution::{
        CrawlReport, DiscoverJob, JobQueue, QueueResetReport, QueueSnapshot, QueuedRunContext,
        QueuedRunReconciliation, Runtime, crawl_now, forward_pending, run_worker,
    },
    telemetry::{AgentReporter, RunMetrics, RunReporter},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResetReport {
    pub agent_id: String,
    pub discovery_queue: String,
    pub articles_queue: String,
    pub delivery_queue: String,
    pub progress_trackers_removed: usize,
    pub outbox_articles_removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStatus {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRunStatus {
    pub run_id: String,
    pub source_id: String,
    pub started_at: DateTime<Utc>,
    pub discovered: usize,
    pub processed: usize,
    pub deliveries_expected: usize,
    pub deliveries_processed: usize,
    pub persisted: usize,
    pub skipped: usize,
    pub delivered: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReconciliation {
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
pub struct RedisStatus {
    pub queues: Vec<QueueStatus>,
    pub open_runs: Vec<OpenRunStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawlerStatus {
    pub agent_id: String,
    pub sqlite_path: PathBuf,
    pub outbox: std::result::Result<OutboxStats, String>,
    pub redis: std::result::Result<RedisStatus, String>,
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
    pub async fn schedule(&self, mut request: CrawlRequest) -> Result<String> {
        self.runtime.config.prepare_request(&mut request)?;
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

    /// Read the current agent's local outbox and Redis queue state.
    pub async fn status(&self) -> CrawlerStatus {
        let sqlite_path = self.runtime.config.sqlite_path();
        let outbox = if Outbox::exists(&sqlite_path) {
            Outbox::open(&sqlite_path, false)
                .and_then(|outbox| outbox.stats())
                .map_err(|error| error.to_string())
        } else {
            Err("not initialized".to_owned())
        };

        let redis = async {
            let queue =
                JobQueue::connect(&self.runtime.config.queue, &self.runtime.agent_id).await?;
            let (queues, open_runs) = queue.status().await?;
            Ok::<RedisStatus, crate::error::CrawlError>(RedisStatus {
                queues: queues.into_iter().map(QueueStatus::from).collect(),
                open_runs: open_runs
                    .into_iter()
                    .map(|run| OpenRunStatus {
                        run_id: run.run.run_id,
                        source_id: run.source_id.to_string(),
                        started_at: run.run.started_at,
                        discovered: run.metrics.articles_discovered,
                        processed: run.articles_processed,
                        deliveries_expected: run.deliveries_expected,
                        deliveries_processed: run.deliveries_processed,
                        persisted: run.metrics.articles_persisted,
                        skipped: run.metrics.articles_skipped,
                        delivered: run.metrics.articles_delivered,
                        failed: run.metrics.articles_failed,
                    })
                    .collect(),
            })
        }
        .await
        .map_err(|error| error.to_string());

        CrawlerStatus {
            agent_id: self.runtime.agent_id.clone(),
            sqlite_path,
            outbox,
            redis,
        }
    }

    /// Inspect a run tracker's accounting without mutating or requeueing work.
    pub async fn reconcile_run(&self, run_id: &str) -> Result<RunReconciliation> {
        let queue = JobQueue::connect(&self.runtime.config.queue, &self.runtime.agent_id).await?;
        queue.reconcile_run(run_id).await.map(Into::into)
    }

    /// Clear this agent's BullMQ state and local SQLite outbox.
    pub async fn reset_agent(&self) -> Result<AgentResetReport> {
        let queue = JobQueue::connect(&self.runtime.config.queue, &self.runtime.agent_id).await?;
        let QueueResetReport {
            agent_id,
            discovery_queue,
            articles_queue,
            delivery_queue,
            progress_trackers_removed,
        } = queue.reset_agent().await?;
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
            delivery_queue,
            progress_trackers_removed,
            outbox_articles_removed,
        })
    }
}

impl From<QueuedRunReconciliation> for RunReconciliation {
    fn from(value: QueuedRunReconciliation) -> Self {
        Self {
            run_id: value.run_id,
            source_id: value.source_id,
            discovery_complete: value.discovery_complete,
            terminal: value.terminal,
            discovered: value.discovered,
            processed: value.processed,
            persisted: value.persisted,
            skipped: value.skipped,
            failed: value.failed,
            deliveries_expected: value.deliveries_expected,
            deliveries_processed: value.deliveries_processed,
            delivered: value.delivered,
        }
    }
}

impl From<QueueSnapshot> for QueueStatus {
    fn from(snapshot: QueueSnapshot) -> Self {
        Self {
            name: snapshot.name,
            workers: snapshot.workers,
            waiting: snapshot.waiting,
            active: snapshot.active,
            delayed: snapshot.delayed,
            prioritized: snapshot.prioritized,
            completed: snapshot.completed,
            failed: snapshot.failed,
            waiting_children: snapshot.waiting_children,
            paused: snapshot.paused,
        }
    }
}
