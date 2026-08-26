//! BullMQ-backed jobs used by `schedule` and `worker`.
//!
//! BullMQ owns queue state transitions, retry backoff, job locks, lock renewal,
//! stalled-job recovery, and retention. This module defines Basango's typed
//! payloads and translates crawler configuration into BullMQ options.

mod model;
mod progress;
mod support;

use bullmq::Queue;
use bullmq::types::BackoffStrategy;

use crate::{
    config::QueueConfig,
    error::{CrawlError, Result},
};

pub use model::{
    AgentResetReport, DeliveryJob, DiscoverJob, FetchJob, OpenQueuedRun, QueueSnapshot,
    QueuedArticleResult, QueuedRunContext, QueuedRunReconciliation, QueuedRunUpdate,
};
pub(crate) use support::redis_options;
use support::{encode_agent_id, queue_options, retention, scoped_queue_name, stable_job_id};

const JOB_ATTEMPTS: u32 = 3;
const RETRY_DELAY_MS: u64 = 1_000;
const RUN_PROGRESS_TTL_SECONDS: usize = 7 * 24 * 60 * 60;

/// Producer-side access to the crawler queues.
pub struct JobQueue {
    pub(super) discovery: Queue,
    pub(super) articles: Queue,
    pub(super) delivery: Queue,
    pub(super) config: QueueConfig,
    pub(super) agent_id: String,
    pub(super) agent_scope: String,
    pub(super) discovery_name: String,
    pub(super) articles_name: String,
    pub(super) delivery_name: String,
    pub(super) progress_client: redis::Client,
}

impl JobQueue {
    pub async fn connect(config: &QueueConfig, agent_id: &str) -> Result<Self> {
        let options = queue_options(config);
        let progress_client = redis::Client::open(config.redis_url.clone())?;
        let agent_scope = encode_agent_id(agent_id);
        let discovery_name = scoped_queue_name(&agent_scope, &config.queues.discovery);
        let articles_name = scoped_queue_name(&agent_scope, &config.queues.articles);
        let delivery_name = scoped_queue_name(&agent_scope, &config.queues.delivery);
        let (discovery, articles, delivery) = tokio::try_join!(
            Queue::with_options(&discovery_name, options.clone()),
            Queue::with_options(&articles_name, options.clone()),
            Queue::with_options(&delivery_name, options),
        )?;
        Ok(Self {
            discovery,
            articles,
            delivery,
            config: config.clone(),
            agent_id: agent_id.to_owned(),
            agent_scope,
            discovery_name,
            articles_name,
            delivery_name,
            progress_client,
        })
    }

    pub fn names(&self) -> [&str; 3] {
        [
            self.discovery_name.as_str(),
            self.articles_name.as_str(),
            self.delivery_name.as_str(),
        ]
    }

    pub fn validate_names(&self, names: &[String]) -> Result<()> {
        let scoped = self.names();
        let base = [
            self.config.queues.discovery.as_str(),
            self.config.queues.articles.as_str(),
            self.config.queues.delivery.as_str(),
        ];
        for name in names {
            if !scoped.contains(&name.as_str()) && !base.contains(&name.as_str()) {
                return Err(CrawlError::Queue(format!(
                    "unknown queue '{name}'; expected {}, {}, or {}",
                    base[0], base[1], base[2]
                )));
            }
        }
        Ok(())
    }

    pub fn resolve_names(&self, names: &[String]) -> Vec<String> {
        names
            .iter()
            .map(|name| {
                if name == &self.config.queues.discovery {
                    self.discovery_name.clone()
                } else if name == &self.config.queues.articles {
                    self.articles_name.clone()
                } else if name == &self.config.queues.delivery {
                    self.delivery_name.clone()
                } else {
                    name.clone()
                }
            })
            .collect()
    }

    pub async fn enqueue_discovery(&self, job: DiscoverJob) -> Result<String> {
        let id = stable_job_id("discover", &job)?;
        let run_id = job.run.run_id.clone();
        let source_id = job.request.source_id.clone();
        let queued = self
            .discovery
            .add("discover-source", job)
            .job_id(&id)
            .attempts(JOB_ATTEMPTS)
            .backoff(BackoffStrategy::Exponential(RETRY_DELAY_MS))
            .remove_on_complete(retention(self.config.retention.completed))
            .remove_on_fail(retention(self.config.retention.failed))
            .await?;
        tracing::info!(
            agent_id = self.agent_id,
            queue = self.discovery_name,
            job_id = queued.id(),
            %run_id,
            source = %source_id,
            "discovery job enqueued"
        );
        Ok(queued.id().to_owned())
    }

    pub async fn enqueue_article(&self, job: FetchJob) -> Result<String> {
        let identity = (&job.run.run_id, &job.request.source_id, &job.article.url);
        let id = stable_job_id("article", &identity)?;
        let queued = self
            .articles
            .add("fetch-article", job)
            .job_id(&id)
            .attempts(JOB_ATTEMPTS)
            .backoff(BackoffStrategy::Exponential(RETRY_DELAY_MS))
            .remove_on_complete(retention(self.config.retention.completed))
            .remove_on_fail(retention(self.config.retention.failed))
            .await?;
        Ok(queued.id().to_owned())
    }

    pub async fn enqueue_delivery(&self, job: DeliveryJob) -> Result<String> {
        let identity = (&job.run.run_id, &job.article_hash);
        let id = stable_job_id("delivery", &identity)?;
        let queued = self
            .delivery
            .add("deliver-article", job)
            .job_id(&id)
            .attempts(JOB_ATTEMPTS)
            .backoff(BackoffStrategy::Exponential(RETRY_DELAY_MS))
            .remove_on_complete(retention(self.config.retention.completed))
            .remove_on_fail(retention(self.config.retention.failed))
            .await?;
        Ok(queued.id().to_owned())
    }

    pub async fn retry_failed_deliveries(&self) -> Result<()> {
        self.delivery.retry_jobs("failed", 1_000, None).await?;
        Ok(())
    }
}

#[cfg(test)]
use bullmq::types::{JobCounts, RemoveOnFinish};
#[cfg(test)]
use support::{metrics_from_values, snapshot_from_counts};

#[cfg(test)]
#[path = "../../../tests/unit/execution/queue.rs"]
mod tests;
