//! BullMQ-backed jobs used by `schedule` and `worker`.
//!
//! BullMQ owns queue state transitions, retry backoff, job locks, lock renewal,
//! stalled-job recovery, and retention. This module only defines Basango's
//! typed payloads and translates crawler configuration into BullMQ options.

use std::time::Duration;

use bullmq::options::RedisConnectionOptions;
use bullmq::types::{BackoffStrategy, KeepJobs, RemoveOnFinish};
use bullmq::{Queue, QueueOptions};
use serde::{Deserialize, Serialize};

use crate::{
    config::QueueConfig,
    domain::CrawlRequest,
    error::{CrawlError, Result},
    sources::ArticleSeed,
};

const JOB_ATTEMPTS: u32 = 3;
const RETRY_DELAY_MS: u64 = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverJob {
    pub request: CrawlRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchJob {
    pub request: CrawlRequest,
    pub article: ArticleSeed,
}

/// Producer-side access to the two crawler queues.
pub struct JobQueue {
    discovery: Queue,
    articles: Queue,
    config: QueueConfig,
}

impl JobQueue {
    pub async fn connect(config: &QueueConfig) -> Result<Self> {
        let options = queue_options(config);
        let (discovery, articles) = tokio::try_join!(
            Queue::with_options(&config.queues.discovery, options.clone()),
            Queue::with_options(&config.queues.articles, options),
        )?;
        Ok(Self {
            discovery,
            articles,
            config: config.clone(),
        })
    }

    pub fn names(&self) -> [&str; 2] {
        [
            self.config.queues.discovery.as_str(),
            self.config.queues.articles.as_str(),
        ]
    }

    pub fn validate_names(&self, names: &[String]) -> Result<()> {
        let valid = self.names();
        for name in names {
            if !valid.contains(&name.as_str()) {
                return Err(CrawlError::Queue(format!(
                    "unknown queue '{name}'; expected {} or {}",
                    valid[0], valid[1]
                )));
            }
        }
        Ok(())
    }

    pub async fn enqueue_discovery(&self, job: DiscoverJob) -> Result<String> {
        let id = stable_job_id("discover", &job)?;
        let queued = self
            .discovery
            .add("discover-source", job)
            .job_id(&id)
            .attempts(JOB_ATTEMPTS)
            .backoff(BackoffStrategy::Exponential(RETRY_DELAY_MS))
            .remove_on_complete(retention(self.config.retention.completed))
            .remove_on_fail(retention(self.config.retention.failed))
            .await?;
        Ok(queued.id().to_owned())
    }

    pub async fn enqueue_article(&self, job: FetchJob) -> Result<String> {
        let identity = (&job.request.source_id, &job.article.url);
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
}

pub fn queue_options(config: &QueueConfig) -> QueueOptions {
    QueueOptions::new()
        .connection(redis_options(config))
        .prefix(config.prefix.clone())
}

pub fn redis_options(config: &QueueConfig) -> RedisConnectionOptions {
    RedisConnectionOptions {
        url: config.redis_url.clone(),
        ..RedisConnectionOptions::default()
    }
}

fn retention(seconds: u64) -> RemoveOnFinish {
    if seconds == 0 {
        RemoveOnFinish::Bool(true)
    } else {
        RemoveOnFinish::Options(KeepJobs {
            age: Some(Duration::from_secs(seconds).as_millis() as u64),
            count: None,
            limit: Some(1_000),
        })
    }
}

fn stable_job_id(prefix: &str, value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(format!("{prefix}-{:x}", md5::compute(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_ids_are_deterministic() {
        let value = ("example", "https://example.com/story");
        assert_eq!(
            stable_job_id("article", &value).unwrap(),
            stable_job_id("article", &value).unwrap()
        );
    }

    #[test]
    fn zero_retention_removes_jobs_immediately() {
        assert!(matches!(retention(0), RemoveOnFinish::Bool(true)));
    }
}
