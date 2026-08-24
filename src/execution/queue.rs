//! BullMQ-backed jobs used by `schedule` and `worker`.
//!
//! BullMQ owns queue state transitions, retry backoff, job locks, lock renewal,
//! stalled-job recovery, and retention. This module only defines Basango's
//! typed payloads and translates crawler configuration into BullMQ options.

use std::time::Duration;

use bullmq::options::RedisConnectionOptions;
use bullmq::types::{BackoffStrategy, KeepJobs, RemoveOnFinish};
use bullmq::{Queue, QueueOptions};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    config::QueueConfig,
    domain::CrawlRequest,
    error::{CrawlError, Result},
    sources::ArticleSeed,
    telemetry::RunMetrics,
};

const JOB_ATTEMPTS: u32 = 3;
const RETRY_DELAY_MS: u64 = 1_000;
const RUN_PROGRESS_TTL_SECONDS: usize = 7 * 24 * 60 * 60;

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

#[derive(Debug, Clone, Copy)]
pub struct QueuedRunUpdate {
    pub metrics: RunMetrics,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResetReport {
    pub agent_id: String,
    pub discovery_queue: String,
    pub articles_queue: String,
    pub progress_trackers_removed: usize,
    pub legacy_queues_removed: bool,
}

/// Producer-side access to the two crawler queues.
pub struct JobQueue {
    discovery: Queue,
    articles: Queue,
    config: QueueConfig,
    agent_id: String,
    agent_scope: String,
    discovery_name: String,
    articles_name: String,
    progress_client: redis::Client,
}

impl JobQueue {
    pub async fn connect(config: &QueueConfig, agent_id: &str) -> Result<Self> {
        let options = queue_options(config);
        let progress_client = redis::Client::open(config.redis_url.clone())?;
        let agent_scope = encode_agent_id(agent_id);
        let discovery_name = scoped_queue_name(&agent_scope, &config.queues.discovery);
        let articles_name = scoped_queue_name(&agent_scope, &config.queues.articles);
        let (discovery, articles) = tokio::try_join!(
            Queue::with_options(&discovery_name, options.clone()),
            Queue::with_options(&articles_name, options),
        )?;
        Ok(Self {
            discovery,
            articles,
            config: config.clone(),
            agent_id: agent_id.to_owned(),
            agent_scope,
            discovery_name,
            articles_name,
            progress_client,
        })
    }

    pub fn names(&self) -> [&str; 2] {
        [self.discovery_name.as_str(), self.articles_name.as_str()]
    }

    pub fn validate_names(&self, names: &[String]) -> Result<()> {
        let scoped = self.names();
        let base = [
            self.config.queues.discovery.as_str(),
            self.config.queues.articles.as_str(),
        ];
        for name in names {
            if !scoped.contains(&name.as_str()) && !base.contains(&name.as_str()) {
                return Err(CrawlError::Queue(format!(
                    "unknown queue '{name}'; expected {} or {}",
                    base[0], base[1]
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
                } else {
                    name.clone()
                }
            })
            .collect()
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

    pub async fn initialize_run(&self, run_id: &str, discovered: usize) -> Result<()> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then
                redis.call('HSET', KEYS[1],
                    'discovered', ARGV[1],
                    'processed', 0,
                    'persisted', 0,
                    'delivered', 0,
                    'failed', 0,
                    'terminalSent', 0)
            end
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return 1
        "#;
        let key = self.run_progress_key(run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        redis::Script::new(SCRIPT)
            .key(key)
            .arg(discovered)
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .invoke_async::<i64>(&mut connection)
            .await?;
        Ok(())
    }

    pub async fn record_run_result(
        &self,
        run_id: &str,
        job_id: &str,
        persisted: usize,
        delivered: usize,
        failed: usize,
    ) -> Result<Option<QueuedRunUpdate>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            if redis.call('HSETNX', KEYS[1], 'job:' .. ARGV[1], 1) == 0 then return {} end
            local processed = redis.call('HINCRBY', KEYS[1], 'processed', 1)
            local persisted = redis.call('HINCRBY', KEYS[1], 'persisted', ARGV[2])
            local delivered = redis.call('HINCRBY', KEYS[1], 'delivered', ARGV[3])
            local failed = redis.call('HINCRBY', KEYS[1], 'failed', ARGV[4])
            local discovered = tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0
            local terminal = 0
            if processed >= discovered and tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 0 then
                redis.call('HSET', KEYS[1], 'terminalSent', 1)
                terminal = 1
            end
            redis.call('EXPIRE', KEYS[1], ARGV[5])
            return {discovered, persisted, delivered, failed, terminal}
        "#;
        let key = self.run_progress_key(run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let values: Vec<i64> = redis::Script::new(SCRIPT)
            .key(key)
            .arg(job_id)
            .arg(persisted)
            .arg(delivered)
            .arg(failed)
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .invoke_async(&mut connection)
            .await?;
        if values.len() != 5 {
            return Ok(None);
        }
        Ok(Some(QueuedRunUpdate {
            metrics: RunMetrics {
                articles_discovered: values[0].max(0) as usize,
                articles_persisted: values[1].max(0) as usize,
                articles_delivered: values[2].max(0) as usize,
                articles_failed: values[3].max(0) as usize,
            },
            terminal: values[4] == 1,
        }))
    }

    pub async fn fail_run(&self, run_id: &str) -> Result<Option<RunMetrics>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            redis.call('HSET', KEYS[1], 'terminalSent', 1)
            redis.call('EXPIRE', KEYS[1], ARGV[1])
            return {
                tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'persisted')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'delivered')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'failed')) or 0
            }
        "#;
        let key = self.run_progress_key(run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let values: Vec<i64> = redis::Script::new(SCRIPT)
            .key(key)
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .invoke_async(&mut connection)
            .await?;
        if values.len() != 4 {
            return Ok(None);
        }
        Ok(Some(RunMetrics {
            articles_discovered: values[0].max(0) as usize,
            articles_persisted: values[1].max(0) as usize,
            articles_delivered: values[2].max(0) as usize,
            articles_failed: values[3].max(0) as usize,
        }))
    }

    pub async fn reset_agent(&self, include_legacy_queues: bool) -> Result<AgentResetReport> {
        self.discovery.obliterate(true, 1_000).await?;
        self.articles.obliterate(true, 1_000).await?;
        if include_legacy_queues {
            let options = queue_options(&self.config);
            let (legacy_discovery, legacy_articles) = tokio::try_join!(
                Queue::with_options(&self.config.queues.discovery, options.clone()),
                Queue::with_options(&self.config.queues.articles, options),
            )?;
            tokio::try_join!(
                legacy_discovery.obliterate(true, 1_000),
                legacy_articles.obliterate(true, 1_000),
            )?;
        }

        let pattern = format!(
            "{}:telemetry:{}:run:*",
            self.config.prefix, self.agent_scope
        );
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let mut cursor = 0_u64;
        let mut progress_trackers_removed = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut connection)
                .await?;
            if !keys.is_empty() {
                progress_trackers_removed += keys.len();
                redis::cmd("DEL")
                    .arg(keys)
                    .query_async::<usize>(&mut connection)
                    .await?;
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(AgentResetReport {
            agent_id: self.agent_id.clone(),
            discovery_queue: self.discovery_name.clone(),
            articles_queue: self.articles_name.clone(),
            progress_trackers_removed,
            legacy_queues_removed: include_legacy_queues,
        })
    }

    fn run_progress_key(&self, run_id: &str) -> String {
        format!(
            "{}:telemetry:{}:run:{run_id}",
            self.config.prefix, self.agent_scope
        )
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

fn encode_agent_id(agent_id: &str) -> String {
    let mut encoded = String::with_capacity(agent_id.len());
    for byte in agent_id.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('_');
            encoded.push_str(&format!("{byte:02x}"));
        }
    }
    if encoded.is_empty() {
        "agent".to_owned()
    } else {
        encoded
    }
}

fn scoped_queue_name(agent_scope: &str, queue_name: &str) -> String {
    format!("{agent_scope}-{queue_name}")
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

    #[test]
    fn queue_names_are_scoped_by_agent_id() {
        assert_eq!(
            scoped_queue_name(&encode_agent_id("basango-pi-01"), "articles"),
            "basango-pi-01-articles"
        );
        assert_eq!(encode_agent_id("pi:west_1"), "pi_3awest_5f1");
    }
}
