use std::time::Duration;

use bullmq::options::RedisConnectionOptions;
use bullmq::types::{JobCounts, KeepJobs, RemoveOnFinish};
use bullmq::{Queue, QueueOptions};
use serde::Serialize;

use crate::{
    config::QueueConfig,
    error::{CrawlError, Result},
    telemetry::RunMetrics,
};

use super::{QueueSnapshot, QueuedRunUpdate};

pub(super) fn queue_options(config: &QueueConfig) -> QueueOptions {
    QueueOptions::new()
        .connection(redis_options(config))
        .prefix(config.prefix.clone())
}

pub(crate) fn redis_options(config: &QueueConfig) -> RedisConnectionOptions {
    RedisConnectionOptions {
        url: config.redis_url.clone(),
        ..RedisConnectionOptions::default()
    }
}

pub(super) async fn queue_snapshot(queue: &Queue, name: &str) -> Result<QueueSnapshot> {
    let (counts, workers) = tokio::try_join!(queue.get_job_counts(), queue.get_workers_count())?;
    Ok(snapshot_from_counts(name, workers, counts))
}

pub(super) fn snapshot_from_counts(name: &str, workers: usize, counts: JobCounts) -> QueueSnapshot {
    QueueSnapshot {
        name: name.to_owned(),
        workers,
        waiting: counts.waiting,
        active: counts.active,
        delayed: counts.delayed,
        prioritized: counts.prioritized,
        completed: counts.completed,
        failed: counts.failed,
        waiting_children: counts.waiting_children,
        paused: counts.paused,
    }
}

pub(super) fn retention(seconds: u64) -> RemoveOnFinish {
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

pub(super) fn stable_job_id(prefix: &str, value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(format!("{prefix}-{:x}", md5::compute(bytes)))
}

pub(super) fn encode_agent_id(agent_id: &str) -> String {
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

pub(super) fn scoped_queue_name(agent_scope: &str, queue_name: &str) -> String {
    format!("{agent_scope}-{queue_name}")
}

pub(super) fn parse_metric(value: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|_| {
        CrawlError::Queue(format!(
            "invalid queued run metric '{value}' in Redis progress tracker"
        ))
    })
}

pub(super) fn metrics_from_values(values: &[i64]) -> Result<RunMetrics> {
    if values.len() != 4 {
        return Err(CrawlError::Queue(format!(
            "expected 4 queued run metrics, received {}",
            values.len()
        )));
    }
    Ok(RunMetrics {
        articles_discovered: values[0].max(0) as usize,
        articles_persisted: values[1].max(0) as usize,
        articles_delivered: values[2].max(0) as usize,
        articles_failed: values[3].max(0) as usize,
    })
}

pub(super) fn queued_update_from_values(values: &[i64]) -> Result<Option<QueuedRunUpdate>> {
    if values.is_empty() {
        return Ok(None);
    }
    if values.len() != 5 {
        return Err(CrawlError::Queue(format!(
            "expected 5 queued run update values, received {}",
            values.len()
        )));
    }
    Ok(Some(QueuedRunUpdate {
        metrics: metrics_from_values(&values[..4])?,
        terminal: values[4] == 1,
    }))
}
