use chrono::{DateTime, Utc};

use crate::{domain::SourceId, error::Result, telemetry::RunMetrics};

use super::{
    AgentResetReport, JobQueue, OpenQueuedRun, PROGRESS_PUBLISH_INTERVAL_MS, QueueSnapshot,
    QueuedArticleResult, QueuedRunContext, QueuedRunUpdate, RUN_PROGRESS_TTL_SECONDS,
    support::{metrics_from_values, parse_metric, queue_snapshot, queued_update_from_values},
};

impl JobQueue {
    pub async fn claim_progress_publication(&self, run_id: &str) -> Result<bool> {
        let key = format!(
            "{}:telemetry:{}:progress-publication:{run_id}",
            self.config.prefix, self.agent_scope
        );
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let acquired: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(1)
            .arg("NX")
            .arg("PX")
            .arg(PROGRESS_PUBLISH_INTERVAL_MS)
            .query_async(&mut connection)
            .await?;

        Ok(acquired.is_some())
    }

    pub async fn prepare_run(&self, run: &QueuedRunContext, source_id: &SourceId) -> Result<()> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then
                redis.call('HSET', KEYS[1],
                    'sourceId', ARGV[1],
                    'startedAt', ARGV[2],
                    'discovered', 0,
                    'discoveryComplete', 0,
                    'articleProcessed', 0,
                    'deliveryExpected', 0,
                    'deliveryProcessed', 0,
                    'persisted', 0,
                    'skipped', 0,
                    'delivered', 0,
                    'failed', 0,
                    'terminalSent', 0)
            end
            redis.call('EXPIRE', KEYS[1], ARGV[3])
            return 1
        "#;
        let key = self.run_progress_key(&run.run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        redis::Script::new(SCRIPT)
            .key(key)
            .arg(source_id.as_str())
            .arg(run.started_at.to_rfc3339())
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .invoke_async::<i64>(&mut connection)
            .await?;
        Ok(())
    }

    pub async fn record_discovery_batch(
        &self,
        run_id: &str,
        batch_id: &str,
        article_job_ids: &[String],
    ) -> Result<Option<RunMetrics>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            redis.call('HSET', KEYS[1], 'batch:' .. ARGV[1], 1)
            local added = 0
            for index = 3, #ARGV do
                if redis.call('HSETNX', KEYS[1], 'article:' .. ARGV[index], 1) == 1 then
                    added = added + 1
                end
            end
            if added > 0 then
                redis.call('HINCRBY', KEYS[1], 'discovered', added)
            end
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return {
                tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'articleProcessed')
                    or redis.call('HGET', KEYS[1], 'processed')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'persisted')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'skipped')) or 0,
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
            .arg(batch_id)
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .arg(article_job_ids)
            .invoke_async(&mut connection)
            .await?;
        if values.is_empty() {
            Ok(None)
        } else {
            Ok(Some(metrics_from_values(&values)?))
        }
    }

    pub async fn finish_discovery(&self, run_id: &str) -> Result<Option<QueuedRunUpdate>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            redis.call('HSET', KEYS[1], 'discoveryComplete', 1)
            local discovered = tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0
            local articleProcessed = tonumber(redis.call('HGET', KEYS[1], 'articleProcessed')
                or redis.call('HGET', KEYS[1], 'processed')) or 0
            local deliveryExpected = tonumber(redis.call('HGET', KEYS[1], 'deliveryExpected')) or 0
            local deliveryProcessed = tonumber(redis.call('HGET', KEYS[1], 'deliveryProcessed')) or 0
            local terminal = 0
            if articleProcessed == discovered and deliveryProcessed == deliveryExpected then
                redis.call('HSET', KEYS[1], 'terminalSent', 1)
                terminal = 1
            end
            redis.call('EXPIRE', KEYS[1], ARGV[1])
            return {
                discovered,
                articleProcessed,
                tonumber(redis.call('HGET', KEYS[1], 'persisted')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'skipped')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'delivered')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'failed')) or 0,
                terminal
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
        queued_update_from_values(&values)
    }

    pub async fn run_is_open(&self, run_id: &str) -> Result<bool> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return 0 end
            return 1
        "#;
        let key = self.run_progress_key(run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let open: i64 = redis::Script::new(SCRIPT)
            .key(key)
            .invoke_async(&mut connection)
            .await?;
        Ok(open == 1)
    }

    pub async fn record_article_result(
        &self,
        run_id: &str,
        job_id: &str,
        result: QueuedArticleResult,
    ) -> Result<Option<QueuedRunUpdate>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            if redis.call('HSETNX', KEYS[1], 'job:' .. ARGV[1], 1) == 0 then return {} end
            if redis.call('HEXISTS', KEYS[1], 'articleProcessed') == 0 then
                redis.call('HSET', KEYS[1], 'articleProcessed',
                    tonumber(redis.call('HGET', KEYS[1], 'processed')) or 0)
            end
            local articleProcessed = redis.call('HINCRBY', KEYS[1], 'articleProcessed', 1)
            local persisted = redis.call('HINCRBY', KEYS[1], 'persisted', ARGV[2])
            local delivered = redis.call('HINCRBY', KEYS[1], 'delivered', ARGV[3])
            local deliveryExpected = redis.call('HINCRBY', KEYS[1], 'deliveryExpected', ARGV[4])
            local failed = redis.call('HINCRBY', KEYS[1], 'failed', ARGV[5])
            local skipped = redis.call('HINCRBY', KEYS[1], 'skipped', ARGV[6])
            local discovered = tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0
            local deliveryProcessed = tonumber(redis.call('HGET', KEYS[1], 'deliveryProcessed')) or 0
            local terminal = 0
            if tonumber(redis.call('HGET', KEYS[1], 'discoveryComplete')) == 1
                and articleProcessed == discovered
                and deliveryProcessed == deliveryExpected
                and tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 0 then
                redis.call('HSET', KEYS[1], 'terminalSent', 1)
                terminal = 1
            end
            redis.call('EXPIRE', KEYS[1], ARGV[7])
            return {discovered, articleProcessed, persisted, skipped, delivered, failed, terminal}
        "#;
        let key = self.run_progress_key(run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let values: Vec<i64> = redis::Script::new(SCRIPT)
            .key(key)
            .arg(job_id)
            .arg(result.persisted)
            .arg(result.delivered)
            .arg(result.delivery_expected)
            .arg(result.failed)
            .arg(result.skipped)
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .invoke_async(&mut connection)
            .await?;
        queued_update_from_values(&values)
    }

    pub async fn record_delivery_result(
        &self,
        run_id: &str,
        job_id: &str,
        delivered: usize,
        failed: usize,
    ) -> Result<Option<QueuedRunUpdate>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            if redis.call('HSETNX', KEYS[1], 'deliveryJob:' .. ARGV[1], 1) == 0 then return {} end
            local deliveryProcessed = redis.call('HINCRBY', KEYS[1], 'deliveryProcessed', 1)
            local delivered = redis.call('HINCRBY', KEYS[1], 'delivered', ARGV[2])
            local failed = redis.call('HINCRBY', KEYS[1], 'failed', ARGV[3])
            local discovered = tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0
            local articleProcessed = tonumber(redis.call('HGET', KEYS[1], 'articleProcessed')
                or redis.call('HGET', KEYS[1], 'processed')) or 0
            local deliveryExpected = tonumber(redis.call('HGET', KEYS[1], 'deliveryExpected')) or 0
            local terminal = 0
            if tonumber(redis.call('HGET', KEYS[1], 'discoveryComplete')) == 1
                and articleProcessed == discovered
                and deliveryProcessed == deliveryExpected
                and tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 0 then
                redis.call('HSET', KEYS[1], 'terminalSent', 1)
                terminal = 1
            end
            redis.call('EXPIRE', KEYS[1], ARGV[4])
            return {discovered, articleProcessed,
                tonumber(redis.call('HGET', KEYS[1], 'persisted')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'skipped')) or 0,
                delivered, failed, terminal}
        "#;
        let key = self.run_progress_key(run_id);
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let values: Vec<i64> = redis::Script::new(SCRIPT)
            .key(key)
            .arg(job_id)
            .arg(delivered)
            .arg(failed)
            .arg(RUN_PROGRESS_TTL_SECONDS)
            .invoke_async(&mut connection)
            .await?;
        queued_update_from_values(&values)
    }

    pub async fn fail_run(&self, run_id: &str) -> Result<Option<RunMetrics>> {
        self.close_run(run_id).await
    }

    pub async fn status(&self) -> Result<(Vec<QueueSnapshot>, Vec<OpenQueuedRun>)> {
        let (discovery, articles, delivery, runs) = tokio::try_join!(
            queue_snapshot(&self.discovery, &self.discovery_name),
            queue_snapshot(&self.articles, &self.articles_name),
            queue_snapshot(&self.delivery, &self.delivery_name),
            self.open_runs(),
        )?;
        Ok((vec![discovery, articles, delivery], runs))
    }

    pub async fn open_runs(&self) -> Result<Vec<OpenQueuedRun>> {
        let pattern = self.run_progress_pattern();
        let mut connection = self
            .progress_client
            .get_multiplexed_async_connection()
            .await?;
        let mut cursor = 0_u64;
        let mut runs = Vec::new();
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut connection)
                .await?;
            for key in keys {
                let values: Vec<Option<String>> = redis::cmd("HMGET")
                    .arg(&key)
                    .arg(&[
                        "sourceId",
                        "startedAt",
                        "discovered",
                        "articleProcessed",
                        "processed",
                        "deliveryExpected",
                        "deliveryProcessed",
                        "persisted",
                        "skipped",
                        "delivered",
                        "failed",
                        "terminalSent",
                    ])
                    .query_async(&mut connection)
                    .await?;
                if values.len() != 12 || values[11].as_deref() == Some("1") {
                    continue;
                }
                let Some(run_id) = key.rsplit(':').next() else {
                    continue;
                };
                let Ok(source_id) = SourceId::new(values[0].as_deref().unwrap_or_default()) else {
                    tracing::warn!(run_id, "skipping queued run tracker without a source ID");
                    continue;
                };
                let Ok(started_at) =
                    DateTime::parse_from_rfc3339(values[1].as_deref().unwrap_or_default())
                else {
                    tracing::warn!(
                        run_id,
                        "skipping queued run tracker with an invalid start time"
                    );
                    continue;
                };
                let metrics = [
                    parse_metric(values[2].as_deref().unwrap_or("0")),
                    parse_metric(values[3].as_deref().or(values[4].as_deref()).unwrap_or("0")),
                    parse_metric(values[7].as_deref().unwrap_or("0")),
                    parse_metric(values[8].as_deref().unwrap_or("0")),
                    parse_metric(values[9].as_deref().unwrap_or("0")),
                    parse_metric(values[10].as_deref().unwrap_or("0")),
                ];
                let [
                    Ok(discovered),
                    Ok(processed),
                    Ok(persisted),
                    Ok(skipped),
                    Ok(delivered),
                    Ok(failed),
                ] = metrics
                else {
                    tracing::warn!(run_id, "skipping queued run tracker with invalid metrics");
                    continue;
                };
                runs.push(OpenQueuedRun {
                    run: QueuedRunContext {
                        run_id: run_id.to_owned(),
                        agent_id: self.agent_id.clone(),
                        started_at: started_at.with_timezone(&Utc),
                    },
                    source_id,
                    articles_processed: processed,
                    deliveries_expected: parse_metric(values[5].as_deref().unwrap_or("0"))?,
                    deliveries_processed: parse_metric(values[6].as_deref().unwrap_or("0"))?,
                    metrics: RunMetrics {
                        articles_discovered: discovered,
                        articles_processed: processed,
                        articles_persisted: persisted,
                        articles_skipped: skipped,
                        articles_delivered: delivered,
                        articles_failed: failed,
                    },
                });
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
        Ok(runs)
    }

    pub async fn reset_agent(&self) -> Result<AgentResetReport> {
        self.discovery.obliterate(true, 1_000).await?;
        self.articles.obliterate(true, 1_000).await?;
        self.delivery.obliterate(true, 1_000).await?;

        let pattern = self.run_progress_pattern();
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
            delivery_queue: self.delivery_name.clone(),
            progress_trackers_removed,
        })
    }

    fn run_progress_key(&self, run_id: &str) -> String {
        format!(
            "{}:telemetry:{}:run:{run_id}",
            self.config.prefix, self.agent_scope
        )
    }

    fn run_progress_pattern(&self) -> String {
        format!(
            "{}:telemetry:{}:run:*",
            self.config.prefix, self.agent_scope
        )
    }

    async fn close_run(&self, run_id: &str) -> Result<Option<RunMetrics>> {
        const SCRIPT: &str = r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
            if tonumber(redis.call('HGET', KEYS[1], 'terminalSent')) == 1 then return {} end
            redis.call('HSET', KEYS[1], 'terminalSent', 1)
            redis.call('EXPIRE', KEYS[1], ARGV[1])
            return {
                tonumber(redis.call('HGET', KEYS[1], 'discovered')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'articleProcessed')
                    or redis.call('HGET', KEYS[1], 'processed')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'persisted')) or 0,
                tonumber(redis.call('HGET', KEYS[1], 'skipped')) or 0,
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
        if values.len() != 6 {
            return Ok(None);
        }
        Ok(Some(metrics_from_values(&values)?))
    }
}
