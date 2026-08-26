//! BullMQ worker orchestration.

mod delivery;
mod discovery;

use std::{collections::VecDeque, sync::Arc, time::Instant};

use bullmq::worker::WorkerEvent;
use bullmq::{Job, Worker, WorkerOptions};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Duration, interval};

use crate::{
    articles::{ArticleIngestionClient, DeliveryIntent, DeliveryStatus, Outbox, normalize},
    error::{CrawlError, Result},
    execution::{DeliveryJob, DiscoverJob, FetchJob, JobQueue, QueuedArticleResult, Runtime},
    sources::SourceAdapter,
    telemetry::{AgentReporter, RunReporter},
};

use delivery::{enqueue_delivery_intent, process_delivery, reconcile_delivery_intents};
use discovery::process_discovery;

const REDIS_ERROR_RESTART_THRESHOLD: usize = 4;
const REDIS_ERROR_WINDOW: Duration = Duration::from_secs(60);

pub async fn run_worker(
    runtime: Runtime,
    queue_names: Vec<String>,
    concurrency: usize,
) -> Result<()> {
    let jobs = Arc::new(JobQueue::connect(&runtime.config.queue, &runtime.agent_id).await?);
    let queue_names = if queue_names.is_empty() {
        jobs.names().into_iter().map(str::to_owned).collect()
    } else {
        jobs.validate_names(&queue_names)?;
        jobs.resolve_names(&queue_names)
    };

    let outbox = Outbox::open(&runtime.config.sqlite_path(), true)?;
    let ingestion = ArticleIngestionClient::new(&runtime.config.ingestion, runtime.http.clone())?;
    reconcile_delivery_intents(&jobs, &outbox).await?;
    let permits = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut workers = Vec::with_capacity(queue_names.len());
    let (worker_error_sender, mut worker_error_receiver) = mpsc::unbounded_channel();

    for queue_name in &queue_names {
        let worker_runtime = runtime.clone();
        let jobs = jobs.clone();
        let outbox = outbox.clone();
        let ingestion = ingestion.clone();
        let permits = permits.clone();
        let processor = move |job: Job, _cancellation| {
            let runtime = worker_runtime.clone();
            let jobs = jobs.clone();
            let outbox = outbox.clone();
            let ingestion = ingestion.clone();
            let permits = permits.clone();
            async move {
                let _permit = permits.acquire_owned().await.map_err(|_| {
                    bullmq::Error::ProcessingError("worker concurrency gate closed".into())
                })?;
                process_job(&runtime, &jobs, &outbox, ingestion.as_ref(), job).await
            }
        };
        let options = WorkerOptions::new()
            .connection(super::queue::redis_options(&runtime.config.queue))
            .prefix(runtime.config.queue.prefix.clone())
            .name("crawler")
            // The shared semaphore is the authoritative process-wide limit.
            // This value lets either queue use the full capacity while idle.
            .concurrency(concurrency.max(1));
        let worker = Arc::new(Worker::with_options(queue_name, processor, options).await?);
        tokio::spawn(log_worker_events(
            worker.clone(),
            worker_error_sender.clone(),
        ));
        workers.push(worker);
    }
    drop(worker_error_sender);

    let heartbeat_reporter = AgentReporter::new(
        &runtime.config.ingestion,
        runtime.http.clone(),
        &runtime.agent_id,
    );
    let heartbeat_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(15));
        loop {
            ticker.tick().await;
            heartbeat_reporter.heartbeat().await;
        }
    });
    let reconciliation_jobs = jobs.clone();
    let reconciliation_outbox = outbox.clone();
    let reconciliation_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if let Err(error) =
                reconcile_delivery_intents(&reconciliation_jobs, &reconciliation_outbox).await
            {
                tracing::warn!(%error, "could not reconcile pending delivery intents");
            }
        }
    });

    tracing::info!(
        agent_id = runtime.agent_id,
        queue_prefix = runtime.config.queue.prefix,
        ?queue_names,
        concurrency,
        "BullMQ crawler worker started"
    );
    let mut redis_errors = VecDeque::new();
    let fatal_error = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(CrawlError::Io)?;
                break None;
            }
            error = worker_error_receiver.recv() => {
                let Some(error) = error else {
                    break Some("all BullMQ event streams closed unexpectedly".to_owned());
                };
                let now = Instant::now();
                redis_errors.push_back(now);
                while redis_errors
                    .front()
                    .is_some_and(|occurred_at| now.duration_since(*occurred_at) > REDIS_ERROR_WINDOW)
                {
                    redis_errors.pop_front();
                }
                if redis_errors.len() >= REDIS_ERROR_RESTART_THRESHOLD {
                    break Some(format!(
                        "BullMQ Redis connection remained unhealthy after {} errors in {} seconds: {error}",
                        redis_errors.len(),
                        REDIS_ERROR_WINDOW.as_secs(),
                    ));
                }
            }
        }
    };
    heartbeat_task.abort();
    reconciliation_task.abort();
    if let Some(error) = &fatal_error {
        tracing::error!(%error, "worker restart requested; preserving queued run progress");
    } else {
        tracing::info!("shutdown requested; draining BullMQ workers");
    }
    let mut close_error = None;
    for worker in &workers {
        if let Err(error) = worker.close(30_000).await {
            tracing::warn!(%error, "BullMQ worker did not drain cleanly");
            close_error = Some(error);
        }
    }
    tracing::info!("worker shutdown preserved open queued runs for the next worker");
    if let Some(error) = close_error {
        return Err(error.into());
    }
    if let Some(error) = fatal_error {
        return Err(CrawlError::Queue(error));
    }
    Ok(())
}

async fn log_worker_events(worker: Arc<Worker>, error_sender: mpsc::UnboundedSender<String>) {
    while let Some(event) = worker.next_event().await {
        match event {
            WorkerEvent::Failed { job_id, error } => {
                tracing::error!(job_id, %error, "BullMQ job failed");
            }
            WorkerEvent::Error(error) => {
                tracing::error!(%error, "BullMQ worker error");
                if is_redis_connection_error(&error) {
                    let _ = error_sender.send(error);
                }
            }
            WorkerEvent::Stalled { job_id } => {
                tracing::warn!(job_id, "BullMQ recovered a stalled job");
            }
            WorkerEvent::Completed { job_id, .. } => {
                tracing::debug!(job_id, "BullMQ job completed");
            }
            WorkerEvent::Closed => break,
            _ => {}
        }
    }
}

fn is_redis_connection_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("redis")
        && [
            "broken pipe",
            "connection closed",
            "connection is closed",
            "connection refused",
            "connection reset",
            "network is unreachable",
            "no route to host",
            "timed out",
            "unexpected eof",
        ]
        .iter()
        .any(|fragment| error.contains(fragment))
}

async fn process_job(
    runtime: &Runtime,
    jobs: &JobQueue,
    outbox: &Outbox,
    ingestion: Option<&ArticleIngestionClient>,
    job: Job,
) -> bullmq::Result<Value> {
    tracing::info!(
        agent_id = runtime.agent_id,
        job_id = job.id(),
        job_name = job.name(),
        "processing BullMQ job"
    );
    match job.name() {
        "discover-source" => {
            let final_attempt = job.attempts_made() + 1 >= job.opts().attempts.unwrap_or(1);
            let payload: DiscoverJob = serde_json::from_value(job.data().clone())?;
            if !jobs
                .run_is_open(&payload.run.run_id)
                .await
                .map_err(processing_error)?
            {
                tracing::info!(
                    run_id = payload.run.run_id,
                    "skipping discovery job for a completed run"
                );
                return Ok(json!({ "articlesQueued": 0 }));
            }
            process_discovery(runtime, jobs, payload, final_attempt)
                .await
                .map(|count| json!({ "articlesQueued": count }))
                .map_err(processing_error)
        }
        "fetch-article" => {
            let job_id = job.id().to_owned();
            let final_attempt = job.attempts_made() + 1 >= job.opts().attempts.unwrap_or(1);
            let payload: FetchJob = serde_json::from_value(job.data().clone())?;
            if !jobs
                .run_is_open(&payload.run.run_id)
                .await
                .map_err(processing_error)?
            {
                tracing::info!(
                    run_id = payload.run.run_id,
                    url = %payload.article.url,
                    "skipping article job for a completed run"
                );
                return Ok(Value::Null);
            }
            match process_article(runtime, jobs, outbox, &payload).await {
                Ok(outcome) => {
                    report_article_result(runtime, jobs, &payload, &job_id, outcome).await?;
                    Ok(Value::Null)
                }
                Err(error) => {
                    if final_attempt {
                        report_article_result(
                            runtime,
                            jobs,
                            &payload,
                            &job_id,
                            QueuedArticleResult {
                                failed: 1,
                                ..QueuedArticleResult::default()
                            },
                        )
                        .await?;
                    }
                    Err(processing_error(error))
                }
            }
        }
        "deliver-article" => {
            let job_id = job.id().to_owned();
            let final_attempt = job.attempts_made() + 1 >= job.opts().attempts.unwrap_or(1);
            let payload: DeliveryJob = serde_json::from_value(job.data().clone())?;
            process_delivery(
                runtime,
                jobs,
                outbox,
                ingestion,
                &payload,
                &job_id,
                final_attempt,
            )
            .await?;
            Ok(Value::Null)
        }
        name => Err(bullmq::Error::Unrecoverable(format!(
            "unknown crawler job '{name}'"
        ))),
    }
}

async fn process_article(
    runtime: &Runtime,
    jobs: &JobQueue,
    outbox: &Outbox,
    payload: &FetchJob,
) -> Result<QueuedArticleResult> {
    let source = runtime.config.source(&payload.request.source_id)?;
    let mut adapter = SourceAdapter::new(source, runtime.http.clone());
    let draft = match adapter.collect(&payload.article, &payload.request).await {
        Ok(draft) => draft,
        // These skips are deterministic and should count as successful jobs.
        Err(CrawlError::InvalidArticle(message)) => {
            tracing::info!(%message, url = %payload.article.url, "skipping invalid article");
            return Ok(QueuedArticleResult {
                skipped: 1,
                ..QueuedArticleResult::default()
            });
        }
        Err(CrawlError::ArticleOutOfDateRange { .. }) => {
            tracing::info!(url = %payload.article.url, "skipping out-of-range article");
            return Ok(QueuedArticleResult {
                skipped: 1,
                ..QueuedArticleResult::default()
            });
        }
        Err(error) => return Err(error),
    };
    let article = normalize(draft)?;
    let intent = DeliveryIntent {
        run_id: payload.run.run_id.clone(),
        agent_id: payload.run.agent_id.clone(),
        source_id: payload.request.source_id.clone(),
        article_hash: article.hash.clone(),
        started_at: payload.run.started_at,
    };
    let status = outbox.save_with_delivery_intent(&article, &intent)?;
    tracing::info!(url = %payload.article.url, ?status, "article job completed");
    Ok(match status {
        DeliveryStatus::Forwarded => {
            if outbox.has_delivery_intent(&payload.run.run_id, article.hash.as_str())? {
                QueuedArticleResult {
                    persisted: 1,
                    delivery_expected: 1,
                    ..QueuedArticleResult::default()
                }
            } else {
                QueuedArticleResult {
                    persisted: 1,
                    delivered: 1,
                    failed: 0,
                    delivery_expected: 0,
                    skipped: 0,
                }
            }
        }
        DeliveryStatus::Pending | DeliveryStatus::Failed => {
            enqueue_delivery_intent(jobs, outbox, &intent).await?;
            QueuedArticleResult {
                persisted: 1,
                delivery_expected: 1,
                ..QueuedArticleResult::default()
            }
        }
    })
}

async fn report_article_result(
    runtime: &Runtime,
    jobs: &JobQueue,
    payload: &FetchJob,
    job_id: &str,
    outcome: QueuedArticleResult,
) -> bullmq::Result<()> {
    let update = jobs
        .record_article_result(&payload.run.run_id, job_id, outcome)
        .await
        .map_err(processing_error)?;
    let Some(update) = update else {
        return Ok(());
    };
    let reporter = queued_run_reporter(runtime, &payload.run, &payload.request.source_id);
    reporter.progress(update.metrics).await;
    if update.terminal {
        reporter
            .completed(update.metrics, queued_duration_ms(payload.run.started_at))
            .await;
    }
    Ok(())
}

pub(super) fn queued_run_reporter(
    runtime: &Runtime,
    run: &super::queue::QueuedRunContext,
    source_id: &crate::domain::SourceId,
) -> RunReporter {
    RunReporter::with_context(
        &runtime.config.ingestion,
        runtime.http.clone(),
        run.run_id.clone(),
        run.agent_id.clone(),
        source_id.to_string(),
    )
}

pub(super) fn queued_duration_ms(started_at: chrono::DateTime<chrono::Utc>) -> u64 {
    chrono::Utc::now()
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64
}

pub(super) fn processing_error(error: CrawlError) -> bullmq::Error {
    bullmq::Error::ProcessingError(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::is_redis_connection_error;

    #[test]
    fn classifies_transient_redis_connection_errors() {
        assert!(is_redis_connection_error("redis error: broken pipe"));
        assert!(is_redis_connection_error(
            "Redis error: connection reset by peer"
        ));
        assert!(!is_redis_connection_error("redis error: WRONGTYPE"));
        assert!(!is_redis_connection_error("HTTP request timed out"));
    }
}
