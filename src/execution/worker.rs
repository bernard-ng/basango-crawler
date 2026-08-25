//! BullMQ worker orchestration.

use std::sync::Arc;

use bullmq::worker::WorkerEvent;
use bullmq::{Job, Worker, WorkerOptions};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::{Duration, interval};

use crate::{
    articles::{ArticleIngestionClient, IngestStatus, Outbox, ingest},
    error::{CrawlError, Result},
    execution::{DiscoverJob, FetchJob, JobQueue, Runtime},
    sources::SourceAdapter,
    telemetry::{AgentReporter, RunMetrics, RunReporter},
};

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
    let permits = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut workers = Vec::with_capacity(queue_names.len());

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
        tokio::spawn(log_worker_events(worker.clone()));
        workers.push(worker);
    }

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

    tracing::info!(
        agent_id = runtime.agent_id,
        queue_prefix = runtime.config.queue.prefix,
        ?queue_names,
        concurrency,
        "BullMQ crawler worker started"
    );
    tokio::signal::ctrl_c().await.map_err(CrawlError::Io)?;
    heartbeat_task.abort();
    tracing::info!("shutdown requested; draining BullMQ workers");
    let mut close_error = None;
    for worker in &workers {
        if let Err(error) = worker.close(30_000).await {
            tracing::warn!(%error, "BullMQ worker did not drain cleanly");
            close_error = Some(error);
        }
    }
    let open_runs = jobs.complete_open_runs().await?;
    for open_run in &open_runs {
        queued_run_reporter(&runtime, &open_run.run, &open_run.source_id)
            .completed(
                open_run.metrics,
                queued_duration_ms(open_run.run.started_at),
            )
            .await;
    }
    tracing::info!(
        runs_completed = open_runs.len(),
        "worker shutdown completed open queued runs"
    );
    if let Some(error) = close_error {
        return Err(error.into());
    }
    Ok(())
}

async fn log_worker_events(worker: Arc<Worker>) {
    while let Some(event) = worker.next_event().await {
        match event {
            WorkerEvent::Failed { job_id, error } => {
                tracing::error!(job_id, %error, "BullMQ job failed");
            }
            WorkerEvent::Error(error) => tracing::error!(%error, "BullMQ worker error"),
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
            match process_article(runtime, outbox, ingestion, &payload).await {
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
                            ArticleRunOutcome {
                                failed: 1,
                                ..ArticleRunOutcome::default()
                            },
                        )
                        .await?;
                    }
                    Err(processing_error(error))
                }
            }
        }
        name => Err(bullmq::Error::Unrecoverable(format!(
            "unknown crawler job '{name}'"
        ))),
    }
}

async fn process_discovery(
    runtime: &Runtime,
    jobs: &JobQueue,
    payload: DiscoverJob,
    final_attempt: bool,
) -> Result<usize> {
    let mut request = payload.request;
    runtime.config.prepare_request(&mut request)?;
    let reporter = queued_run_reporter(runtime, &payload.run, &request.source_id);
    reporter.started().await;
    let result: Result<usize> = async {
        runtime.resolve_date_range(&mut request).await;
        let source = runtime.config.source(&request.source_id)?;
        let adapter = SourceAdapter::new(source, runtime.http.clone());
        let mut batches = adapter.stream_discovery(request.clone());
        let mut count = 0usize;
        while let Some(batch) = batches.recv().await {
            let batch = batch?;
            if !jobs.run_is_open(&payload.run.run_id).await? {
                tracing::info!(
                    run_id = payload.run.run_id,
                    source = %request.source_id,
                    "queued run was completed during discovery; stopping"
                );
                return Ok(count);
            }
            let batch_count = batch.articles.len();
            let Some(metrics) = jobs
                .record_discovery_batch(&payload.run.run_id, &batch.id, batch_count)
                .await?
            else {
                return Ok(count);
            };
            count = metrics.articles_discovered;
            reporter.progress(metrics).await;
            for article in batch.articles {
                jobs.enqueue_article(FetchJob {
                    request: request.clone(),
                    article,
                    run: payload.run.clone(),
                })
                .await?;
            }
        }

        if let Some(update) = jobs.finish_discovery(&payload.run.run_id).await?
            && update.terminal
        {
            reporter
                .completed(update.metrics, queued_duration_ms(payload.run.started_at))
                .await;
        }
        tracing::info!(source = %request.source_id, count, "discovery job queued articles");
        Ok(count)
    }
    .await;

    if let Err(error) = &result {
        if final_attempt {
            let metrics = match jobs.fail_run(&payload.run.run_id).await {
                Ok(Some(metrics)) => metrics,
                Ok(None) => RunMetrics::default(),
                Err(tracking_error) => {
                    tracing::warn!(
                        run_id = payload.run.run_id,
                        %tracking_error,
                        "could not close queued run progress tracker"
                    );
                    RunMetrics::default()
                }
            };
            reporter
                .failed(
                    metrics,
                    queued_duration_ms(payload.run.started_at),
                    error.to_string(),
                )
                .await;
        } else {
            tracing::warn!(
                run_id = payload.run.run_id,
                %error,
                "discovery job attempt failed; BullMQ will retry it"
            );
        }
    }
    result
}

#[derive(Debug, Clone, Copy, Default)]
struct ArticleRunOutcome {
    persisted: usize,
    delivered: usize,
    failed: usize,
}

async fn process_article(
    runtime: &Runtime,
    outbox: &Outbox,
    ingestion: Option<&ArticleIngestionClient>,
    payload: &FetchJob,
) -> Result<ArticleRunOutcome> {
    let source = runtime.config.source(&payload.request.source_id)?;
    let mut adapter = SourceAdapter::new(source, runtime.http.clone());
    let draft = match adapter.collect(&payload.article, &payload.request).await {
        Ok(draft) => draft,
        // These skips are deterministic and should count as successful jobs.
        Err(CrawlError::InvalidArticle(message)) => {
            tracing::info!(%message, url = %payload.article.url, "skipping invalid article");
            return Ok(ArticleRunOutcome::default());
        }
        Err(CrawlError::ArticleOutOfDateRange { .. }) => {
            tracing::info!(url = %payload.article.url, "skipping out-of-range article");
            return Ok(ArticleRunOutcome::default());
        }
        Err(error) => return Err(error),
    };
    let (_, status) = ingest(draft, outbox, ingestion).await?;
    tracing::info!(url = %payload.article.url, ?status, "article job completed");
    Ok(match status {
        IngestStatus::Persisted => ArticleRunOutcome {
            persisted: 1,
            ..ArticleRunOutcome::default()
        },
        IngestStatus::AlreadyForwarded | IngestStatus::Forwarded => ArticleRunOutcome {
            persisted: 1,
            delivered: 1,
            failed: 0,
        },
        IngestStatus::DeliveryFailed => ArticleRunOutcome {
            persisted: 1,
            delivered: 0,
            failed: 1,
        },
    })
}

async fn report_article_result(
    runtime: &Runtime,
    jobs: &JobQueue,
    payload: &FetchJob,
    job_id: &str,
    outcome: ArticleRunOutcome,
) -> bullmq::Result<()> {
    let update = jobs
        .record_run_result(
            &payload.run.run_id,
            job_id,
            outcome.persisted,
            outcome.delivered,
            outcome.failed,
        )
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

fn queued_run_reporter(
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

fn queued_duration_ms(started_at: chrono::DateTime<chrono::Utc>) -> u64 {
    chrono::Utc::now()
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64
}

fn processing_error(error: CrawlError) -> bullmq::Error {
    bullmq::Error::ProcessingError(error.to_string())
}
