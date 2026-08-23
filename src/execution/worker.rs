//! BullMQ worker orchestration.

use std::sync::Arc;

use bullmq::worker::WorkerEvent;
use bullmq::{Job, Worker, WorkerOptions};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::{Duration, interval};

use crate::{
    articles::{ArticleIngestionClient, Outbox, ingest},
    error::{CrawlError, Result},
    execution::{DiscoverJob, FetchJob, JobQueue, Runtime},
    sources::SourceAdapter,
    telemetry::AgentReporter,
};

pub async fn run_worker(
    runtime: Runtime,
    queue_names: Vec<String>,
    concurrency: usize,
) -> Result<()> {
    let jobs = Arc::new(JobQueue::connect(&runtime.config.queue).await?);
    let queue_names = if queue_names.is_empty() {
        jobs.names().into_iter().map(str::to_owned).collect()
    } else {
        jobs.validate_names(&queue_names)?;
        queue_names
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

    let heartbeat_reporter = AgentReporter::new(&runtime.config.ingestion, runtime.http.clone());
    let heartbeat_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(15));
        loop {
            ticker.tick().await;
            heartbeat_reporter.heartbeat().await;
        }
    });

    tracing::info!(?queue_names, concurrency, "BullMQ crawler worker started");
    tokio::signal::ctrl_c().await.map_err(CrawlError::Io)?;
    heartbeat_task.abort();
    tracing::info!("shutdown requested; draining BullMQ workers");
    for worker in &workers {
        worker.close(30_000).await?;
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
    match job.name() {
        "discover-source" => {
            let payload: DiscoverJob = serde_json::from_value(job.data().clone())?;
            process_discovery(runtime, jobs, payload)
                .await
                .map(|count| json!({ "articlesQueued": count }))
                .map_err(processing_error)
        }
        "fetch-article" => {
            let payload: FetchJob = serde_json::from_value(job.data().clone())?;
            process_article(runtime, outbox, ingestion, payload)
                .await
                .map(|()| Value::Null)
                .map_err(processing_error)
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
) -> Result<usize> {
    let mut request = payload.request;
    runtime.resolve_date_range(&mut request).await;
    let source = runtime.config.source(&request.source_id)?;
    let mut adapter = SourceAdapter::new(source, runtime.http.clone());
    let articles = adapter.discover(&request).await?;
    let count = articles.len();
    for article in articles {
        jobs.enqueue_article(FetchJob {
            request: request.clone(),
            article,
        })
        .await?;
    }
    tracing::info!(source = %request.source_id, count, "discovery job queued articles");
    Ok(count)
}

async fn process_article(
    runtime: &Runtime,
    outbox: &Outbox,
    ingestion: Option<&ArticleIngestionClient>,
    payload: FetchJob,
) -> Result<()> {
    let source = runtime.config.source(&payload.request.source_id)?;
    let mut adapter = SourceAdapter::new(source, runtime.http.clone());
    let draft = match adapter.collect(&payload.article, &payload.request).await {
        Ok(draft) => draft,
        // These skips are deterministic and should count as successful jobs.
        Err(CrawlError::InvalidArticle(message)) => {
            tracing::info!(%message, url = %payload.article.url, "skipping invalid article");
            return Ok(());
        }
        Err(CrawlError::ArticleOutOfDateRange { .. }) => {
            tracing::info!(url = %payload.article.url, "skipping out-of-range article");
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let (_, status) = ingest(draft, outbox, ingestion).await?;
    tracing::info!(url = %payload.article.url, ?status, "article job completed");
    Ok(())
}

fn processing_error(error: CrawlError) -> bullmq::Error {
    bullmq::Error::ProcessingError(error.to_string())
}
