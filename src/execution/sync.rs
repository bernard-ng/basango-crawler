//! Direct crawl and outbox delivery workflows.

use std::time::Duration as StdDuration;
use std::time::Instant;

use chrono::Duration;
use tokio::{
    sync::oneshot,
    time::{Duration as TokioDuration, interval},
};
use uuid::Uuid;

use crate::{
    articles::{ArticleIngestionClient, DeliveryResult, IngestStatus, Outbox, ingest},
    domain::{ArticleDraft, CrawlRequest, SourceId},
    error::{CrawlError, Result},
    execution::Runtime,
    sources::SourceAdapter,
    telemetry::{RunMetrics, RunReporter},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrawlReport {
    pub collected: usize,
    pub stored: usize,
    pub delivered: usize,
    pub failed: usize,
}

/// Run one source from listing discovery through article ingestion.
pub async fn crawl_now(runtime: &Runtime, mut request: CrawlRequest) -> Result<CrawlReport> {
    runtime.config.prepare_request(&mut request)?;
    let reporter = RunReporter::new(
        &runtime.config.ingestion,
        runtime.http.clone(),
        request.source_id.as_str(),
        &runtime.agent_id,
    );
    let heartbeat_reporter = reporter.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut ticker = interval(TokioDuration::from_secs(15));
        loop {
            ticker.tick().await;
            heartbeat_reporter.heartbeat().await;
        }
    });
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let shutdown_task = tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                let _ = shutdown_sender.send(());
            }
            Err(error) => tracing::warn!(%error, "could not listen for Ctrl-C"),
        }
    });
    let result = crawl_with_reporter(runtime, &mut request, &reporter, shutdown_receiver).await;
    shutdown_task.abort();
    heartbeat_task.abort();
    result
}

enum CrawlEvent {
    Draft,
    ShutdownRequested,
    ShutdownUnavailable,
}

async fn crawl_with_reporter(
    runtime: &Runtime,
    request: &mut CrawlRequest,
    reporter: &RunReporter,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<CrawlReport> {
    let started_at = Instant::now();
    reporter.preparing().await;
    let mut report = CrawlReport::default();

    let outcome: Result<()> = async {
        let source = runtime.config.source(&request.source_id)?;
        runtime.resolve_date_range(request).await;
        let adapter = SourceAdapter::new(source, runtime.http.clone());
        let outbox = Outbox::open(&runtime.config.sqlite_path(), true)?;
        let ingestion =
            ArticleIngestionClient::new(&runtime.config.ingestion, runtime.http.clone())?;

        reporter.started().await;
        let mut drafts = adapter.stream(request.clone());
        let mut shutdown_available = true;
        let mut interrupted = false;

        loop {
            let mut next_draft: Option<Option<Result<ArticleDraft>>> = None;
            let event = tokio::select! {
                signal = &mut shutdown, if shutdown_available => match signal {
                    Ok(()) => CrawlEvent::ShutdownRequested,
                    Err(_) => CrawlEvent::ShutdownUnavailable,
                },
                item = drafts.recv() => {
                    next_draft = Some(item);
                    CrawlEvent::Draft
                },
            };
            let item = match event {
                CrawlEvent::Draft => match next_draft.expect("draft event carries a value") {
                    Some(item) => item,
                    None => break,
                },
                CrawlEvent::ShutdownRequested => {
                    interrupted = true;
                    break;
                }
                CrawlEvent::ShutdownUnavailable => {
                    shutdown_available = false;
                    continue;
                }
            };
            let draft = item?;
            report.collected += 1;
            match ingest(draft, &outbox, ingestion.as_ref()).await {
                Ok((_, status)) => {
                    report.stored += 1;
                    if matches!(status, IngestStatus::Forwarded | IngestStatus::AlreadyForwarded) {
                        report.delivered += 1;
                    }
                    if status == IngestStatus::DeliveryFailed {
                        report.failed += 1;
                    }
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::error!(%error, source = %request.source_id, "article ingestion failed");
                }
            }
            reporter.progress((&report).into()).await;
        }

        if interrupted {
            tracing::info!(
                source = %request.source_id,
                collected = report.collected,
                stored = report.stored,
                delivered = report.delivered,
                failed = report.failed,
                "Ctrl-C received; completing crawl with persisted progress"
            );
        }

        Ok(())
    }
    .await;

    if let Err(error) = outcome {
        reporter
            .failed(
                (&report).into(),
                elapsed_millis(started_at),
                error.to_string(),
            )
            .await;
        return Err(error);
    }

    reporter
        .completed((&report).into(), elapsed_millis(started_at))
        .await;
    Ok(report)
}

/// Claim and deliver pending/failed outbox rows.
pub async fn forward_pending(
    runtime: &Runtime,
    source_id: Option<&SourceId>,
    limit: usize,
    retry_all: bool,
) -> Result<CrawlReport> {
    let outbox_path = runtime.config.sqlite_path();
    if !Outbox::exists(&outbox_path) {
        return Err(CrawlError::Configuration(format!(
            "SQLite outbox does not exist: {}",
            outbox_path.display()
        )));
    }
    let ingestion = ArticleIngestionClient::new(&runtime.config.ingestion, runtime.http.clone())?
        .ok_or_else(|| {
        CrawlError::Configuration(
            "delivery requires BASANGO_API_CRAWLER_ENDPOINT or ingestion.endpoint".into(),
        )
    })?;

    let claim_id = format!("{}:{}", std::process::id(), Uuid::now_v7());
    let outbox = Outbox::open(&outbox_path, false)?;
    let articles = outbox.claim(
        &claim_id,
        source_id.map(SourceId::as_str),
        limit,
        retry_all,
        Duration::from_std(StdDuration::from_secs(15 * 60))
            .expect("15 minutes fits Chrono's duration"),
    )?;
    let mut report = CrawlReport {
        collected: articles.len(),
        stored: articles.len(),
        ..CrawlReport::default()
    };
    tracing::info!(
        claimed = articles.len(),
        retry_all,
        source = source_id.map(SourceId::as_str).unwrap_or("<all>"),
        "claimed outbox articles for delivery"
    );

    for record in articles {
        let delivery = tokio::select! {
            delivery = ingestion.deliver(&record.article) => Some(delivery),
            signal = tokio::signal::ctrl_c() => match signal {
                Ok(()) => None,
                Err(error) => {
                    tracing::warn!(%error, "could not listen for Ctrl-C during outbox delivery");
                    Some(ingestion.deliver(&record.article).await)
                }
            },
        };
        let Some(delivery) = delivery else {
            let released = outbox.release_claim(&claim_id)?;
            tracing::info!(
                delivered = report.delivered,
                failed = report.failed,
                released,
                "Ctrl-C received; released remaining outbox claims"
            );
            return Ok(report);
        };

        match delivery {
            DeliveryResult::Delivered { .. } => {
                outbox.mark_forwarded(&record.article.hash)?;
                report.delivered += 1;
            }
            DeliveryResult::Failed {
                retryable, message, ..
            } => {
                tracing::warn!(
                    url = %record.article.link,
                    retryable,
                    error = %message,
                    "outbox article delivery failed"
                );
                outbox.mark_failed(&record.article.hash, &message, retryable)?;
                report.failed += 1;
            }
        }
    }
    Ok(report)
}

impl From<&CrawlReport> for RunMetrics {
    fn from(report: &CrawlReport) -> Self {
        Self {
            articles_discovered: report.collected,
            articles_persisted: report.stored,
            articles_delivered: report.delivered,
            articles_failed: report.failed,
        }
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "../../tests/unit/execution/sync.rs"]
mod tests;
