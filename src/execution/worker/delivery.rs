use crate::{
    articles::{ArticleIngestionClient, DeliveryIntent, DeliveryResult, DeliveryStatus, Outbox},
    error::Result,
    execution::{DeliveryJob, JobQueue, QueuedRunContext, Runtime},
};

use super::{processing_error, queued_duration_ms, queued_run_reporter};

pub(super) async fn process_delivery(
    runtime: &Runtime,
    jobs: &JobQueue,
    outbox: &Outbox,
    ingestion: Option<&ArticleIngestionClient>,
    payload: &DeliveryJob,
    job_id: &str,
    final_attempt: bool,
) -> bullmq::Result<()> {
    let Some(record) = outbox
        .get(payload.article_hash.as_str())
        .map_err(processing_error)?
    else {
        tracing::error!(
            article_hash = %payload.article_hash,
            "delivery article is missing from SQLite"
        );
        return finish_delivery(runtime, jobs, outbox, payload, job_id, false).await;
    };

    if record.status == DeliveryStatus::Forwarded {
        finish_delivery(runtime, jobs, outbox, payload, job_id, true).await?;
        return Ok(());
    }

    let Some(ingestion) = ingestion else {
        let message = "delivery queue requires an ingestion API endpoint";
        outbox
            .mark_failed(payload.article_hash.as_str(), message, false)
            .map_err(processing_error)?;
        tracing::error!(article_hash = %payload.article_hash, %message);
        return finish_delivery(runtime, jobs, outbox, payload, job_id, false).await;
    };
    match ingestion.deliver(&record.article).await {
        DeliveryResult::Delivered { .. } => {
            outbox
                .mark_forwarded(payload.article_hash.as_str())
                .map_err(processing_error)?;
            finish_delivery(runtime, jobs, outbox, payload, job_id, true).await
        }
        DeliveryResult::Failed {
            retryable, message, ..
        } => {
            outbox
                .mark_failed(payload.article_hash.as_str(), &message, retryable)
                .map_err(processing_error)?;
            if !retryable || final_attempt {
                finish_delivery(runtime, jobs, outbox, payload, job_id, false).await
            } else {
                Err(bullmq::Error::ProcessingError(message))
            }
        }
    }
}

async fn finish_delivery(
    runtime: &Runtime,
    jobs: &JobQueue,
    outbox: &Outbox,
    payload: &DeliveryJob,
    job_id: &str,
    succeeded: bool,
) -> bullmq::Result<()> {
    let update = jobs
        .record_delivery_result(
            &payload.run.run_id,
            job_id,
            usize::from(succeeded),
            usize::from(!succeeded),
        )
        .await
        .map_err(processing_error)?;
    outbox
        .complete_delivery_intent(
            &payload.run.run_id,
            payload.article_hash.as_str(),
            succeeded,
        )
        .map_err(processing_error)?;
    let Some(update) = update else {
        return Ok(());
    };
    let reporter = queued_run_reporter(runtime, &payload.run, &payload.source_id);

    if update.terminal {
        reporter
            .completed(update.metrics, queued_duration_ms(payload.run.started_at))
            .await;
    } else if jobs
        .claim_progress_publication(&payload.run.run_id)
        .await
        .map_err(processing_error)?
    {
        reporter.progress(update.metrics).await;
    }

    Ok(())
}

pub(super) async fn reconcile_delivery_intents(jobs: &JobQueue, outbox: &Outbox) -> Result<usize> {
    jobs.retry_failed_deliveries().await?;
    let intents = outbox.pending_delivery_intents(1_000)?;
    for intent in &intents {
        enqueue_delivery_intent(jobs, outbox, intent).await?;
    }
    if !intents.is_empty() {
        tracing::info!(count = intents.len(), "reconciled pending delivery intents");
    }
    Ok(intents.len())
}

pub(super) async fn enqueue_delivery_intent(
    jobs: &JobQueue,
    outbox: &Outbox,
    intent: &DeliveryIntent,
) -> Result<()> {
    jobs.enqueue_delivery(DeliveryJob {
        article_hash: intent.article_hash.clone(),
        source_id: intent.source_id.clone(),
        run: QueuedRunContext {
            run_id: intent.run_id.clone(),
            agent_id: intent.agent_id.clone(),
            started_at: intent.started_at,
        },
    })
    .await?;
    outbox.mark_delivery_intent_queued(&intent.run_id, intent.article_hash.as_str())
}
