use crate::{
    error::Result,
    execution::{DiscoverJob, FetchJob, JobQueue, Runtime},
    sources::SourceAdapter,
    telemetry::RunMetrics,
};

use super::{queued_duration_ms, queued_run_reporter};

pub(super) async fn process_discovery(
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
            let mut article_job_ids = Vec::with_capacity(batch.articles.len());
            for article in batch.articles {
                article_job_ids.push(
                    jobs.enqueue_article(FetchJob {
                        request: request.clone(),
                        article,
                        run: payload.run.clone(),
                    })
                    .await?,
                );
            }
            let Some(metrics) = jobs
                .record_discovery_batch(&payload.run.run_id, &batch.id, &article_job_ids)
                .await?
            else {
                return Ok(count);
            };
            count = metrics.articles_discovered;

            if jobs.claim_progress_publication(&payload.run.run_id).await? {
                reporter.progress(metrics).await;
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
