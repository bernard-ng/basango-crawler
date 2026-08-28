//! Registration and archive-size synchronization for configured sources.

use std::sync::Arc;

use serde::Serialize;
use tokio::{sync::Semaphore, task::JoinSet};

use crate::{
    articles::endpoint_url, config::SourceConfig, error::Result, execution::Runtime,
    sources::SourceAdapter,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceSyncItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_articles: Option<usize>,
    kind: String,
    name: String,
    url: String,
}

#[derive(Serialize)]
struct SourceSyncPayload<'a> {
    sources: &'a [SourceSyncItem],
}

pub(super) async fn synchronize(runtime: &Runtime) -> Result<()> {
    let Some(base) = &runtime.config.ingestion.endpoint else {
        tracing::debug!("ingestion API is disabled; skipping source synchronization");
        return Ok(());
    };
    let endpoint = endpoint_url(base, "ingest/sources/sync")?;
    let registrations = runtime
        .config
        .sources
        .iter()
        .map(SourceSyncItem::from)
        .collect::<Vec<_>>();

    publish(runtime, &endpoint, &registrations).await?;
    tracing::info!(
        sources = registrations.len(),
        "registered configured crawler sources"
    );

    let estimates = estimate_sources(runtime).await;
    if estimates.is_empty() {
        tracing::warn!("no source archive estimates were available to synchronize");
        return Ok(());
    }

    publish(runtime, &endpoint, &estimates).await?;
    tracing::info!(
        estimated = estimates.len(),
        total = registrations.len(),
        "synchronized source archive estimates"
    );

    Ok(())
}

async fn estimate_sources(runtime: &Runtime) -> Vec<SourceSyncItem> {
    let permits = Arc::new(Semaphore::new(
        runtime.config.runtime.worker_concurrency.max(1),
    ));
    let mut tasks = JoinSet::new();

    for source in runtime.config.sources.iter().cloned() {
        let http = runtime.http.clone();
        let permits = permits.clone();

        tasks.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .expect("source estimate semaphore remains open");
            let registration = SourceSyncItem::from(&source);
            let estimate = SourceAdapter::new(source, http)
                .estimate_total_articles()
                .await;

            (registration, estimate)
        });
    }

    let mut estimates = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((mut registration, Ok(estimate))) => {
                registration.estimated_articles = Some(estimate);
                estimates.push(registration);
            }
            Ok((registration, Err(error))) => tracing::warn!(
                source = registration.name,
                %error,
                "could not estimate source archive size"
            ),
            Err(error) => tracing::warn!(%error, "source archive estimate task failed"),
        }
    }
    estimates.sort_unstable_by(|left, right| left.name.cmp(&right.name));

    estimates
}

async fn publish(runtime: &Runtime, endpoint: &url::Url, sources: &[SourceSyncItem]) -> Result<()> {
    let headers = [("Authorization", runtime.config.ingestion.token.as_str())];
    runtime
        .http
        .post_json(endpoint, &headers, &SourceSyncPayload { sources })
        .await?
        .require_success()?;

    Ok(())
}

impl From<&SourceConfig> for SourceSyncItem {
    fn from(source: &SourceConfig) -> Self {
        Self {
            estimated_articles: None,
            kind: source.kind().to_owned(),
            name: source.id().to_string(),
            url: source.url().to_string(),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/execution/source_sync.rs"]
mod tests;
