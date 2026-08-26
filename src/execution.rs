//! Application orchestration.
//!
//! Source modules know how to collect; article modules know how to persist and
//! deliver. Execution modules coordinate those capabilities for each command.

mod queue;
mod sync;
mod worker;

pub(crate) use queue::{
    AgentResetReport as QueueResetReport, DeliveryJob, DiscoverJob, FetchJob, JobQueue,
    QueueSnapshot, QueuedRunContext,
};
pub use sync::CrawlReport;
pub(crate) use sync::{crawl_now, forward_pending};
pub(crate) use worker::run_worker;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    articles::endpoint_url,
    config::CrawlerConfig,
    domain::{CrawlRequest, DateRange, UpdateDirection},
    error::Result,
    http::HttpClient,
    telemetry::agent_id,
};

/// Shared, immutable dependencies are placed in `Arc` so queued jobs can own a
/// cheap reference while running concurrently.
#[derive(Clone)]
pub(crate) struct Runtime {
    pub config: Arc<CrawlerConfig>,
    pub http: HttpClient,
    pub agent_id: String,
}

impl Runtime {
    pub fn new(config: CrawlerConfig) -> Result<Self> {
        config.validate()?;
        let agent_id = agent_id()?;
        let http = HttpClient::new(&config.http)?;
        Ok(Self {
            config: Arc::new(config),
            http,
            agent_id,
        })
    }

    /// Ask the ingestion API for the last known article boundary when the caller did
    /// not explicitly provide a date range. API unavailability should not
    /// prevent a manual crawl, so failures are logged and treated as no range.
    pub async fn resolve_date_range(&self, request: &mut CrawlRequest) {
        if let Some(range) = request.date_range {
            tracing::info!(
                source = %request.source_id,
                start = %range.start,
                end = %range.end,
                "using requested publication date range"
            );
            return;
        }
        let Some(base) = &self.config.ingestion.endpoint else {
            tracing::debug!(
                source = %request.source_id,
                "ingestion API is disabled; crawling without a publication date filter"
            );
            return;
        };
        let Ok(endpoint) = endpoint_url(base, "ingest/sources/publication-bounds") else {
            return;
        };
        let headers = [("Authorization", self.config.ingestion.token.as_str())];
        let payload = serde_json::json!({ "name": request.source_id.as_str() });
        let response = match self.http.post_json(&endpoint, &headers, &payload).await {
            Ok(response) if response.is_success() => response,
            Ok(response) => {
                tracing::warn!(
                    source = %request.source_id,
                    status = %response.status,
                    body = %response.body_lossy(),
                    "publication-bound lookup failed; crawling without a date filter"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    source = %request.source_id,
                    %error,
                    "publication-bound lookup failed; crawling without a date filter"
                );
                return;
            }
        };
        let dates: SourcePublicationBounds = match response.json() {
            Ok(dates) => dates,
            Err(error) => {
                tracing::warn!(%error, "ingestion API returned invalid publication bounds");
                return;
            }
        };

        let now = Utc::now();
        let direction = request.direction.unwrap_or(self.config.runtime.direction);
        match automatic_date_range(direction, &dates, now) {
            Ok(Some(range)) => {
                tracing::info!(
                    source = %request.source_id,
                    ?direction,
                    start = %range.start,
                    end = %range.end,
                    "using ingestion API publication date range"
                );
                request.date_range = Some(range);
            }
            Ok(None) => tracing::info!(
                source = %request.source_id,
                ?direction,
                "source has no stored publication bounds; crawling without a date filter"
            ),
            Err(error) => tracing::warn!(
                source = %request.source_id,
                ?direction,
                %error,
                "invalid publication bounds; crawling without a date filter"
            ),
        }
    }
}

fn automatic_date_range(
    direction: UpdateDirection,
    dates: &SourcePublicationBounds,
    now: DateTime<Utc>,
) -> Result<Option<DateRange>> {
    match direction {
        UpdateDirection::Forward => match dates.latest.or(dates.earliest) {
            Some(start) => DateRange::new(start, now).map(Some),
            None => Ok(None),
        },
        UpdateDirection::Backward => match dates.earliest {
            Some(end) => DateRange::new(DateTime::<Utc>::UNIX_EPOCH, end).map(Some),
            None => Ok(None),
        },
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourcePublicationBounds {
    earliest: Option<DateTime<Utc>>,
    latest: Option<DateTime<Utc>>,
}

#[cfg(test)]
#[path = "../tests/unit/execution.rs"]
mod tests;
