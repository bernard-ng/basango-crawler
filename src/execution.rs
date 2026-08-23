//! Application orchestration.
//!
//! Source modules know how to collect; article modules know how to persist and
//! deliver. Execution modules coordinate those capabilities for each command.

mod queue;
mod sync;
mod worker;

pub(crate) use queue::{DiscoverJob, FetchJob, JobQueue};
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
};

/// Shared, immutable dependencies are placed in `Arc` so queued jobs can own a
/// cheap reference while running concurrently.
#[derive(Clone)]
pub(crate) struct Runtime {
    pub config: Arc<CrawlerConfig>,
    pub http: HttpClient,
}

impl Runtime {
    pub fn new(config: CrawlerConfig) -> Result<Self> {
        config.validate()?;
        let http = HttpClient::new(&config.http)?;
        Ok(Self {
            config: Arc::new(config),
            http,
        })
    }

    /// Ask the ingestion API for the last known article boundary when the caller did
    /// not explicitly provide a date range. API unavailability should not
    /// prevent a manual crawl, so failures are logged and treated as no range.
    pub async fn resolve_date_range(&self, request: &mut CrawlRequest) {
        if request.date_range.is_some() {
            return;
        }
        let Some(base) = &self.config.ingestion.endpoint else {
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
                tracing::warn!(status = %response.status, "publication-bound lookup failed");
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "publication-bound lookup failed");
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
        let start = match self.config.runtime.direction {
            UpdateDirection::Forward => dates.latest.unwrap_or(dates.earliest),
            UpdateDirection::Backward => dates.earliest,
        };
        request.date_range = DateRange::new(start, now).ok();
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourcePublicationBounds {
    earliest: DateTime<Utc>,
    latest: Option<DateTime<Utc>>,
}
