use std::collections::HashSet;

use crate::error::{CrawlError, Result};

use super::{CrawlerConfig, schema};

pub(super) fn validate(config: &CrawlerConfig) -> Result<()> {
    schema::validate(&serde_json::to_value(config)?)?;

    if config.queue.queues.discovery == config.queue.queues.articles {
        return Err(CrawlError::Configuration(
            "discovery and article queue names must be distinct".into(),
        ));
    }
    if config.ingestion.endpoint.is_some() && config.ingestion.token.trim().is_empty() {
        return Err(CrawlError::Configuration(
            "ingestion.token is required when ingestion.endpoint is configured".into(),
        ));
    }
    if config.http.backoff.max < config.http.backoff.initial {
        return Err(CrawlError::Configuration(
            "http.backoff.max must be greater than or equal to http.backoff.initial".into(),
        ));
    }

    let mut ids = HashSet::new();
    for source in &config.sources {
        if !ids.insert(source.id()) {
            return Err(CrawlError::Configuration(format!(
                "duplicate source id '{}'",
                source.id()
            )));
        }
    }
    Ok(())
}
