use std::collections::HashSet;

use crate::error::{CrawlError, Result};

use super::{CrawlerConfig, SourceConfig, schema};

pub(super) fn validate(config: &CrawlerConfig) -> Result<()> {
    schema::validate(&serde_json::to_value(config)?)?;

    let queue_names = [
        config.queue.queues.discovery.as_str(),
        config.queue.queues.articles.as_str(),
        config.queue.queues.delivery.as_str(),
    ];
    if queue_names[0] == queue_names[1]
        || queue_names[0] == queue_names[2]
        || queue_names[1] == queue_names[2]
    {
        return Err(CrawlError::Configuration(
            "discovery, article, and delivery queue names must be distinct".into(),
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
        if let SourceConfig::Html(source) = source {
            let uses_category = source.pagination_template.contains("{category}");
            if uses_category && source.indexed_categories.is_empty() {
                return Err(CrawlError::Configuration(format!(
                    "HTML source '{}' uses {{category}} but has no indexed_categories",
                    source.common.id
                )));
            }
            if !uses_category && !source.indexed_categories.is_empty() {
                return Err(CrawlError::Configuration(format!(
                    "HTML source '{}' declares indexed_categories but its pagination_template has no {{category}}",
                    source.common.id
                )));
            }
            let mut categories = HashSet::new();
            for category in &source.indexed_categories {
                let normalized = category.to_lowercase();
                if category.trim() != category {
                    return Err(CrawlError::Configuration(format!(
                        "indexed category '{category}' for source '{}' has surrounding whitespace",
                        source.common.id
                    )));
                }
                if !categories.insert(normalized) {
                    return Err(CrawlError::Configuration(format!(
                        "duplicate indexed category '{category}' for source '{}'",
                        source.common.id
                    )));
                }
            }
        }
    }
    Ok(())
}
