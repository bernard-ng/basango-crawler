//! Typed crawler configuration.
//!
//! Loading, environment overrides, structural schemas, semantic validation,
//! and each configuration area live in separate modules. Callers consume the
//! typed facade exported here.

mod environment;
mod http;
mod ingestion;
mod loader;
mod paths;
mod queue;
mod runtime;
mod schema;
mod source;
mod validation;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    domain::SourceId,
    error::{CrawlError, Result},
};

pub use http::{BackoffConfig, HttpClientConfig};
pub use ingestion::IngestionApiConfig;
pub use paths::PathsConfig;
pub use queue::{JobRetention, QueueConfig, QueueNames};
pub use runtime::CrawlerRuntimeConfig;
pub use source::{
    CommonSourceConfig, HtmlSelectors, HtmlSourceConfig, MetadataStrategy, SourceConfig,
    WordPressSourceConfig,
};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CrawlerConfig {
    pub ingestion: IngestionApiConfig,
    pub http: HttpClientConfig,
    pub paths: PathsConfig,
    pub queue: QueueConfig,
    pub runtime: CrawlerRuntimeConfig,
    pub sources: Vec<SourceConfig>,
}

impl CrawlerConfig {
    /// Load JSON, apply environment overrides, and validate the final value.
    pub fn load(path: Option<PathBuf>) -> Result<Self> {
        loader::load(path)
    }

    /// Validate configurations constructed by embedding applications.
    pub fn validate(&self) -> Result<()> {
        validation::validate(self)
    }

    pub fn source(&self, source_id: &SourceId) -> Result<SourceConfig> {
        self.sources
            .iter()
            .find(|source| source.id() == source_id)
            .cloned()
            .ok_or_else(|| CrawlError::SourceNotFound(source_id.to_string()))
    }

    pub fn data_path(&self) -> PathBuf {
        self.paths.data_path()
    }

    pub fn sqlite_path(&self) -> PathBuf {
        self.paths.sqlite_path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_configuration_matches_the_zod_and_rust_schemas() {
        let config = loader::parse(loader::BUNDLED_CONFIG).unwrap();
        assert_eq!(config.queue.queues.discovery, "discovery");
        assert_eq!(config.queue.queues.articles, "articles");
        assert!(matches!(config.sources[0], SourceConfig::Html(_)));
    }

    #[test]
    fn zod_schema_reports_nested_configuration_paths() {
        let error = loader::parse(
            r#"{
                "http": { "timeout": 0 },
                "sources": [{ "kind": "wordpress", "id": "example", "url": "not-a-url" }]
            }"#,
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("http.timeout"), "{message}");
        assert!(message.contains("sources.0.url"), "{message}");
    }

    #[test]
    fn duplicate_source_ids_are_rejected_semantically() {
        let error = loader::parse(
            r#"{
                "sources": [
                    { "kind": "wordpress", "id": "duplicate", "url": "https://one.example" },
                    { "kind": "wordpress", "id": "duplicate", "url": "https://two.example" }
                ]
            }"#,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate source id 'duplicate'")
        );
    }

    #[test]
    fn nested_configuration_wrapper_is_rejected() {
        let result = loader::parse(
            r#"{
                "crawler": {
                    "sources": [{ "kind": "wordpress", "id": "example", "url": "https://example.com" }]
                }
            }"#,
        );
        assert!(result.is_err());
    }
}
