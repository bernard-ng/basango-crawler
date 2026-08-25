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
    domain::{CrawlRequest, SourceId},
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

    pub fn prepare_request(&self, request: &mut CrawlRequest) -> Result<()> {
        let source = self.source(&request.source_id)?;
        request.category = source.canonical_category(request.category.as_deref())?;
        Ok(())
    }

    pub fn data_path(&self) -> PathBuf {
        self.paths.data_path()
    }

    pub fn sqlite_path(&self) -> PathBuf {
        self.paths.sqlite_path()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/config/mod.rs"]
mod tests;
