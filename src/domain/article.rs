use std::{fmt, ops::Deref};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use super::SourceId;
use crate::error::{CrawlError, Result};

/// The stable identity of an article URL in the local outbox and ingestion API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ArticleHash(String);

impl ArticleHash {
    pub fn from_url(url: &Url) -> Self {
        Self(format!("{:x}", md5::compute(url.as_str())))
    }

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CrawlError::InvalidArticle(format!(
                "invalid article hash '{value}'"
            )));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArticleHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for ArticleHash {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for ArticleHash {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<'de> Deserialize<'de> for ArticleHash {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Optional metadata discovered from Open Graph or WordPress fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArticleMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl ArticleMetadata {
    pub fn is_empty(&self) -> bool {
        self.url.is_none()
            && self.title.is_none()
            && self.author.is_none()
            && self.description.is_none()
            && self.image.is_none()
            && self.published_at.is_none()
            && self.updated_at.is_none()
    }
}

/// A source crawler's output before normalization and hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArticleDraft {
    pub title: String,
    pub body: String,
    pub link: Url,
    pub source_id: SourceId,
    pub categories: Vec<String>,
    pub metadata: Option<ArticleMetadata>,
    pub published_at: DateTime<Utc>,
}

/// The validated representation persisted locally and sent to the API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Article {
    pub hash: ArticleHash,
    pub title: String,
    pub body: String,
    pub link: Url,
    pub source_id: SourceId,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ArticleMetadata>,
    pub published_at: DateTime<Utc>,
}
