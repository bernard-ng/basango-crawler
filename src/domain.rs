//! Domain types: the vocabulary of the crawler.
//!
//! Domain values describe *what* Basango works with. They deliberately do not
//! know how HTTP, Redis, SQLite, or the CLI work. This dependency direction is
//! what lets the same types move through synchronous and queued execution.

use std::{fmt, str::FromStr};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{CrawlError, Result};

// --- Source identity -------------------------------------------------------

/// A validated source identifier.
///
/// Using a newtype prevents an arbitrary or empty `String` from being passed
/// wherever the crawler expects the identity of a configured source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SourceId(String);

impl SourceId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let normalized = value.trim();
        if normalized.is_empty() {
            return Err(CrawlError::Configuration(
                "source id cannot be empty".into(),
            ));
        }
        Ok(Self(normalized.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SourceId {
    type Err = CrawlError;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl AsRef<str> for SourceId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'de> Deserialize<'de> for SourceId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

// --- Articles -------------------------------------------------------------

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
    /// Empty metadata is represented as `None` instead of an object full of
    /// `null` values. That makes absence explicit to downstream code.
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

/// The validated representation persisted in the outbox and sent to the API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Article {
    pub hash: String,
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

// --- Ranges and crawl options --------------------------------------------

/// Inclusive page boundaries. HTML sources may start at page zero, whereas
/// WordPress starts at page one, so zero is a valid value here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRange {
    pub start: u32,
    pub end: u32,
}

impl PageRange {
    pub fn new(start: u32, end: u32) -> Result<Self> {
        if end < start {
            return Err(CrawlError::InvalidRange(format!(
                "end page {end} is before start page {start}"
            )));
        }
        Ok(Self { start, end })
    }

    /// Parse the CLI representation `start:end`.
    pub fn parse(spec: &str) -> Result<Self> {
        let (start, end) = split_range(spec, "page")?;
        Self::new(
            start
                .parse()
                .map_err(|_| CrawlError::InvalidRange(format!("invalid start page '{start}'")))?,
            end.parse()
                .map_err(|_| CrawlError::InvalidRange(format!("invalid end page '{end}'")))?,
        )
    }
}

impl fmt::Display for PageRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.start, self.end)
    }
}

/// Inclusive UTC time boundaries used to filter articles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl DateRange {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Self> {
        if end < start {
            return Err(CrawlError::InvalidRange(
                "end date must be on or after start date".into(),
            ));
        }
        Ok(Self { start, end })
    }

    /// Parse `YYYY-MM-DD:YYYY-MM-DD`. The end date is expanded to the last
    /// nanosecond of that day so the range behaves as users expect.
    pub fn parse(spec: &str) -> Result<Self> {
        let (start, end) = split_range(spec, "date")?;
        let start = parse_date(start)?
            .and_hms_opt(0, 0, 0)
            .expect("midnight is valid");
        let end = parse_date(end)?
            .and_hms_nano_opt(23, 59, 59, 999_999_999)
            .expect("end of day is valid");
        Self::new(Utc.from_utc_datetime(&start), Utc.from_utc_datetime(&end))
    }

    pub fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        self.start <= timestamp && timestamp <= self.end
    }

    /// Crawlers receive newest-first listings. Once an article is older than
    /// `start`, all following items are normally older too and crawling can stop.
    pub fn is_older_than_range(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp < self.start
    }
}

impl fmt::Display for DateRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}",
            self.start.format("%Y-%m-%d"),
            self.end.format("%Y-%m-%d")
        )
    }
}

/// One request to crawl a configured source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrawlRequest {
    pub source_id: SourceId,
    pub page_range: Option<PageRange>,
    pub date_range: Option<DateRange>,
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<UpdateDirection>,
}

/// Crawling direction used when the backend supplies an update boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UpdateDirection {
    Backward,
    #[default]
    Forward,
}

impl FromStr for UpdateDirection {
    type Err = CrawlError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "backward" => Ok(Self::Backward),
            "forward" => Ok(Self::Forward),
            _ => Err(CrawlError::InvalidRange(format!(
                "invalid update direction '{value}'; expected forward or backward"
            ))),
        }
    }
}

// --- Helpers --------------------------------------------------------------

fn split_range<'a>(spec: &'a str, kind: &str) -> Result<(&'a str, &'a str)> {
    spec.split_once(':').ok_or_else(|| {
        CrawlError::InvalidRange(format!("invalid {kind} range '{spec}'; expected start:end"))
    })
}

fn parse_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| CrawlError::InvalidRange(format!("invalid date '{value}'; use YYYY-MM-DD")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_range_rejects_reversed_bounds() {
        assert!(PageRange::parse("5:2").is_err());
    }

    #[test]
    fn timestamp_range_includes_the_whole_end_day() {
        let range = DateRange::parse("2025-01-01:2025-01-02").unwrap();
        let end_of_day = DateTime::parse_from_rfc3339("2025-01-02T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(range.contains(end_of_day));
    }

    #[test]
    fn source_id_is_trimmed_and_cannot_be_empty() {
        assert_eq!(SourceId::new(" example ").unwrap().as_str(), "example");
        assert!(SourceId::new("   ").is_err());
        assert!(serde_json::from_str::<SourceId>(r#""""#).is_err());
    }

    #[test]
    fn absent_metadata_fields_are_omitted_from_api_payloads() {
        let metadata = ArticleMetadata {
            title: Some("Article title".into()),
            ..ArticleMetadata::default()
        };

        assert_eq!(
            serde_json::to_value(metadata).unwrap(),
            serde_json::json!({ "title": "Article title" })
        );
    }
}
