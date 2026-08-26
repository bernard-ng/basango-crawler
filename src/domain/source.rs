use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::{CrawlError, Result};

/// A validated source identifier.
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

/// A normalized category path used by category-indexed sources.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct CategorySlug(String);

impl CategorySlug {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let normalized = value.trim().to_lowercase();
        if normalized.is_empty() {
            return Err(CrawlError::Configuration(
                "category slug cannot be empty".into(),
            ));
        }
        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CategorySlug {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CategorySlug {
    type Err = CrawlError;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for CategorySlug {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}
