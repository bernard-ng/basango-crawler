use std::{fmt, str::FromStr};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::SourceId;
use crate::error::{CrawlError, Result};

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

fn split_range<'a>(spec: &'a str, kind: &str) -> Result<(&'a str, &'a str)> {
    spec.split_once(':').ok_or_else(|| {
        CrawlError::InvalidRange(format!("invalid {kind} range '{spec}'; expected start:end"))
    })
}

fn parse_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| CrawlError::InvalidRange(format!("invalid date '{value}'; use YYYY-MM-DD")))
}
