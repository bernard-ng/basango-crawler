use std::{env, str::FromStr};

use url::Url;

use crate::{
    domain::UpdateDirection,
    error::{CrawlError, Result},
};

use super::CrawlerConfig;

pub(super) fn apply(config: &mut CrawlerConfig) -> Result<()> {
    if let Some(raw) = value("BASANGO_API_CRAWLER_ENDPOINT") {
        config.ingestion.endpoint = Some(Url::parse(&raw).map_err(|error| {
            CrawlError::Configuration(format!("invalid ingestion API endpoint: {error}"))
        })?);
    }
    set_string("BASANGO_API_CRAWLER_TOKEN", &mut config.ingestion.token);
    set_string("BASANGO_CRAWLER_REDIS_URL", &mut config.queue.redis_url);
    set_string(
        "BASANGO_CRAWLER_QUEUE_DISCOVERY",
        &mut config.queue.queues.discovery,
    );
    set_string(
        "BASANGO_CRAWLER_QUEUE_ARTICLES",
        &mut config.queue.queues.articles,
    );
    set_parsed(
        "BASANGO_CRAWLER_RETAIN_COMPLETED",
        &mut config.queue.retention.completed,
    )?;
    set_parsed(
        "BASANGO_CRAWLER_RETAIN_FAILED",
        &mut config.queue.retention.failed,
    )?;
    set_string(
        "BASANGO_CRAWLER_FETCH_USER_AGENT",
        &mut config.http.user_agent,
    );
    set_parsed(
        "BASANGO_CRAWLER_FETCH_MAX_RETRIES",
        &mut config.http.max_retries,
    )?;
    if let Some(raw) = value("BASANGO_CRAWLER_FETCH_RESPECT_RETRY_AFTER") {
        config.http.respect_retry_after =
            parse_bool("BASANGO_CRAWLER_FETCH_RESPECT_RETRY_AFTER", &raw)?;
    }
    if let Some(raw) = value("BASANGO_CRAWLER_UPDATE_DIRECTION") {
        config.runtime.direction = match raw.as_str() {
            "forward" => UpdateDirection::Forward,
            "backward" => UpdateDirection::Backward,
            _ => {
                return Err(CrawlError::Configuration(format!(
                    "BASANGO_CRAWLER_UPDATE_DIRECTION must be 'forward' or 'backward', got '{raw}'"
                )));
            }
        };
    }
    if let Some(raw) = value("BASANGO_CRAWLER_DATA_PATH") {
        config.paths.data = raw.into();
    }
    if let Some(raw) = value("BASANGO_CRAWLER_ROOT_PATH") {
        config.paths.root = raw.into();
    }
    if let Some(raw) = value("BASANGO_CRAWLER_SQLITE_PATH") {
        config.paths.sqlite = Some(raw.into());
    }
    Ok(())
}

pub(super) fn value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn set_string(name: &str, target: &mut String) {
    if let Some(raw) = value(name) {
        *target = raw;
    }
}

fn set_parsed<T>(name: &str, target: &mut T) -> Result<()>
where
    T: FromStr,
{
    if let Some(raw) = value(name) {
        *target = raw.parse().map_err(|_| {
            CrawlError::Configuration(format!(
                "environment variable {name} has invalid value '{raw}'"
            ))
        })?;
    }
    Ok(())
}

fn parse_bool(name: &str, raw: &str) -> Result<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(CrawlError::Configuration(format!(
            "environment variable {name} must be a boolean, got '{raw}'"
        ))),
    }
}
