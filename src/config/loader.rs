use std::{borrow::Cow, fs, path::Path, path::PathBuf};

use serde_json::Value;

use crate::error::{CrawlError, Result};

use super::{CrawlerConfig, environment, schema, validation};

pub(super) const BUNDLED_CONFIG: &str = include_str!("../../config/crawler.json");

pub(super) fn load(path: Option<PathBuf>) -> Result<CrawlerConfig> {
    let _ = dotenvy::dotenv();
    let path =
        path.or_else(|| environment::value("BASANGO_CRAWLER_CONFIG_PATH").map(PathBuf::from));
    let raw = read(path.as_deref())?;
    let mut config = decode(&raw)?;
    environment::apply(&mut config)?;
    validation::validate(&config)?;
    Ok(config)
}

#[cfg(test)]
pub(super) fn parse(raw: &str) -> Result<CrawlerConfig> {
    let config = decode(raw)?;
    validation::validate(&config)?;
    Ok(config)
}

fn decode(raw: &str) -> Result<CrawlerConfig> {
    let value: Value = serde_json::from_str(raw)?;
    schema::validate(&value)?;
    serde_json::from_value(value).map_err(Into::into)
}

fn read(path: Option<&Path>) -> Result<Cow<'static, str>> {
    match path {
        Some(path) => fs::read_to_string(path).map(Cow::Owned).map_err(|error| {
            CrawlError::Configuration(format!("cannot read {}: {error}", path.display()))
        }),
        None => Ok(Cow::Borrowed(BUNDLED_CONFIG)),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/config/loader.rs"]
mod tests;
