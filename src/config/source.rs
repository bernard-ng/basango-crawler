use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    domain::SourceId,
    error::{CrawlError, Result},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommonSourceConfig {
    #[serde(default = "default_date_format")]
    pub date_format: String,
    pub id: SourceId,
    #[serde(default)]
    pub rate_limit: bool,
    pub url: Url,
}

impl Default for CommonSourceConfig {
    fn default() -> Self {
        Self {
            date_format: default_date_format(),
            id: SourceId::new("unnamed").expect("static source id is valid"),
            rate_limit: false,
            url: Url::parse("http://localhost").expect("static URL is valid"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HtmlSourceConfig {
    #[serde(flatten)]
    pub common: CommonSourceConfig,
    #[serde(default)]
    pub fetch_details: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexed_categories: Vec<String>,
    pub pagination_template: String,
    pub selectors: HtmlSelectors,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HtmlSelectors {
    pub body: String,
    #[serde(default)]
    pub categories: Option<String>,
    pub date: String,
    pub link: String,
    pub list: String,
    pub title: String,
    #[serde(default = "default_pagination_selector")]
    pub pagination: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WordPressSourceConfig {
    #[serde(flatten)]
    pub common: CommonSourceConfig,
    #[serde(default)]
    pub metadata_strategy: MetadataStrategy,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MetadataStrategy {
    #[default]
    Auto,
    Yoast,
    Rest,
    Fetch,
    None,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceConfig {
    Html(Box<HtmlSourceConfig>),
    #[serde(rename = "wordpress")]
    WordPress(WordPressSourceConfig),
}

impl SourceConfig {
    pub fn id(&self) -> &SourceId {
        &self.common().id
    }

    pub fn url(&self) -> &Url {
        &self.common().url
    }

    pub fn common(&self) -> &CommonSourceConfig {
        match self {
            Self::Html(source) => &source.common,
            Self::WordPress(source) => &source.common,
        }
    }

    pub fn canonical_category(&self, requested: Option<&str>) -> Result<Option<String>> {
        let Self::Html(source) = self else {
            return Ok(requested
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned));
        };
        if source.indexed_categories.is_empty() {
            return Ok(requested
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned));
        }

        let valid = source.indexed_categories.join(", ");
        let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
            return Err(CrawlError::Configuration(format!(
                "source '{}' requires --category; valid categories: {valid}",
                source.common.id
            )));
        };
        if let Some(category) = source
            .indexed_categories
            .iter()
            .find(|category| category.to_lowercase() == requested.to_lowercase())
        {
            return Ok(Some(category.clone()));
        }

        let suggestion = closest_category(requested, &source.indexed_categories)
            .map(|category| format!(" Did you mean '{category}'?"))
            .unwrap_or_default();
        Err(CrawlError::Configuration(format!(
            "category '{requested}' is not indexed for source '{}'.{suggestion} Valid categories: {valid}",
            source.common.id
        )))
    }
}

fn closest_category<'a>(requested: &str, categories: &'a [String]) -> Option<&'a str> {
    let requested = requested.to_lowercase();
    categories
        .iter()
        .map(|category| {
            let category_lower = category.to_lowercase();
            let leaf = category_lower.rsplit('/').next().unwrap_or(&category_lower);
            let distance =
                edit_distance(&requested, &category_lower).min(edit_distance(&requested, leaf));
            (category.as_str(), distance, leaf.chars().count())
        })
        .min_by_key(|(_, distance, _)| *distance)
        .filter(|(_, distance, length)| *distance <= 2.max(*length / 3))
        .map(|(category, _, _)| category)
}

fn edit_distance(left: &str, right: &str) -> usize {
    let mut previous: Vec<usize> = (0..=right.chars().count()).collect();
    let mut current = vec![0; previous.len()];
    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right.chars().enumerate() {
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + usize::from(left_char != right_char));
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.chars().count()]
}

fn default_pagination_selector() -> String {
    "ul.pagination > li a".into()
}

fn default_date_format() -> String {
    "yyyy-LL-dd HH:mm".into()
}

#[cfg(test)]
#[path = "../../tests/unit/config/source.rs"]
mod tests;
