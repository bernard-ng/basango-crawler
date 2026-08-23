use serde::{Deserialize, Serialize};
use url::Url;

use crate::domain::SourceId;

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
    Html(HtmlSourceConfig),
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
}

fn default_pagination_selector() -> String {
    "ul.pagination > li a".into()
}

fn default_date_format() -> String {
    "yyyy-LL-dd HH:mm".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_config_deserializes_directly_into_its_variant() {
        let json = r#"{
            "kind": "html",
            "id": "example",
            "url": "https://example.com",
            "pagination_template": "news",
            "selectors": {
                "body": ".body",
                "date": "time",
                "link": "a",
                "list": ".article",
                "title": "h1"
            }
        }"#;
        let source: SourceConfig = serde_json::from_str(json).unwrap();
        let SourceConfig::Html(source) = source else {
            panic!("expected HTML source");
        };
        assert_eq!(source.selectors.list, ".article");
        assert_eq!(source.common.id.as_str(), "example");
    }
}
