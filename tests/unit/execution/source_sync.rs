use crate::config::{CommonSourceConfig, MetadataStrategy, SourceConfig, WordPressSourceConfig};

use super::*;

#[test]
fn registration_uses_the_crawler_source_identity() {
    let source = SourceConfig::WordPress(WordPressSourceConfig {
        common: CommonSourceConfig {
            id: crate::domain::SourceId::new("example.com").unwrap(),
            url: url::Url::parse("https://example.com").unwrap(),
            ..CommonSourceConfig::default()
        },
        metadata_strategy: MetadataStrategy::default(),
    });

    let registration = SourceSyncItem::from(&source);

    assert_eq!(registration.name, "example.com");
    assert_eq!(registration.kind, "wordpress");
    assert_eq!(registration.url, "https://example.com/");
    assert_eq!(registration.estimated_articles, None);
}

#[test]
fn synchronization_payload_omits_an_unavailable_estimate() {
    let items = vec![SourceSyncItem {
        estimated_articles: None,
        kind: "html".into(),
        name: "example.com".into(),
        url: "https://example.com".into(),
    }];

    let value = serde_json::to_value(SourceSyncPayload { sources: &items }).unwrap();

    assert_eq!(value["sources"][0]["name"], "example.com");
    assert!(value["sources"][0].get("estimatedArticles").is_none());
}
