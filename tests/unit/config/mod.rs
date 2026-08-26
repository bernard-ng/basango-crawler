use super::*;

#[test]
fn bundled_configuration_matches_the_zod_and_rust_schemas() {
    let config = loader::parse(loader::BUNDLED_CONFIG).unwrap();
    assert_eq!(config.queue.queues.discovery, "discovery");
    assert_eq!(config.queue.queues.articles, "articles");
    assert_eq!(config.queue.queues.delivery, "delivery");
    assert!(matches!(config.sources[0], SourceConfig::Html(_)));
}

#[test]
fn zod_schema_reports_nested_configuration_paths() {
    let error = loader::parse(
        r#"{
            "http": { "timeout": 0 },
            "sources": [{ "kind": "wordpress", "id": "example", "url": "not-a-url" }]
        }"#,
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("http.timeout"), "{message}");
    assert!(message.contains("sources.0.url"), "{message}");
}

#[test]
fn duplicate_source_ids_are_rejected_semantically() {
    let error = loader::parse(
        r#"{
            "sources": [
                { "kind": "wordpress", "id": "duplicate", "url": "https://one.example" },
                { "kind": "wordpress", "id": "duplicate", "url": "https://two.example" }
            ]
        }"#,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate source id 'duplicate'")
    );
}

#[test]
fn category_templates_require_an_index() {
    let error = loader::parse(
        r#"{
            "sources": [{
                "kind": "html",
                "id": "example",
                "url": "https://example.com",
                "pagination_template": "category/{category}",
                "selectors": {
                    "body": ".body",
                    "date": "time",
                    "link": "a",
                    "list": ".article",
                    "title": "h1"
                }
            }]
        }"#,
    )
    .unwrap_err();
    assert!(error.to_string().contains("has no indexed_categories"));
}

#[test]
fn nested_configuration_wrapper_is_rejected() {
    let result = loader::parse(
        r#"{
            "crawler": {
                "sources": [{ "kind": "wordpress", "id": "example", "url": "https://example.com" }]
            }
        }"#,
    );
    assert!(result.is_err());
}
