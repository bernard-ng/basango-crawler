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
    assert!(source.indexed_categories.is_empty());
}

#[test]
fn indexed_categories_are_required_and_offer_typo_suggestions() {
    let source = match serde_json::from_str::<SourceConfig>(
        r#"{
            "kind": "html",
            "id": "example",
            "url": "https://example.com",
            "indexed_categories": ["actualite/politique", "sport"],
            "pagination_template": "category/{category}",
            "selectors": {
                "body": ".body",
                "date": "time",
                "link": "a",
                "list": ".article",
                "title": "h1"
            }
        }"#,
    )
    .unwrap()
    {
        SourceConfig::Html(source) => SourceConfig::Html(source),
        SourceConfig::WordPress(_) => panic!("expected HTML source"),
    };

    assert_eq!(
        source.canonical_category(Some("SPORT")).unwrap(),
        Some("sport".into())
    );
    let missing = source.canonical_category(None).unwrap_err().to_string();
    assert!(missing.contains("requires --category"), "{missing}");
    let typo = source
        .canonical_category(Some("politique"))
        .unwrap_err()
        .to_string();
    assert!(
        typo.contains("Did you mean 'actualite/politique'?"),
        "{typo}"
    );
}
