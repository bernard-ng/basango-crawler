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
