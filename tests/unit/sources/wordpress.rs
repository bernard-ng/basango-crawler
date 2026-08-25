use super::*;

#[test]
fn extracts_yoast_metadata() {
    let link = Url::parse("https://example.com/story").unwrap();
    let post = WordPressPost {
        link: Some(link.clone()),
        yoast_head_json: Some(YoastMetadata {
            og_title: Some("Yoast title".into()),
            og_image: vec![YoastImage {
                url: Some("/cover.jpg".into()),
            }],
            ..YoastMetadata::default()
        }),
        ..WordPressPost::default()
    };
    let metadata = yoast_metadata(&post, &link).unwrap();
    assert_eq!(metadata.title.as_deref(), Some("Yoast title"));
    assert_eq!(
        metadata.image.unwrap().as_str(),
        "https://example.com/cover.jpg"
    );
}

#[test]
fn parses_wordpress_naive_datetime_as_utc() {
    let date: chrono::DateTime<chrono::Utc> =
        common::parse_published_at("2025-01-02T03:04:05", "yyyy-LL-dd'T'HH:mm:ss").unwrap();
    assert_eq!(date.to_rfc3339(), "2025-01-02T03:04:05+00:00");
}
