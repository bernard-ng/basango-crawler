use chrono::Utc;
use url::Url;

use super::*;

#[test]
fn normalizes_text_and_deduplicates_categories() {
    let article = normalize(ArticleDraft {
        title: "  A\u{00a0}title  ".into(),
        body: "body\n\n\nsecond".into(),
        link: Url::parse("https://example.com/article").unwrap(),
        source_id: crate::domain::SourceId::new("example").unwrap(),
        categories: vec!["News".into(), "news".into()],
        metadata: None,
        published_at: Utc::now(),
    })
    .unwrap();

    assert_eq!(article.title, "A title");
    assert_eq!(article.body, "body\nsecond");
    assert_eq!(article.categories, vec!["News"]);
    assert_eq!(article.hash.len(), 32);
}
