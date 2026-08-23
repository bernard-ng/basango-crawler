//! Integration test for the durable article pipeline.
//!
//! Source-specific HTML/WordPress parsing has focused unit tests. This test
//! crosses public module boundaries: draft → normalization → outbox persistence.

use basango::{ArticleDraft, DeliveryStatus, Outbox, SourceId, normalize};
use chrono::Utc;
use tempfile::tempdir;
use url::Url;

#[test]
fn draft_is_normalized_and_persisted_for_later_delivery() {
    let directory = tempdir().unwrap();
    let sqlite_path = directory.path().join("crawler.db");
    let draft = ArticleDraft {
        title: "  Fixture\u{00a0}story  ".into(),
        body: "Hello from Rust.\n\n\nSecond paragraph.".into(),
        link: Url::parse("https://example.com/story").unwrap(),
        source_id: SourceId::new("fixture").unwrap(),
        categories: vec!["Learning".into(), "learning".into()],
        metadata: None,
        published_at: Utc::now(),
    };

    // `None` means no ingestion API client is configured. The outbox therefore
    // becomes the durable hand-off point for a later `push` command.
    let article = normalize(draft).unwrap();
    let outbox = Outbox::open(&sqlite_path, true).unwrap();
    let status = outbox.save(&article).unwrap();

    assert_eq!(status, DeliveryStatus::Pending);
    assert_eq!(article.title, "Fixture story");
    assert_eq!(article.categories, vec!["Learning"]);
    let pending = outbox.list_pending(Some("fixture"), 10).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].article, article);
}
