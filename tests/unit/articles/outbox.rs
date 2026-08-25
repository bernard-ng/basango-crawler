use chrono::Utc;
use tempfile::tempdir;
use url::Url;

use super::*;

fn article() -> Article {
    Article {
        hash: "hash-1".into(),
        title: "Title".into(),
        body: "Body".into(),
        link: Url::parse("https://example.com/one").unwrap(),
        source_id: crate::domain::SourceId::new("example").unwrap(),
        categories: vec!["news".into()],
        metadata: None,
        published_at: Utc::now(),
    }
}

#[test]
fn forwarded_rows_stay_forwarded_when_saved_again() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("outbox.db");
    let outbox = Outbox::open(&path, true).unwrap();
    let article = article();

    assert_eq!(outbox.save(&article).unwrap(), DeliveryStatus::Pending);
    outbox.mark_forwarded(&article.hash).unwrap();
    assert_eq!(outbox.save(&article).unwrap(), DeliveryStatus::Forwarded);
}

#[test]
fn claim_reserves_pending_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("outbox.db");
    let outbox = Outbox::open(&path, true).unwrap();
    outbox.save(&article()).unwrap();

    let claimed = outbox
        .claim("worker-1", None, 10, false, Duration::minutes(15))
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].claimed_by.as_deref(), Some("worker-1"));

    let second = outbox
        .claim("worker-2", None, 10, false, Duration::minutes(15))
        .unwrap();
    assert!(second.is_empty());
}

#[test]
fn retry_all_claims_non_retryable_failures_after_a_client_fix() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("outbox.db");
    let outbox = Outbox::open(&path, true).unwrap();
    let article = article();
    outbox.save(&article).unwrap();
    outbox
        .mark_failed(&article.hash, "HTTP 400 from an old payload", false)
        .unwrap();

    assert!(
        outbox
            .claim("normal", None, 10, false, Duration::minutes(15))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        outbox
            .claim("manual", None, 10, true, Duration::minutes(15))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn releasing_a_claim_makes_an_interrupted_batch_available_again() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("outbox.db");
    let outbox = Outbox::open(&path, true).unwrap();
    outbox.save(&article()).unwrap();
    assert_eq!(
        outbox
            .claim("interrupted", None, 10, false, Duration::minutes(15))
            .unwrap()
            .len(),
        1
    );

    assert_eq!(outbox.release_claim("interrupted").unwrap(), 1);
    assert_eq!(
        outbox
            .claim("replacement", None, 10, false, Duration::minutes(15))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn clear_empties_the_outbox_without_removing_its_schema() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("outbox.db");
    let outbox = Outbox::open(&path, true).unwrap();
    outbox.save(&article()).unwrap();

    assert_eq!(outbox.clear().unwrap(), 1);
    assert!(
        outbox
            .claim("worker", None, 10, true, Duration::minutes(15))
            .unwrap()
            .is_empty()
    );
    assert_eq!(outbox.clear().unwrap(), 0);
}

#[test]
fn stats_summarize_delivery_and_claim_state() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("outbox.db");
    let outbox = Outbox::open(&path, true).unwrap();
    let first = article();
    outbox.save(&first).unwrap();
    outbox
        .claim("worker", None, 1, false, Duration::minutes(15))
        .unwrap();

    let mut second = article();
    second.hash = "hash-2".into();
    second.link = Url::parse("https://example.com/two").unwrap();
    outbox.save(&second).unwrap();
    outbox.mark_failed(&second.hash, "temporary", true).unwrap();

    let mut third = article();
    third.hash = "hash-3".into();
    third.link = Url::parse("https://example.com/three").unwrap();
    outbox.save(&third).unwrap();
    outbox.mark_forwarded(&third.hash).unwrap();

    assert_eq!(
        outbox.stats().unwrap(),
        OutboxStats {
            total: 3,
            pending: 1,
            forwarded: 1,
            failed: 1,
            retryable_failed: 1,
            claimed: 1,
        }
    );
}
