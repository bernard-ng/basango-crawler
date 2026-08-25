use super::*;

#[test]
fn appends_endpoint_to_an_existing_base_path() {
    let base = Url::parse("https://api.example.com/crawler").unwrap();
    assert_eq!(
        endpoint_url(&base, "ingest/articles").unwrap().as_str(),
        "https://api.example.com/crawler/ingest/articles"
    );
}

#[test]
fn retryable_statuses_match_transport_semantics() {
    assert!(is_retryable_status(503));
    assert!(!is_retryable_status(422));
}
