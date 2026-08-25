use super::*;

#[test]
fn retry_after_accepts_seconds() {
    assert_eq!(parse_retry_after("12"), Some(Duration::from_secs(12)));
}

#[test]
fn transient_statuses_are_explicit() {
    assert!(is_transient(StatusCode::TOO_MANY_REQUESTS));
    assert!(!is_transient(StatusCode::NOT_FOUND));
}
