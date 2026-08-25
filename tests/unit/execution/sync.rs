use super::*;

#[test]
fn summary_default_starts_at_zero() {
    assert_eq!(CrawlReport::default().collected, 0);
}
