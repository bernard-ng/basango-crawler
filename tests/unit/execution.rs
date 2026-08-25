use chrono::TimeZone;

use super::*;

#[test]
fn empty_publication_bounds_do_not_filter_a_first_crawl() {
    let bounds = SourcePublicationBounds {
        earliest: None,
        latest: None,
    };
    let now = Utc.with_ymd_and_hms(2026, 8, 24, 9, 0, 0).unwrap();

    assert_eq!(
        automatic_date_range(UpdateDirection::Forward, &bounds, now).unwrap(),
        None
    );
}

#[test]
fn forward_publication_bounds_start_at_the_latest_article() {
    let earliest = Utc.with_ymd_and_hms(2026, 8, 1, 10, 0, 0).unwrap();
    let latest = Utc.with_ymd_and_hms(2026, 8, 22, 20, 36, 57).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 24, 9, 0, 0).unwrap();
    let bounds = SourcePublicationBounds {
        earliest: Some(earliest),
        latest: Some(latest),
    };

    let range = automatic_date_range(UpdateDirection::Forward, &bounds, now)
        .unwrap()
        .unwrap();
    assert_eq!(range.start, latest);
    assert_eq!(range.end, now);
}

#[test]
fn backward_publication_bounds_end_at_the_earliest_article() {
    let earliest = Utc.with_ymd_and_hms(2026, 8, 1, 10, 0, 0).unwrap();
    let latest = Utc.with_ymd_and_hms(2026, 8, 22, 20, 36, 57).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 24, 9, 0, 0).unwrap();
    let bounds = SourcePublicationBounds {
        earliest: Some(earliest),
        latest: Some(latest),
    };

    let range = automatic_date_range(UpdateDirection::Backward, &bounds, now)
        .unwrap()
        .unwrap();
    assert_eq!(range.start, DateTime::<Utc>::UNIX_EPOCH);
    assert_eq!(range.end, earliest);
}
