use super::*;

#[test]
fn parses_offset_without_colon_using_configured_format() {
    let published_at =
        parse_published_at("2026-08-03T21:00:54+0100", "yyyy-LL-dd'T'HH:mm:ssxx").unwrap();

    assert_eq!(published_at.to_rfc3339(), "2026-08-03T20:00:54+00:00");
}
