//! Parsing helpers shared by source implementations.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use scraper::Html;
use url::Url;

/// Resolve a link against its source while preserving already-absolute URLs.
pub(crate) fn absolute_url(base: &Url, value: &str) -> Option<Url> {
    Url::parse(value).or_else(|_| base.join(value)).ok()
}

/// Convert a JavaScript/date-fns-oriented source format into common Chrono
/// formats. Unknown formats still fall back to RFC 3339 and a few safe defaults.
pub(crate) fn parse_published_at(raw: &str, configured_format: &str) -> Option<DateTime<Utc>> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }

    if let Ok(date) = DateTime::parse_from_rfc3339(value) {
        return Some(date.with_timezone(&Utc));
    }
    if let Ok(date) = DateTime::parse_from_rfc2822(value) {
        return Some(date.with_timezone(&Utc));
    }

    let chrono_format = match configured_format {
        "dd.MM.yyyy" => "%d.%m.%Y",
        "yyyy-LL-dd" => "%Y-%m-%d",
        "yyyy-LL-dd HH:mm" => "%Y-%m-%d %H:%M",
        "yyyy-LL-dd'T'HH:mm:ss" => "%Y-%m-%dT%H:%M:%S",
        other => other,
    };

    if let Ok(value) = NaiveDateTime::parse_from_str(value, chrono_format) {
        // A source-local timezone is not always provided. Treating naive values
        // as UTC is deterministic; offset-bearing values above retain offsets.
        return Some(Utc.from_utc_datetime(&value));
    }
    if let Ok(value) = NaiveDate::parse_from_str(value, chrono_format) {
        return value
            .and_hms_opt(0, 0, 0)
            .map(|value| Utc.from_utc_datetime(&value));
    }

    // WordPress commonly omits its timezone suffix.
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
        .ok()
        .map(|value| Utc.from_utc_datetime(&value))
}

pub(crate) fn text_from_html(html: &str) -> Option<String> {
    let fragment = Html::parse_fragment(html);
    let text = fragment.root_element().text().collect::<Vec<_>>().join(" ");
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!text.is_empty()).then_some(text)
}
