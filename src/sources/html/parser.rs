use scraper::{ElementRef, Html, Selector};

use crate::error::{CrawlError, Result};

pub(super) struct ListingEntry {
    pub(super) html: String,
}

pub(super) fn parse_selector(value: &str) -> Result<Selector> {
    Selector::parse(value).map_err(|error| {
        CrawlError::InvalidSourceSelectors(format!("selector '{value}' is invalid: {error}"))
    })
}

pub(super) fn extract_text(document: &Html, selector: &str) -> Result<Option<String>> {
    let selector = parse_selector(selector)?;
    Ok(document.select(&selector).next().and_then(|element| {
        let name = element.value().name();
        let special = match name {
            "img" => element
                .value()
                .attr("alt")
                .or_else(|| element.value().attr("title")),
            "time" => element.value().attr("datetime"),
            "meta" => element.value().attr("content"),
            _ => None,
        };
        special
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .or_else(|| element_text(element))
    }))
}

pub(super) fn extract_attribute(document: &Html, selector: &str) -> Result<Option<String>> {
    let selector = parse_selector(selector)?;
    Ok(document.select(&selector).next().and_then(|element| {
        element
            .value()
            .attr("href")
            .or_else(|| element.value().attr("data-href"))
            .or_else(|| element.value().attr("src"))
            .map(str::to_owned)
    }))
}

pub(super) fn element_text(element: ElementRef<'_>) -> Option<String> {
    let value = element.text().collect::<Vec<_>>().join(" ");
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    (!value.is_empty()).then_some(value)
}
