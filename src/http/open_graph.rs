//! Open Graph metadata extraction.

use scraper::{Html, Selector};
use url::Url;

use crate::{
    domain::ArticleMetadata,
    error::Result,
    http::{HttpClient, user_agent::OPEN_GRAPH_USER_AGENT},
};

/// Fetch a page with the Open Graph crawler user-agent and parse its metadata.
pub async fn consume_url(client: &HttpClient, url: &Url) -> Result<Option<ArticleMetadata>> {
    let response = client
        .get_with_user_agent(url, OPEN_GRAPH_USER_AGENT)
        .await?
        .require_success()?;
    Ok(consume_html(&response.text()?, url))
}

/// Extract metadata without a network request when a caller already has HTML.
pub fn consume_html(html: &str, page_url: &Url) -> Option<ArticleMetadata> {
    if html.trim().is_empty() {
        return None;
    }
    let document = Html::parse_document(html);
    let metadata = ArticleMetadata {
        title: pick([meta(&document, "og:title"), text(&document, "title")]),
        description: pick([
            meta(&document, "og:description"),
            meta(&document, "description"),
        ]),
        image: pick([
            meta(&document, "og:image"),
            attribute(&document, "img", "src"),
        ])
        .and_then(|value| absolute_url(page_url, &value)),
        url: pick([
            meta(&document, "og:url"),
            attribute(&document, "link[rel='canonical']", "href"),
            Some(page_url.as_str().to_owned()),
        ])
        .and_then(|value| absolute_url(page_url, &value)),
        author: pick([
            meta(&document, "article:author"),
            meta(&document, "og:article:author"),
        ]),
        published_at: pick([
            meta(&document, "article:published_time"),
            meta(&document, "og:article:published_time"),
        ]),
        updated_at: pick([
            meta(&document, "article:modified_time"),
            meta(&document, "og:article:modified_time"),
        ]),
    };
    (!metadata.is_empty()).then_some(metadata)
}

// Selector parsing can fail, so helpers return `None` rather than panicking on
// malformed markup or an accidentally invalid selector.
fn meta(document: &Html, property: &str) -> Option<String> {
    let selector = Selector::parse(&format!(
        "meta[property='{property}'], meta[name='{property}']"
    ))
    .ok()?;
    document
        .select(&selector)
        .next()?
        .value()
        .attr("content")
        .map(str::to_owned)
}

fn text(document: &Html, selector: &str) -> Option<String> {
    let selector = Selector::parse(selector).ok()?;
    let value = document
        .select(&selector)
        .next()?
        .text()
        .collect::<Vec<_>>()
        .join(" ");
    (!value.trim().is_empty()).then(|| value.trim().to_owned())
}

fn attribute(document: &Html, selector: &str, name: &str) -> Option<String> {
    let selector = Selector::parse(selector).ok()?;
    document
        .select(&selector)
        .next()?
        .value()
        .attr(name)
        .map(str::to_owned)
}

fn pick<const N: usize>(values: [Option<String>; N]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

fn absolute_url(base: &Url, value: &str) -> Option<Url> {
    Url::parse(value).or_else(|_| base.join(value)).ok()
}

#[cfg(test)]
#[path = "../../tests/unit/http/open_graph.rs"]
mod tests;
