//! Generic CSS-selector-driven HTML crawler.

mod parser;

use std::{collections::HashSet, time::Duration};

use regex::Regex;
use scraper::Html;
use tokio::sync::mpsc;
use tokio::time::sleep;
use url::Url;

use crate::{
    config::HtmlSourceConfig,
    domain::{ArticleDraft, CrawlRequest, PageRange},
    error::{CrawlError, Result},
    http::{HttpClient, consume_open_graph_html},
    sources::{ArticleSeed, DiscoveryBatch, common},
};

use parser::{ListingEntry, element_text, extract_attribute, extract_text, parse_selector};

pub struct HtmlCrawler {
    source: HtmlSourceConfig,
    http: HttpClient,
}

impl HtmlCrawler {
    pub fn new(source: HtmlSourceConfig, http: HttpClient) -> Self {
        Self { source, http }
    }

    /// Crawl listings and detail pages directly in one process.
    pub async fn crawl_into(
        &self,
        request: &CrawlRequest,
        sender: &mpsc::Sender<Result<ArticleDraft>>,
    ) -> Result<()> {
        let page_range = match request.page_range {
            Some(range) => range,
            None => self.pagination(request.category.as_deref()).await?,
        };

        tracing::info!(
            source = %self.source.common.id,
            category = request.category.as_deref().unwrap_or("<none>"),
            pages = %page_range,
            date_start = request.date_range.map(|range| range.start.to_rfc3339()).as_deref(),
            date_end = request.date_range.map(|range| range.end.to_rfc3339()).as_deref(),
            "starting HTML crawl"
        );

        let mut successful_pages = 0usize;
        let mut matched_entries = 0usize;
        let mut emitted_articles = 0usize;
        let mut filtered_articles = 0usize;
        let mut detail_fetch_failures = 0usize;
        let mut parse_failures = 0usize;
        let mut missing_links = 0usize;
        let mut first_fetch_error = None;

        'pages: for page in page_range.start..=page_range.end {
            let endpoint = self.endpoint_url(page, request.category.as_deref())?;
            tracing::debug!(
                source = %self.source.common.id,
                %endpoint,
                page,
                "fetching HTML listing"
            );
            let listing = match self.fetch_text(&endpoint).await {
                Ok(listing) => listing,
                Err(error) => {
                    tracing::error!(%error, %endpoint, page, "failed to fetch HTML listing");
                    if first_fetch_error.is_none() {
                        first_fetch_error = Some(error);
                    }
                    continue;
                }
            };
            successful_pages += 1;
            let entries = self.listing_entries(&listing)?;
            matched_entries += entries.len();
            tracing::info!(
                source = %self.source.common.id,
                %endpoint,
                page,
                entries = entries.len(),
                selector = %self.source.selectors.list,
                "parsed HTML listing"
            );
            if entries.is_empty() {
                tracing::warn!(
                    source = %self.source.common.id,
                    page,
                    %endpoint,
                    selector = %self.source.selectors.list,
                    "HTML listing contained no matching articles"
                );
            }

            for entry in entries {
                let Some(link) = self.extract_link(&entry)? else {
                    missing_links += 1;
                    tracing::warn!(page, "skipping HTML listing entry without a link");
                    continue;
                };
                let html = if self.source.fetch_details {
                    match self.fetch_text(&link).await {
                        Ok(html) => html,
                        Err(error) => {
                            detail_fetch_failures += 1;
                            tracing::error!(%error, %link, "failed to fetch HTML detail page");
                            continue;
                        }
                    }
                } else {
                    entry.html
                };

                match self.parse_article(&html, Some(&link), request.category.as_deref()) {
                    Ok(draft) => {
                        if let Some(range) = request.date_range {
                            if range.is_older_than_range(draft.published_at) {
                                // Listings are newest-first. This is a control
                                // signal, not a failure, so stop after collected data.
                                filtered_articles += 1;
                                tracing::info!(
                                    source = %self.source.common.id,
                                    %link,
                                    published_at = %draft.published_at,
                                    range_start = %range.start,
                                    "stopping at article older than the publication date range"
                                );
                                break 'pages;
                            }
                            if !range.contains(draft.published_at) {
                                filtered_articles += 1;
                                tracing::debug!(
                                    source = %self.source.common.id,
                                    %link,
                                    published_at = %draft.published_at,
                                    range_start = %range.start,
                                    range_end = %range.end,
                                    "skipping article outside the publication date range"
                                );
                                continue;
                            }
                        }
                        if sender.send(Ok(draft)).await.is_err() {
                            break 'pages;
                        }
                        emitted_articles += 1;
                    }
                    Err(error) => {
                        parse_failures += 1;
                        tracing::error!(%error, %link, "failed to parse HTML article");
                    }
                }
            }
        }

        if successful_pages == 0 {
            return Err(first_fetch_error.unwrap_or_else(|| {
                CrawlError::Configuration("HTML crawl did not fetch any listing pages".into())
            }));
        }

        tracing::info!(
            source = %self.source.common.id,
            successful_pages,
            matched_entries,
            emitted_articles,
            filtered_articles,
            detail_fetch_failures,
            parse_failures,
            missing_links,
            "HTML crawl finished"
        );
        Ok(())
    }

    /// Discover detail URLs for the Redis-backed execution mode.
    pub async fn discover_into(
        &self,
        request: &CrawlRequest,
        sender: &mpsc::Sender<Result<DiscoveryBatch>>,
    ) -> Result<()> {
        let page_range = match request.page_range {
            Some(range) => range,
            None => self.pagination(request.category.as_deref()).await?,
        };
        tracing::info!(
            source = %self.source.common.id,
            category = request.category.as_deref().unwrap_or("<none>"),
            pages = %page_range,
            "starting HTML discovery"
        );
        let mut seen = HashSet::new();
        let mut total_discovered = 0usize;

        for page in page_range.start..=page_range.end {
            let endpoint = self.endpoint_url(page, request.category.as_deref())?;
            let listing = self.fetch_text(&endpoint).await?;
            let entries = self.listing_entries(&listing)?;
            let mut articles = Vec::new();
            for entry in &entries {
                if let Some(url) = self.extract_link(entry)?
                    && seen.insert(url.clone())
                {
                    articles.push(ArticleSeed { url, data: None });
                }
            }
            total_discovered += articles.len();
            tracing::info!(
                source = %self.source.common.id,
                page,
                last_page = page_range.end,
                entries = entries.len(),
                discovered = articles.len(),
                total_discovered,
                "HTML discovery page completed"
            );
            if sender
                .send(Ok(DiscoveryBatch {
                    id: format!("page:{page}"),
                    articles,
                }))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }

    pub async fn collect(&self, url: &Url, request: &CrawlRequest) -> Result<ArticleDraft> {
        let html = self.fetch_text(url).await?;
        let draft = self.parse_article(&html, Some(url), request.category.as_deref())?;
        if request
            .date_range
            .is_some_and(|range| !range.contains(draft.published_at))
        {
            return Err(CrawlError::ArticleOutOfDateRange {
                url: url.to_string(),
            });
        }
        Ok(draft)
    }

    pub fn endpoint_url(&self, page: u32, category: Option<&str>) -> Result<Url> {
        let mut template = self.source.pagination_template.clone();
        template = template.replace("{category}", category.unwrap_or_default());
        if template.contains("{page}") {
            template = template.replace("{page}", &page.to_string());
        }

        let mut url =
            common::absolute_url(&self.source.common.url, &template).ok_or_else(|| {
                CrawlError::Configuration(format!("invalid pagination template '{template}'"))
            })?;
        if !self.source.pagination_template.contains("{page}") && page > 0 {
            url.query_pairs_mut().append_pair("page", &page.to_string());
        }
        Ok(url)
    }

    async fn pagination(&self, category: Option<&str>) -> Result<PageRange> {
        let fallback = PageRange::new(0, 1)?;
        let url = self.endpoint_url(0, category)?;
        let Ok(html) = self.fetch_text(&url).await else {
            return Ok(fallback);
        };
        let document = Html::parse_document(&html);
        let selector = parse_selector(&self.source.selectors.pagination)?;
        let Some(href) = document
            .select(&selector)
            .filter_map(|element| element.value().attr("href"))
            .next_back()
        else {
            return Ok(fallback);
        };

        let absolute = common::absolute_url(&self.source.common.url, href);
        let page = absolute
            .as_ref()
            .and_then(|url| url.query_pairs().find(|(key, _)| key == "page"))
            .and_then(|(_, value)| value.parse::<u32>().ok())
            .or_else(|| {
                Regex::new(r"(?:page[=/]|[?&]page=)(\d+)")
                    .expect("static regex is valid")
                    .captures(href)
                    .and_then(|captures| captures.get(1))
                    .and_then(|value| value.as_str().parse().ok())
            })
            .unwrap_or(1);
        PageRange::new(0, page.max(1))
    }

    fn listing_entries(&self, html: &str) -> Result<Vec<ListingEntry>> {
        let document = Html::parse_document(html);
        let selector = parse_selector(&self.source.selectors.list)?;
        Ok(document
            .select(&selector)
            .map(|element| ListingEntry {
                html: element.html(),
            })
            .collect())
    }

    fn extract_link(&self, entry: &ListingEntry) -> Result<Option<Url>> {
        let fragment = Html::parse_fragment(&entry.html);
        let selector = parse_selector(&self.source.selectors.link)?;
        let value = fragment.select(&selector).next().and_then(|element| {
            element
                .value()
                .attr("href")
                .or_else(|| element.value().attr("data-href"))
                .or_else(|| element.value().attr("src"))
        });
        Ok(value.and_then(|value| common::absolute_url(&self.source.common.url, value)))
    }

    fn parse_article(
        &self,
        html: &str,
        known_url: Option<&Url>,
        selected_category: Option<&str>,
    ) -> Result<ArticleDraft> {
        let document = Html::parse_document(html);
        let title = extract_text(&document, &self.source.selectors.title)?
            .ok_or_else(|| CrawlError::InvalidArticle("missing article title".into()))?;
        let link = known_url
            .cloned()
            .or_else(|| {
                extract_attribute(&document, &self.source.selectors.link)
                    .ok()
                    .flatten()
                    .and_then(|value| common::absolute_url(&self.source.common.url, &value))
            })
            .ok_or_else(|| CrawlError::InvalidArticle("missing article link".into()))?;
        let raw_date = extract_text(&document, &self.source.selectors.date)?
            .ok_or_else(|| CrawlError::InvalidArticle("missing article date".into()))?;
        let published_at = common::parse_published_at(&raw_date, &self.source.common.date_format)
            .ok_or_else(|| {
            CrawlError::InvalidArticle(format!("cannot parse article date '{raw_date}'"))
        })?;

        let body_selector = parse_selector(&self.source.selectors.body)?;
        let parts: Vec<String> = document
            .select(&body_selector)
            .map(|node| html2md::parse_html(&node.html()))
            .filter(|part| !part.trim().is_empty())
            .collect();
        let body = if parts.is_empty() {
            html2md::parse_html(html)
        } else {
            parts.join("\n")
        };

        let categories = self.extract_categories(&document, selected_category)?;
        let metadata = consume_open_graph_html(html, &link);
        Ok(ArticleDraft {
            title,
            body,
            link,
            source_id: self.source.common.id.clone(),
            categories,
            metadata,
            published_at,
        })
    }

    fn extract_categories(&self, document: &Html, fallback: Option<&str>) -> Result<Vec<String>> {
        let Some(selector) = &self.source.selectors.categories else {
            return Ok(fallback
                .map(|category| vec![category.to_lowercase()])
                .unwrap_or_default());
        };
        let selector = parse_selector(selector)?;
        let mut seen = HashSet::new();
        Ok(document
            .select(&selector)
            .filter_map(element_text)
            .map(|value| value.to_lowercase())
            .filter(|value| seen.insert(value.clone()))
            .collect())
    }

    async fn fetch_text(&self, url: &Url) -> Result<String> {
        if self.source.common.rate_limit {
            // The original config only carries a boolean. One second is a
            // conservative default until a per-source duration is introduced.
            sleep(Duration::from_secs(1)).await;
        }
        self.http.get(url).await?.require_success()?.text()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sources/html.rs"]
mod tests;
