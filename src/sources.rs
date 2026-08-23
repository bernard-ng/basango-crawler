//! Source adapters for HTML sites and WordPress REST APIs.
//!
//! Both adapters stream the same domain value (`ArticleDraft`). A bounded
//! channel lets collection overlap persistence while applying backpressure
//! when SQLite or the backend is slower than the source website.

mod common;
mod html;
mod wordpress;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use url::Url;

use crate::{
    config::SourceConfig,
    domain::{ArticleDraft, CrawlRequest},
    error::Result,
    http::HttpClient,
};

/// A discovery result that can be serialized into an article-fetch job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArticleSeed {
    pub url: Url,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

pub enum SourceAdapter {
    Html(html::HtmlCrawler),
    WordPress(wordpress::WordPressCrawler),
}

impl SourceAdapter {
    pub fn new(source: SourceConfig, http: HttpClient) -> Self {
        match source {
            SourceConfig::Html(config) => Self::Html(html::HtmlCrawler::new(config, http)),
            SourceConfig::WordPress(config) => {
                Self::WordPress(wordpress::WordPressCrawler::new(config, http))
            }
        }
    }

    /// Start collecting immediately and return a bounded stream of results.
    pub fn stream(mut self, request: CrawlRequest) -> mpsc::Receiver<Result<ArticleDraft>> {
        const BUFFERED_ARTICLES: usize = 32;
        let (sender, receiver) = mpsc::channel(BUFFERED_ARTICLES);
        tokio::spawn(async move {
            let result = match &mut self {
                Self::Html(crawler) => crawler.crawl_into(&request, &sender).await,
                Self::WordPress(crawler) => crawler.crawl_into(&request, &sender).await,
            };
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        receiver
    }

    pub async fn discover(&mut self, request: &CrawlRequest) -> Result<Vec<ArticleSeed>> {
        match self {
            Self::Html(crawler) => crawler.discover(request).await,
            Self::WordPress(crawler) => crawler.discover(request).await,
        }
    }

    pub async fn collect(
        &mut self,
        seed: &ArticleSeed,
        request: &CrawlRequest,
    ) -> Result<ArticleDraft> {
        match self {
            Self::Html(crawler) => crawler.collect(&seed.url, request).await,
            Self::WordPress(crawler) => crawler.collect(seed, request).await,
        }
    }
}
