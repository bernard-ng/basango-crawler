//! WordPress REST API source adapter.

use std::{collections::HashMap, time::Duration};

use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::sleep;
use url::Url;

use crate::{
    config::{MetadataStrategy, WordPressSourceConfig},
    domain::{ArticleDraft, ArticleMetadata, CrawlRequest, PageRange},
    error::{CrawlError, Result},
    http::{HttpClient, consume_open_graph_url},
    sources::{ArticleSeed, DiscoveryBatch, common},
};

const POST_FIELDS: &str =
    "date,slug,link,title.rendered,content.rendered,excerpt.rendered,categories,yoast_head_json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RenderedField {
    #[serde(default)]
    pub rendered: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WordPressPost {
    #[serde(default)]
    pub categories: Vec<u64>,
    #[serde(default)]
    pub content: RenderedField,
    pub date: Option<String>,
    #[serde(default)]
    pub excerpt: RenderedField,
    pub link: Option<Url>,
    pub slug: Option<String>,
    #[serde(default)]
    pub title: RenderedField,
    pub yoast_head_json: Option<YoastMetadata>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YoastMetadata {
    pub article_modified_time: Option<String>,
    pub article_published_time: Option<String>,
    pub author: Option<String>,
    pub description: Option<String>,
    pub og_description: Option<String>,
    #[serde(default)]
    pub og_image: Vec<YoastImage>,
    pub og_title: Option<String>,
    pub og_url: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YoastImage {
    pub url: Option<String>,
}

pub struct WordPressCrawler {
    source: WordPressSourceConfig,
    http: HttpClient,
    categories: HashMap<u64, String>,
}

impl WordPressCrawler {
    pub fn new(source: WordPressSourceConfig, http: HttpClient) -> Self {
        Self {
            source,
            http,
            categories: HashMap::new(),
        }
    }

    pub async fn crawl_into(
        &mut self,
        request: &CrawlRequest,
        sender: &mpsc::Sender<Result<ArticleDraft>>,
    ) -> Result<()> {
        let range = match request.page_range {
            Some(range) => range,
            None => self.pagination().await?,
        };

        for page in range.start..=range.end {
            let posts = match self.fetch_page(page).await {
                Ok(posts) => posts,
                Err(error) => {
                    tracing::error!(%error, page, source = %self.source.common.id, "failed to fetch WordPress page");
                    continue;
                }
            };
            for post in posts {
                match self.post_to_draft(&post).await {
                    Ok(draft) => {
                        if let Some(range) = request.date_range {
                            if range.is_older_than_range(draft.published_at) {
                                return Ok(());
                            }
                            if !range.contains(draft.published_at) {
                                continue;
                            }
                        }
                        if sender.send(Ok(draft)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(error) => tracing::error!(%error, "failed to parse WordPress article"),
                }
            }
        }
        Ok(())
    }

    pub async fn discover_into(
        &self,
        request: &CrawlRequest,
        sender: &mpsc::Sender<Result<DiscoveryBatch>>,
    ) -> Result<()> {
        let range = match request.page_range {
            Some(range) => range,
            None => self.pagination().await?,
        };
        tracing::info!(
            source = %self.source.common.id,
            pages = %range,
            "starting WordPress discovery"
        );
        let mut total_discovered = 0usize;
        for page in range.start..=range.end {
            let posts = self.fetch_page(page).await?;
            let mut articles = Vec::new();
            for post in &posts {
                if let Some(url) = post.link.clone() {
                    articles.push(ArticleSeed {
                        url,
                        data: Some(serde_json::to_value(post)?),
                    });
                }
            }
            total_discovered += articles.len();
            tracing::info!(
                source = %self.source.common.id,
                page,
                last_page = range.end,
                posts = posts.len(),
                discovered = articles.len(),
                total_discovered,
                "WordPress discovery page completed"
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

    pub async fn collect(
        &mut self,
        seed: &ArticleSeed,
        request: &CrawlRequest,
    ) -> Result<ArticleDraft> {
        let value = seed.data.as_ref().ok_or_else(|| {
            CrawlError::InvalidArticle("WordPress details job is missing its REST payload".into())
        })?;
        let post: WordPressPost = serde_json::from_value(value.clone())?;
        let draft = self.post_to_draft(&post).await?;
        if request
            .date_range
            .is_some_and(|range| !range.contains(draft.published_at))
        {
            return Err(CrawlError::ArticleOutOfDateRange {
                url: seed.url.to_string(),
            });
        }
        Ok(draft)
    }

    async fn post_to_draft(&mut self, post: &WordPressPost) -> Result<ArticleDraft> {
        let link = post
            .link
            .clone()
            .ok_or_else(|| CrawlError::InvalidArticle("missing WordPress article link".into()))?;
        let title = common::text_from_html(&post.title.rendered)
            .or_else(|| post.slug.clone())
            .unwrap_or_else(|| "Untitled".into());
        let raw_date = post
            .date
            .as_deref()
            .ok_or_else(|| CrawlError::InvalidArticle("missing WordPress article date".into()))?;
        let published_at = common::parse_published_at(raw_date, &self.source.common.date_format)
            .ok_or_else(|| {
                CrawlError::InvalidArticle(format!("cannot parse WordPress date '{raw_date}'"))
            })?;
        let categories = self.map_categories(&post.categories).await;
        let metadata = self.metadata(post, &link).await;

        Ok(ArticleDraft {
            title,
            body: html2md::parse_html(&post.content.rendered),
            link,
            source_id: self.source.common.id.clone(),
            categories,
            metadata,
            published_at,
        })
    }

    async fn metadata(&self, post: &WordPressPost, link: &Url) -> Option<ArticleMetadata> {
        let strategy = self.source.metadata_strategy;
        let extracted = match strategy {
            MetadataStrategy::None | MetadataStrategy::Fetch => None,
            MetadataStrategy::Yoast => yoast_metadata(post, link),
            MetadataStrategy::Rest => rest_metadata(post),
            MetadataStrategy::Auto => yoast_metadata(post, link).or_else(|| rest_metadata(post)),
        };
        let should_fetch = matches!(strategy, MetadataStrategy::Fetch)
            || matches!(strategy, MetadataStrategy::Auto) && extracted.is_none();
        if should_fetch {
            consume_open_graph_url(&self.http, link)
                .await
                .ok()
                .flatten()
        } else {
            extracted
        }
    }

    async fn pagination(&self) -> Result<PageRange> {
        let mut url = self.api_url("wp-json/wp/v2/posts")?;
        url.query_pairs_mut()
            .append_pair("_fields", "id")
            .append_pair("per_page", "100");
        let response = self.fetch(&url).await?;
        let pages = header_number(&response.headers, "x-wp-totalpages").unwrap_or(1);
        let posts = header_number(&response.headers, "x-wp-total").unwrap_or(0);
        tracing::info!(
            pages,
            posts,
            source = %self.source.common.id,
            "WordPress pagination"
        );
        PageRange::new(1, pages.max(1))
    }

    fn page_url(&self, page: u32) -> Result<Url> {
        let mut url = self.api_url("wp-json/wp/v2/posts")?;
        url.query_pairs_mut()
            .append_pair("_fields", POST_FIELDS)
            .append_pair("orderby", "date")
            .append_pair("order", "desc")
            .append_pair("page", &page.to_string())
            .append_pair("per_page", "100");
        Ok(url)
    }

    async fn fetch_page(&self, page: u32) -> Result<Vec<WordPressPost>> {
        self.fetch(&self.page_url(page)?)
            .await?
            .require_success()?
            .json()
    }

    async fn fetch_categories(&mut self) -> Result<()> {
        let mut url = self.api_url("wp-json/wp/v2/categories")?;
        url.query_pairs_mut()
            .append_pair("_fields", "id,slug,count")
            .append_pair("orderby", "count")
            .append_pair("order", "desc")
            .append_pair("per_page", "100");
        let categories: Vec<WordPressCategory> =
            self.fetch(&url).await?.require_success()?.json()?;
        self.categories
            .extend(categories.into_iter().map(|item| (item.id, item.slug)));
        Ok(())
    }

    async fn map_categories(&mut self, ids: &[u64]) -> Vec<String> {
        if self.categories.is_empty()
            && let Err(error) = self.fetch_categories().await
        {
            tracing::warn!(%error, "failed to fetch WordPress categories");
        }
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.into_iter()
            .filter_map(|id| self.categories.get(&id).cloned())
            .collect()
    }

    fn api_url(&self, path: &str) -> Result<Url> {
        // `Url::join` treats a base without a trailing slash as a file. Force a
        // directory base so a configured subpath is not accidentally replaced.
        let mut base = self.source.common.url.clone();
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        base.join(path).map_err(Into::into)
    }

    async fn fetch(&self, url: &Url) -> Result<crate::http::HttpResponse> {
        if self.source.common.rate_limit {
            sleep(Duration::from_secs(1)).await;
        }
        self.http.get(url).await
    }
}

#[derive(Debug, Deserialize)]
struct WordPressCategory {
    id: u64,
    slug: String,
}

fn header_number(headers: &HeaderMap, name: &str) -> Option<u32> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}

fn yoast_metadata(post: &WordPressPost, link: &Url) -> Option<ArticleMetadata> {
    let yoast = post.yoast_head_json.as_ref()?;
    let metadata = ArticleMetadata {
        author: pick([yoast.author.clone()]),
        description: pick([yoast.og_description.clone(), yoast.description.clone()]),
        image: pick([yoast.og_image.iter().find_map(|image| image.url.clone())])
            .and_then(|value| common::absolute_url(link, &value)),
        published_at: pick([yoast.article_published_time.clone(), post.date.clone()]),
        title: pick([yoast.og_title.clone(), yoast.title.clone()]),
        updated_at: pick([yoast.article_modified_time.clone()]),
        url: pick([yoast.og_url.clone(), Some(link.to_string())])
            .and_then(|value| common::absolute_url(link, &value)),
    };
    (!metadata.is_empty()).then_some(metadata)
}

fn rest_metadata(post: &WordPressPost) -> Option<ArticleMetadata> {
    let metadata = ArticleMetadata {
        title: common::text_from_html(&post.title.rendered),
        description: common::text_from_html(&post.excerpt.rendered),
        url: post.link.clone(),
        published_at: post.date.clone(),
        ..ArticleMetadata::default()
    };
    (!metadata.is_empty()).then_some(metadata)
}

fn pick<const N: usize>(values: [Option<String>; N]) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_yoast_metadata() {
        let link = Url::parse("https://example.com/story").unwrap();
        let post = WordPressPost {
            link: Some(link.clone()),
            yoast_head_json: Some(YoastMetadata {
                og_title: Some("Yoast title".into()),
                og_image: vec![YoastImage {
                    url: Some("/cover.jpg".into()),
                }],
                ..YoastMetadata::default()
            }),
            ..WordPressPost::default()
        };
        let metadata = yoast_metadata(&post, &link).unwrap();
        assert_eq!(metadata.title.as_deref(), Some("Yoast title"));
        assert_eq!(
            metadata.image.unwrap().as_str(),
            "https://example.com/cover.jpg"
        );
    }

    #[test]
    fn parses_wordpress_naive_datetime_as_utc() {
        let date: chrono::DateTime<chrono::Utc> =
            common::parse_published_at("2025-01-02T03:04:05", "yyyy-LL-dd'T'HH:mm:ss").unwrap();
        assert_eq!(date.to_rfc3339(), "2025-01-02T03:04:05+00:00");
    }
}
