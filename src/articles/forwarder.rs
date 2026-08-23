//! Delivery of persisted articles to the Basango ingestion API.

use url::Url;

use crate::{config::IngestionApiConfig, domain::Article, error::Result, http::HttpClient};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryResult {
    Delivered {
        status: u16,
    },
    Failed {
        retryable: bool,
        status: Option<u16>,
        message: String,
    },
}

#[derive(Clone)]
pub struct ArticleIngestionClient {
    client: HttpClient,
    endpoint: Url,
    token: String,
}

impl ArticleIngestionClient {
    pub fn new(config: &IngestionApiConfig, client: HttpClient) -> Result<Option<Self>> {
        let Some(endpoint) = config.endpoint.clone() else {
            return Ok(None);
        };
        Ok(Some(Self {
            client,
            endpoint,
            token: config.token.clone(),
        }))
    }

    pub async fn deliver(&self, article: &Article) -> DeliveryResult {
        let endpoint = match endpoint_url(&self.endpoint, "ingest/articles") {
            Ok(endpoint) => endpoint,
            Err(error) => {
                return DeliveryResult::Failed {
                    retryable: false,
                    status: None,
                    message: error.to_string(),
                };
            }
        };
        let headers = [
            ("Authorization", self.token.as_str()),
            ("Idempotency-Key", article.hash.as_str()),
        ];

        match self.client.post_json(&endpoint, &headers, article).await {
            Ok(response) if response.is_success() => DeliveryResult::Delivered {
                status: response.status.as_u16(),
            },
            Ok(response) => {
                let status = response.status.as_u16();
                DeliveryResult::Failed {
                    retryable: is_retryable_status(status),
                    status: Some(status),
                    message: format!(
                        "forwarding failed with HTTP {status}: {}",
                        response.body_lossy()
                    ),
                }
            }
            Err(error) => DeliveryResult::Failed {
                retryable: true,
                status: None,
                message: error.to_string(),
            },
        }
    }
}

/// Append a path segment without `Url::join`'s “replace the final segment”
/// behavior. API base URLs often contain a path such as `/crawler`.
pub(crate) fn endpoint_url(base: &Url, segment: &str) -> Result<Url> {
    let mut result = base.clone();
    let mut path = result.path().trim_end_matches('/').to_owned();
    path.push('/');
    path.push_str(segment.trim_start_matches('/'));
    result.set_path(&path);
    Ok(result)
}

fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429) || status >= 500
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_endpoint_to_an_existing_base_path() {
        let base = Url::parse("https://api.example.com/crawler").unwrap();
        assert_eq!(
            endpoint_url(&base, "ingest/articles").unwrap().as_str(),
            "https://api.example.com/crawler/ingest/articles"
        );
    }

    #[test]
    fn retryable_statuses_match_transport_semantics() {
        assert!(is_retryable_status(503));
        assert!(!is_retryable_status(422));
    }
}
