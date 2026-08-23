//! Retrying asynchronous HTTP client.

use std::time::{Duration, SystemTime};

use rand::Rng;
use reqwest::{
    Method, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER, USER_AGENT},
    redirect::Policy,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::sleep;
use url::Url;

use crate::{
    config::HttpClientConfig,
    error::{CrawlError, Result},
};

use super::user_agent;

/// An owned response keeps callers independent of Reqwest's streaming body.
/// Crawled pages are bounded article/listing documents, so buffering is a
/// reasonable and much simpler trade-off for this application.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub url: Url,
    body: Vec<u8>,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    pub fn text(&self) -> Result<String> {
        // News archives occasionally contain legacy bytes despite claiming
        // UTF-8. Lossy decoding preserves the page's usable text instead of
        // rejecting an otherwise crawlable article.
        Ok(String::from_utf8_lossy(&self.body).into_owned())
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    /// Convert non-2xx statuses into the application's typed error.
    pub fn require_success(self) -> Result<Self> {
        if self.is_success() {
            return Ok(self);
        }
        Err(CrawlError::HttpStatus {
            status: self.status.as_u16(),
            url: self.url.to_string(),
            body: String::from_utf8_lossy(&self.body)
                .chars()
                .take(1_024)
                .collect(),
        })
    }

    pub fn body_lossy(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Cheaply cloneable client; Reqwest internally shares its connection pool.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    options: HttpClientConfig,
}

impl HttpClient {
    pub fn new(options: &HttpClientConfig) -> Result<Self> {
        let redirect = if options.follow_redirects {
            Policy::limited(10)
        } else {
            Policy::none()
        };
        let inner = reqwest::Client::builder()
            .redirect(redirect)
            .timeout(options.timeout())
            // Disabling certificate checks is dangerous. The option is useful
            // for controlled local environments; verification stays enabled.
            .danger_accept_invalid_certs(!options.verify_ssl)
            .build()?;
        Ok(Self {
            inner,
            options: options.clone(),
        })
    }

    pub async fn get(&self, url: &Url) -> Result<HttpResponse> {
        self.request(Method::GET, url, HeaderMap::new(), None).await
    }

    pub async fn get_with_user_agent(&self, url: &Url, agent: &str) -> Result<HttpResponse> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(agent).map_err(|error| {
                CrawlError::Configuration(format!("invalid user-agent header: {error}"))
            })?,
        );
        self.request(Method::GET, url, headers, None).await
    }

    pub async fn post_json<T: Serialize + ?Sized>(
        &self,
        url: &Url,
        headers: &[(&str, &str)],
        value: &T,
    ) -> Result<HttpResponse> {
        let mut header_map = HeaderMap::new();
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                CrawlError::Configuration(format!("invalid HTTP header name: {error}"))
            })?;
            let value = HeaderValue::from_str(value).map_err(|error| {
                CrawlError::Configuration(format!("invalid HTTP header value: {error}"))
            })?;
            header_map.insert(name, value);
        }
        self.request(
            Method::POST,
            url,
            header_map,
            Some(serde_json::to_value(value)?),
        )
        .await
    }

    async fn request(
        &self,
        method: Method,
        url: &Url,
        headers: HeaderMap,
        json: Option<serde_json::Value>,
    ) -> Result<HttpResponse> {
        let max_attempts = self.options.max_retries + 1;

        for attempt in 0..max_attempts {
            let mut request = self
                .inner
                .request(method.clone(), url.clone())
                .headers(headers.clone());
            if !headers.contains_key(USER_AGENT) {
                request = request.header(
                    USER_AGENT,
                    user_agent::choose(self.options.rotate, &self.options.user_agent),
                );
            }
            if let Some(value) = &json {
                request = request.json(value);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let response_url = response.url().clone();
                    let response_headers = response.headers().clone();

                    if is_transient(status) && attempt + 1 < max_attempts {
                        self.delay(attempt, Some(&response_headers)).await;
                        continue;
                    }

                    let body = response.bytes().await?.to_vec();
                    return Ok(HttpResponse {
                        status,
                        headers: response_headers,
                        url: response_url,
                        body,
                    });
                }
                Err(error) if attempt + 1 < max_attempts => {
                    tracing::warn!(attempt = attempt + 1, %url, %error, "HTTP transport failed; retrying");
                    self.delay(attempt, None).await;
                }
                Err(error) => return Err(error.into()),
            }
        }

        unreachable!("the retry loop always returns on its last attempt")
    }

    async fn delay(&self, attempt: u32, headers: Option<&HeaderMap>) {
        let retry_after = headers
            .filter(|_| self.options.respect_retry_after)
            .and_then(|headers| headers.get(RETRY_AFTER))
            .and_then(|value| value.to_str().ok())
            .and_then(parse_retry_after);
        sleep(retry_after.unwrap_or_else(|| self.backoff(attempt))).await;
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let base = (self.options.backoff.initial
            * self.options.backoff.multiplier.powi(attempt as i32))
        .min(self.options.backoff.max);
        let jitter = rand::rng().random_range(0.0..=base * 0.25);
        Duration::from_secs_f64(base + jitter)
    }
}

fn is_transient(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let target = httpdate::parse_http_date(value).ok()?;
    Some(target.duration_since(SystemTime::now()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_seconds() {
        assert_eq!(parse_retry_after("12"), Some(Duration::from_secs(12)));
    }

    #[test]
    fn transient_statuses_are_explicit() {
        assert!(is_transient(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_transient(StatusCode::NOT_FOUND));
    }
}
