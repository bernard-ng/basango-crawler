use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpClientConfig {
    pub backoff: BackoffConfig,
    pub follow_redirects: bool,
    pub max_retries: u32,
    pub respect_retry_after: bool,
    pub rotate: bool,
    pub timeout: u64,
    pub user_agent: String,
    pub verify_ssl: bool,
}

impl HttpClientConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout)
    }
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            backoff: BackoffConfig::default(),
            follow_redirects: true,
            max_retries: 3,
            respect_retry_after: true,
            rotate: true,
            timeout: 20,
            user_agent: "Basango/0.1 (+https://github.com/bernard-ng/basango)".into(),
            verify_ssl: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffConfig {
    pub initial: f64,
    pub max: f64,
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: 1.0,
            max: 30.0,
            multiplier: 2.0,
        }
    }
}
