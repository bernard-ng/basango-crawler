use serde::{Deserialize, Serialize};

use crate::domain::UpdateDirection;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CrawlerRuntimeConfig {
    pub direction: UpdateDirection,
    pub worker_concurrency: usize,
}

impl Default for CrawlerRuntimeConfig {
    fn default() -> Self {
        Self {
            direction: UpdateDirection::Forward,
            worker_concurrency: 5,
        }
    }
}
