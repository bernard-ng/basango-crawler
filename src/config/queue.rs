use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    pub prefix: String,
    pub queues: QueueNames,
    pub redis_url: String,
    pub retention: JobRetention,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            prefix: "basango:crawler".into(),
            queues: QueueNames::default(),
            redis_url: "redis://localhost:6379/0".into(),
            retention: JobRetention::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueNames {
    pub discovery: String,
    pub articles: String,
    pub delivery: String,
}

impl Default for QueueNames {
    fn default() -> Self {
        Self {
            discovery: "discovery".into(),
            articles: "articles".into(),
            delivery: "delivery".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobRetention {
    pub completed: u64,
    pub failed: u64,
}

impl Default for JobRetention {
    fn default() -> Self {
        Self {
            completed: 3_600,
            failed: 86_400,
        }
    }
}
