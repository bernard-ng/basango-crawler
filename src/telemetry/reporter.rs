use std::env;

use chrono::Utc;
use uuid::Uuid;

use crate::{
    articles::endpoint_url,
    config::IngestionApiConfig,
    http::HttpClient,
    telemetry::signal::{IngestionSignal, RunSignalContext},
};

use super::RunMetrics;

/// Telemetry available to a long-lived worker agent.
#[derive(Clone)]
pub struct AgentReporter {
    publisher: SignalPublisher,
}

impl AgentReporter {
    pub fn new(config: &IngestionApiConfig, client: HttpClient) -> Self {
        Self {
            publisher: SignalPublisher::new(config, client),
        }
    }

    pub async fn heartbeat(&self) {
        self.publisher.heartbeat().await;
    }
}

/// Telemetry scoped to one source run.
///
/// Keeping run identity in this type makes it impossible for agent-only code
/// to accidentally emit a run signal without a run or source identifier.
#[derive(Clone)]
pub struct RunReporter {
    publisher: SignalPublisher,
    run_id: String,
    source_id: String,
}

impl RunReporter {
    pub fn new(config: &IngestionApiConfig, client: HttpClient, source_id: &str) -> Self {
        Self {
            publisher: SignalPublisher::new(config, client),
            run_id: Uuid::now_v7().to_string(),
            source_id: source_id.to_owned(),
        }
    }

    pub async fn heartbeat(&self) {
        self.publisher.heartbeat().await;
    }

    pub async fn preparing(&self) {
        self.publisher
            .publish(IngestionSignal::RunPreparing {
                context: self.context(),
            })
            .await;
    }

    pub async fn started(&self) {
        self.publisher
            .publish(IngestionSignal::RunStarted {
                context: self.context(),
            })
            .await;
    }

    pub async fn progress(&self, metrics: RunMetrics) {
        self.publisher
            .publish(IngestionSignal::RunProgress {
                context: self.context(),
                metrics,
            })
            .await;
    }

    pub async fn completed(&self, metrics: RunMetrics, duration_ms: u64) {
        self.publisher
            .publish(IngestionSignal::RunCompleted {
                context: self.context(),
                metrics,
                duration_ms,
            })
            .await;
    }

    pub async fn failed(&self, metrics: RunMetrics, duration_ms: u64, error: String) {
        self.publisher
            .publish(IngestionSignal::RunFailed {
                context: self.context(),
                metrics,
                duration_ms,
                error,
            })
            .await;
    }

    fn context(&self) -> RunSignalContext {
        RunSignalContext {
            signal_id: signal_id(),
            agent_id: self.publisher.agent_id.clone(),
            emitted_at: Utc::now(),
            version: self.publisher.version.clone(),
            run_id: self.run_id.clone(),
            source_id: self.source_id.clone(),
        }
    }
}

#[derive(Clone)]
struct SignalPublisher {
    agent_id: String,
    client: HttpClient,
    endpoint: Option<url::Url>,
    token: String,
    version: String,
}

impl SignalPublisher {
    fn new(config: &IngestionApiConfig, client: HttpClient) -> Self {
        let endpoint = config
            .endpoint
            .as_ref()
            .and_then(|base| endpoint_url(base, "ingest/signals").ok());
        Self {
            agent_id: agent_id(),
            client,
            endpoint,
            token: config.token.clone(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }

    async fn heartbeat(&self) {
        self.publish(IngestionSignal::AgentHeartbeat {
            signal_id: signal_id(),
            agent_id: self.agent_id.clone(),
            emitted_at: Utc::now(),
            version: self.version.clone(),
        })
        .await;
    }

    async fn publish(&self, signal: IngestionSignal) {
        match serde_json::to_string(&signal) {
            Ok(serialized) => tracing::info!(signal = serialized, "ingestion signal"),
            Err(error) => tracing::warn!(%error, "could not serialize ingestion signal"),
        }

        let Some(endpoint) = &self.endpoint else {
            return;
        };
        let headers = [("Authorization", self.token.as_str())];
        match self.client.post_json(endpoint, &headers, &signal).await {
            Ok(response) if response.is_success() => {}
            Ok(response) => tracing::warn!(
                status = %response.status,
                body = %response.body_lossy(),
                "ingestion API rejected a signal"
            ),
            Err(error) => tracing::warn!(%error, "could not publish ingestion signal"),
        }
    }
}

fn signal_id() -> String {
    Uuid::now_v7().to_string()
}

fn agent_id() -> String {
    if let Ok(value) = env::var("BASANGO_CRAWLER_AGENT_ID") {
        if !value.trim().is_empty() {
            return value;
        }
    }
    // Keep accepting the former variable while deployments migrate.
    if let Ok(value) = env::var("BASANGO_CRAWLER_NODE_ID") {
        if !value.trim().is_empty() {
            return value;
        }
    }
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| format!("crawler-{}", std::process::id()))
}
