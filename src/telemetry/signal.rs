use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunMetrics {
    pub articles_discovered: usize,
    pub articles_persisted: usize,
    pub articles_delivered: usize,
    pub articles_failed: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum IngestionSignal {
    #[serde(rename = "agent.heartbeat")]
    AgentHeartbeat {
        #[serde(rename = "signalId")]
        signal_id: String,
        #[serde(rename = "agentId")]
        agent_id: String,
        #[serde(rename = "emittedAt")]
        emitted_at: DateTime<Utc>,
        version: String,
    },
    #[serde(rename = "agent.reset")]
    AgentReset {
        #[serde(rename = "signalId")]
        signal_id: String,
        #[serde(rename = "agentId")]
        agent_id: String,
        #[serde(rename = "emittedAt")]
        emitted_at: DateTime<Utc>,
        version: String,
    },
    #[serde(rename = "run.preparing")]
    RunPreparing {
        #[serde(flatten)]
        context: RunSignalContext,
    },
    #[serde(rename = "run.started")]
    RunStarted {
        #[serde(flatten)]
        context: RunSignalContext,
    },
    #[serde(rename = "run.progress")]
    RunProgress {
        #[serde(flatten)]
        context: RunSignalContext,
        metrics: RunMetrics,
    },
    #[serde(rename = "run.completed")]
    RunCompleted {
        #[serde(flatten)]
        context: RunSignalContext,
        metrics: RunMetrics,
        #[serde(rename = "durationMs")]
        duration_ms: u64,
    },
    #[serde(rename = "run.failed")]
    RunFailed {
        #[serde(flatten)]
        context: RunSignalContext,
        metrics: RunMetrics,
        #[serde(rename = "durationMs")]
        duration_ms: u64,
        error: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSignalContext {
    pub signal_id: String,
    pub agent_id: String,
    pub emitted_at: DateTime<Utc>,
    pub version: String,
    pub run_id: String,
    pub source_id: String,
}

#[cfg(test)]
#[path = "../../tests/unit/telemetry/signal.rs"]
mod tests;
