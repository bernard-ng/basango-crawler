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
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn serializes_a_discriminated_progress_signal() {
        let signal = IngestionSignal::RunProgress {
            context: RunSignalContext {
                signal_id: "signal-1".into(),
                agent_id: "agent-1".into(),
                emitted_at: Utc.with_ymd_and_hms(2026, 8, 23, 12, 0, 0).unwrap(),
                version: "1.0.0".into(),
                run_id: "run-1".into(),
                source_id: "source-1".into(),
            },
            metrics: RunMetrics {
                articles_discovered: 3,
                articles_persisted: 2,
                articles_delivered: 1,
                articles_failed: 0,
            },
        };

        let value = serde_json::to_value(signal).unwrap();
        assert_eq!(value["type"], "run.progress");
        assert_eq!(value["signalId"], "signal-1");
        assert_eq!(value["metrics"]["articlesDelivered"], 1);
        assert!(value.get("event").is_none());
    }
}
