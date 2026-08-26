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
            articles_processed: 3,
            articles_persisted: 2,
            articles_skipped: 1,
            articles_delivered: 1,
            articles_failed: 0,
        },
    };

    let value = serde_json::to_value(signal).unwrap();
    assert_eq!(value["type"], "run.progress");
    assert_eq!(value["signalId"], "signal-1");
    assert_eq!(value["metrics"]["articlesDelivered"], 1);
    assert_eq!(value["metrics"]["articlesProcessed"], 3);
    assert_eq!(value["metrics"]["articlesSkipped"], 1);
    assert!(value.get("event").is_none());
}

#[test]
fn serializes_agent_reset_without_run_context() {
    let signal = IngestionSignal::AgentReset {
        signal_id: "signal-1".into(),
        agent_id: "basango-pi-01".into(),
        emitted_at: Utc.with_ymd_and_hms(2026, 8, 24, 12, 0, 0).unwrap(),
        version: "1.0.0".into(),
    };

    let value = serde_json::to_value(signal).unwrap();
    assert_eq!(value["type"], "agent.reset");
    assert!(value.get("runId").is_none());
}
