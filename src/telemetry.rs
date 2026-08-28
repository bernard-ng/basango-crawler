//! Operational telemetry emitted by crawler agents.
//!
//! Signals describe what the crawler observed. The API owns the database
//! projection used by the operations dashboard.

mod reporter;
mod signal;

use std::time::Duration;

pub(crate) use reporter::{AgentReporter, RunReporter, agent_id};
pub(crate) use signal::RunMetrics;

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
