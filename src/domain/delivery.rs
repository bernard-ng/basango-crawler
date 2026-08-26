/// Durable state of an article's delivery to the ingestion API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryState {
    Pending,
    Forwarded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered {
        status: u16,
    },
    Failed {
        decision: RetryDecision,
        status: Option<u16>,
        message: String,
    },
}
