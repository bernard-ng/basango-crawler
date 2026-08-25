//! Article ingestion pipeline.
//!
//! A crawler first produces an [`ArticleDraft`].
//! This module then normalizes it, durably stores it, and optionally forwards
//! it. Persisting before network delivery is the *outbox pattern*: a process
//! crash cannot silently lose an article that was already collected.

mod forwarder;
mod normalize;
mod outbox;

pub(crate) use forwarder::endpoint_url;
pub use forwarder::{ArticleIngestionClient, DeliveryResult};
pub use normalize::normalize;
pub use outbox::{DeliveryStatus, Outbox, OutboxEntry, OutboxStats};

use crate::{
    domain::{Article, ArticleDraft},
    error::Result,
};
/// What happened when a draft entered the durable ingestion pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestStatus {
    /// It was saved but no backend is configured, so `deliver` can send it later.
    Persisted,
    /// It had already reached the backend in a previous attempt.
    AlreadyForwarded,
    /// It was saved and forwarded during this call.
    Forwarded,
    /// Forwarding failed; the outbox retains error and retry information.
    DeliveryFailed,
}

/// Normalize, persist, and—when configured—forward one article.
pub async fn ingest(
    draft: ArticleDraft,
    outbox: &Outbox,
    ingestion: Option<&ArticleIngestionClient>,
) -> Result<(Article, IngestStatus)> {
    let article = normalize(draft)?;
    let status = outbox.save(&article)?;

    if status == DeliveryStatus::Forwarded {
        return Ok((article, IngestStatus::AlreadyForwarded));
    }

    let Some(ingestion) = ingestion else {
        return Ok((article, IngestStatus::Persisted));
    };

    // `Outbox` releases its synchronous lock before this network await.
    match ingestion.deliver(&article).await {
        DeliveryResult::Delivered { .. } => {
            outbox.mark_forwarded(&article.hash)?;
            Ok((article, IngestStatus::Forwarded))
        }
        DeliveryResult::Failed {
            retryable, message, ..
        } => {
            tracing::warn!(
                url = %article.link,
                retryable,
                error = %message,
                "article forwarding failed; retained in outbox"
            );
            outbox.mark_failed(&article.hash, &message, retryable)?;
            Ok((article, IngestStatus::DeliveryFailed))
        }
    }
}
