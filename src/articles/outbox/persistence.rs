use chrono::Utc;
use rusqlite::{Connection, named_params};

use crate::{domain::Article, error::Result};

use super::DeliveryStatus;

pub(super) fn save_article(connection: &Connection, article: &Article) -> Result<DeliveryStatus> {
    let timestamp = Utc::now().to_rfc3339();
    let categories = serde_json::to_string(&article.categories)?;
    let metadata = article
        .metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let payload = serde_json::to_string(article)?;

    connection.execute(
        r#"
        INSERT INTO articles (
            hash, source_id, link, title, body, categories, metadata,
            published_at, payload, status, attempts, retryable,
            last_error, created_at, updated_at, forwarded_at
        )
        VALUES (
            :hash, :source_id, :link, :title, :body, :categories, :metadata,
            :published_at, :payload, 'pending', 0, 1, NULL, :now, :now, NULL
        )
        ON CONFLICT(hash) DO UPDATE SET
            source_id = excluded.source_id,
            link = excluded.link,
            title = excluded.title,
            body = excluded.body,
            categories = excluded.categories,
            metadata = excluded.metadata,
            published_at = excluded.published_at,
            payload = excluded.payload,
            status = CASE WHEN articles.status = 'forwarded' THEN 'forwarded' ELSE 'pending' END,
            last_error = CASE WHEN articles.status = 'forwarded' THEN articles.last_error ELSE NULL END,
            retryable = CASE WHEN articles.status = 'forwarded' THEN articles.retryable ELSE 1 END,
            updated_at = excluded.updated_at,
            forwarded_at = CASE WHEN articles.status = 'forwarded' THEN articles.forwarded_at ELSE NULL END,
            claimed_at = NULL,
            claimed_by = NULL
        "#,
        named_params! {
            ":hash": article.hash.as_str(),
            ":source_id": article.source_id.as_str(),
            ":link": article.link.as_str(),
            ":title": article.title,
            ":body": article.body,
            ":categories": categories,
            ":metadata": metadata,
            ":published_at": article.published_at.to_rfc3339(),
            ":payload": payload,
            ":now": timestamp,
        },
    )?;

    let status: String = connection.query_row(
        "SELECT status FROM articles WHERE hash = ?1",
        [article.hash.as_str()],
        |row| row.get(0),
    )?;
    status.parse()
}
