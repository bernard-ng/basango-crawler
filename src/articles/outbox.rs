//! Durable SQLite article outbox.
//!
//! SQLite is synchronous, so these methods are intentionally short and no lock
//! is held across `.await`. Clones share one connection inside a process; WAL
//! mode still allows other crawler processes to coexist safely.

use std::{
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
};

use chrono::{DateTime, Duration, Utc};
use rusqlite::{
    Connection, OptionalExtension, TransactionBehavior, named_params, params, types::Type,
};

use crate::{
    domain::Article,
    error::{CrawlError, Result},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Forwarded,
    Failed,
}

impl FromStr for DeliveryStatus {
    type Err = CrawlError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "forwarded" => Ok(Self::Forwarded),
            "failed" => Ok(Self::Failed),
            other => Err(CrawlError::Configuration(format!(
                "unknown outbox status '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub article: Article,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub retryable: bool,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub forwarded_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub claimed_by: Option<String>,
}

#[derive(Clone)]
pub struct Outbox {
    connection: Arc<Mutex<Connection>>,
}

impl Outbox {
    pub fn open(path: &Path, create: bool) -> Result<Self> {
        if !create && !path.exists() {
            return Err(CrawlError::Configuration(format!(
                "SQLite outbox does not exist: {}",
                path.display()
            )));
        }
        if create
            && let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let connection = Connection::open(path)?;
        let outbox = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        outbox.migrate()?;
        Ok(outbox)
    }

    pub fn exists(path: &Path) -> bool {
        path.exists()
    }

    /// Upsert by hash while preserving an already-forwarded state. This is the
    /// idempotency boundary: crawling the same URL twice does not redeliver it.
    pub fn save(&self, article: &Article) -> Result<DeliveryStatus> {
        let timestamp = Utc::now().to_rfc3339();
        let categories = serde_json::to_string(&article.categories)?;
        let metadata = article
            .metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let payload = serde_json::to_string(article)?;

        let connection = self.connection()?;
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
                ":hash": article.hash,
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
            [&article.hash],
            |row| row.get(0),
        )?;
        status.parse()
    }

    pub fn list_pending(&self, source_id: Option<&str>, limit: usize) -> Result<Vec<OutboxEntry>> {
        let sql = if source_id.is_some() {
            r#"SELECT payload, status, attempts, retryable, last_error, created_at,
                      updated_at, forwarded_at, claimed_at, claimed_by
               FROM articles
               WHERE status IN ('pending', 'failed') AND retryable = 1 AND source_id = ?1
               ORDER BY created_at ASC LIMIT ?2"#
        } else {
            r#"SELECT payload, status, attempts, retryable, last_error, created_at,
                      updated_at, forwarded_at, claimed_at, claimed_by
               FROM articles
               WHERE status IN ('pending', 'failed') AND retryable = 1
               ORDER BY created_at ASC LIMIT ?1"#
        };
        let connection = self.connection()?;
        let mut statement = connection.prepare(sql)?;
        let rows = if let Some(source_id) = source_id {
            statement.query_map(params![source_id, limit as i64], row_to_article)?
        } else {
            statement.query_map([limit as i64], row_to_article)?
        };
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Atomically reserve a batch for one pusher.
    ///
    /// `IMMEDIATE` obtains SQLite's write reservation before selecting rows.
    /// Without that transaction boundary, two processes could select and send
    /// the same pending articles concurrently.
    pub fn claim(
        &self,
        claimed_by: &str,
        source_id: Option<&str>,
        limit: usize,
        claim_ttl: Duration,
    ) -> Result<Vec<OutboxEntry>> {
        let now = Utc::now();
        let expires_before = now - claim_ttl;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let select = if source_id.is_some() {
            r#"SELECT hash FROM articles
               WHERE status IN ('pending', 'failed') AND retryable = 1
                 AND source_id = ?1 AND (claimed_at IS NULL OR claimed_at < ?2)
               ORDER BY created_at ASC LIMIT ?3"#
        } else {
            r#"SELECT hash FROM articles
               WHERE status IN ('pending', 'failed') AND retryable = 1
                 AND (claimed_at IS NULL OR claimed_at < ?1)
               ORDER BY created_at ASC LIMIT ?2"#
        };
        let hashes = {
            let mut statement = transaction.prepare(select)?;
            if let Some(source_id) = source_id {
                statement
                    .query_map(
                        params![source_id, expires_before.to_rfc3339(), limit as i64],
                        |row| row.get::<_, String>(0),
                    )?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            } else {
                statement
                    .query_map(params![expires_before.to_rfc3339(), limit as i64], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            }
        };

        for hash in &hashes {
            transaction.execute(
                "UPDATE articles SET claimed_at = ?1, claimed_by = ?2, updated_at = ?1 WHERE hash = ?3",
                params![now.to_rfc3339(), claimed_by, hash],
            )?;
        }
        transaction.commit()?;
        drop(connection);

        hashes
            .iter()
            .map(|hash| {
                self.get(hash)?
                    .ok_or_else(|| CrawlError::Queue("claimed article disappeared".into()))
            })
            .collect()
    }

    pub fn mark_forwarded(&self, hash: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.connection()?.execute(
            r#"UPDATE articles SET status = 'forwarded', last_error = NULL,
                 retryable = 0, updated_at = ?1, forwarded_at = ?1,
                 claimed_at = NULL, claimed_by = NULL WHERE hash = ?2"#,
            params![now, hash],
        )?;
        Ok(())
    }

    pub fn mark_failed(&self, hash: &str, error: &str, retryable: bool) -> Result<()> {
        self.connection()?.execute(
            r#"UPDATE articles SET status = 'failed', attempts = attempts + 1,
                 retryable = ?1, last_error = ?2, updated_at = ?3,
                 claimed_at = NULL, claimed_by = NULL WHERE hash = ?4"#,
            params![retryable, error, Utc::now().to_rfc3339(), hash],
        )?;
        Ok(())
    }

    pub fn get(&self, hash: &str) -> Result<Option<OutboxEntry>> {
        self.connection()?
            .query_row(
                r#"SELECT payload, status, attempts, retryable, last_error, created_at,
                          updated_at, forwarded_at, claimed_at, claimed_by
                   FROM articles WHERE hash = ?1"#,
                [hash],
                row_to_article,
            )
            .optional()
            .map_err(Into::into)
    }

    fn migrate(&self) -> Result<()> {
        self.connection()?.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS articles (
                hash TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                link TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                categories TEXT NOT NULL DEFAULT '[]',
                metadata TEXT,
                published_at TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'forwarded', 'failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                retryable INTEGER NOT NULL DEFAULT 1,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                forwarded_at TEXT,
                claimed_at TEXT,
                claimed_by TEXT
            );
            CREATE INDEX IF NOT EXISTS articles_status_created_at_idx
                ON articles(status, created_at);
            CREATE INDEX IF NOT EXISTS articles_source_status_idx
                ON articles(source_id, status);
            CREATE INDEX IF NOT EXISTS articles_claimed_at_created_at_idx
                ON articles(claimed_at, created_at);
            "#,
        )?;
        Ok(())
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| CrawlError::OutboxLockPoisoned)
    }
}

fn row_to_article(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
    let payload: String = row.get(0)?;
    let status: String = row.get(1)?;
    Ok(OutboxEntry {
        article: serde_json::from_str(&payload).map_err(|error| sql_conversion(0, error))?,
        status: status.parse().map_err(|error| sql_conversion(1, error))?,
        attempts: row.get(2)?,
        retryable: row.get(3)?,
        last_error: row.get(4)?,
        created_at: parse_sql_date(row, 5)?,
        updated_at: parse_sql_date(row, 6)?,
        forwarded_at: parse_optional_sql_date(row, 7)?,
        claimed_at: parse_optional_sql_date(row, 8)?,
        claimed_by: row.get(9)?,
    })
}

fn parse_sql_date(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    let value: String = row.get(index)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|date| date.with_timezone(&Utc))
        .map_err(|error| sql_conversion(index, error))
}

fn parse_optional_sql_date(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let value: Option<String> = row.get(index)?;
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|date| date.with_timezone(&Utc))
                .map_err(|error| sql_conversion(index, error))
        })
        .transpose()
}

fn sql_conversion(
    index: usize,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::tempdir;
    use url::Url;

    use super::*;

    fn article() -> Article {
        Article {
            hash: "hash-1".into(),
            title: "Title".into(),
            body: "Body".into(),
            link: Url::parse("https://example.com/one").unwrap(),
            source_id: crate::domain::SourceId::new("example").unwrap(),
            categories: vec!["news".into()],
            metadata: None,
            published_at: Utc::now(),
        }
    }

    #[test]
    fn forwarded_rows_stay_forwarded_when_saved_again() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("outbox.db");
        let outbox = Outbox::open(&path, true).unwrap();
        let article = article();

        assert_eq!(outbox.save(&article).unwrap(), DeliveryStatus::Pending);
        outbox.mark_forwarded(&article.hash).unwrap();
        assert_eq!(outbox.save(&article).unwrap(), DeliveryStatus::Forwarded);
    }

    #[test]
    fn claim_reserves_pending_rows() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("outbox.db");
        let outbox = Outbox::open(&path, true).unwrap();
        outbox.save(&article()).unwrap();

        let claimed = outbox
            .claim("worker-1", None, 10, Duration::minutes(15))
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].claimed_by.as_deref(), Some("worker-1"));

        let second = outbox
            .claim("worker-2", None, 10, Duration::minutes(15))
            .unwrap();
        assert!(second.is_empty());
    }
}
