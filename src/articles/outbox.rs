//! Durable SQLite article outbox.
//!
//! SQLite is synchronous, so these methods are intentionally short and no lock
//! is held across `.await`. Clones share one connection inside a process; WAL
//! mode still allows other crawler processes to coexist safely.

mod intents;
mod model;
mod persistence;

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, types::Type};

use crate::{
    domain::Article,
    error::{CrawlError, Result},
};

pub use model::{DeliveryIntent, DeliveryStatus, OutboxEntry, OutboxStats};
use persistence::save_article;

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

    /// Remove every locally persisted article while keeping the SQLite schema.
    pub fn clear(&self) -> Result<usize> {
        let connection = self.connection()?;
        Ok(connection.execute("DELETE FROM articles", [])?)
    }

    pub fn stats(&self) -> Result<OutboxStats> {
        self.connection()?
            .query_row(
                r#"SELECT
                    COUNT(*),
                    COALESCE(SUM(status = 'pending'), 0),
                    COALESCE(SUM(status = 'forwarded'), 0),
                    COALESCE(SUM(status = 'failed'), 0),
                    COALESCE(SUM(status = 'failed' AND retryable = 1), 0),
                    COALESCE(SUM(claimed_at IS NOT NULL), 0),
                    (SELECT COALESCE(SUM(status = 'pending'), 0) FROM delivery_intents),
                    (SELECT COALESCE(SUM(status = 'failed'), 0) FROM delivery_intents)
                FROM articles"#,
                [],
                |row| {
                    Ok(OutboxStats {
                        total: row.get(0)?,
                        pending: row.get(1)?,
                        forwarded: row.get(2)?,
                        failed: row.get(3)?,
                        retryable_failed: row.get(4)?,
                        claimed: row.get(5)?,
                        delivery_intents_pending: row.get(6)?,
                        delivery_intents_failed: row.get(7)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    /// Upsert by hash while preserving an already-forwarded state. This is the
    /// idempotency boundary: crawling the same URL twice does not redeliver it.
    pub fn save(&self, article: &Article) -> Result<DeliveryStatus> {
        let connection = self.connection()?;
        save_article(&connection, article)
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
        retry_all: bool,
        claim_ttl: Duration,
    ) -> Result<Vec<OutboxEntry>> {
        let now = Utc::now();
        let expires_before = now - claim_ttl;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let select = if source_id.is_some() {
            r#"SELECT hash FROM articles
               WHERE status IN ('pending', 'failed')
                 AND source_id = ?1 AND (?2 = 1 OR retryable = 1)
                 AND (claimed_at IS NULL OR claimed_at < ?3)
               ORDER BY created_at ASC LIMIT ?4"#
        } else {
            r#"SELECT hash FROM articles
               WHERE status IN ('pending', 'failed') AND (?1 = 1 OR retryable = 1)
                 AND (claimed_at IS NULL OR claimed_at < ?2)
               ORDER BY created_at ASC LIMIT ?3"#
        };
        let hashes = {
            let mut statement = transaction.prepare(select)?;
            if let Some(source_id) = source_id {
                statement
                    .query_map(
                        params![
                            source_id,
                            retry_all,
                            expires_before.to_rfc3339(),
                            limit as i64
                        ],
                        |row| row.get::<_, String>(0),
                    )?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            } else {
                statement
                    .query_map(
                        params![retry_all, expires_before.to_rfc3339(), limit as i64],
                        |row| row.get::<_, String>(0),
                    )?
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

    pub fn release_claim(&self, claimed_by: &str) -> Result<usize> {
        self.connection()?
            .execute(
                r#"UPDATE articles SET claimed_at = NULL, claimed_by = NULL
                   WHERE claimed_by = ?1"#,
                [claimed_by],
            )
            .map_err(Into::into)
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

            CREATE TABLE IF NOT EXISTS delivery_intents (
                run_id TEXT NOT NULL,
                article_hash TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'completed', 'failed')),
                created_at TEXT NOT NULL,
                queued_at TEXT,
                completed_at TEXT,
                PRIMARY KEY (run_id, article_hash),
                FOREIGN KEY (article_hash) REFERENCES articles(hash) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS delivery_intents_pending_idx
                ON delivery_intents(status, queued_at, created_at);
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
#[path = "../../tests/unit/articles/outbox.rs"]
mod tests;
