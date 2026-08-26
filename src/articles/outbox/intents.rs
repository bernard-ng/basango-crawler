use chrono::Utc;
use rusqlite::{TransactionBehavior, params};

use crate::{
    domain::{Article, ArticleHash, SourceId},
    error::Result,
};

use super::{
    DeliveryIntent, DeliveryStatus, Outbox, parse_sql_date, persistence::save_article,
    sql_conversion,
};

impl Outbox {
    pub fn save_with_delivery_intent(
        &self,
        article: &Article,
        intent: &DeliveryIntent,
    ) -> Result<DeliveryStatus> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let status = save_article(&transaction, article)?;
        if status != DeliveryStatus::Forwarded {
            transaction.execute(
                r#"INSERT INTO delivery_intents (
                       run_id, article_hash, agent_id, source_id, started_at,
                       status, created_at, queued_at, completed_at
                   ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, NULL, NULL)
                   ON CONFLICT(run_id, article_hash) DO NOTHING"#,
                params![
                    intent.run_id.as_str(),
                    intent.article_hash.as_str(),
                    intent.agent_id.as_str(),
                    intent.source_id.as_str(),
                    intent.started_at.to_rfc3339(),
                    Utc::now().to_rfc3339(),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(status)
    }

    pub fn pending_delivery_intents(&self, limit: usize) -> Result<Vec<DeliveryIntent>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT run_id, agent_id, source_id, article_hash, started_at
               FROM delivery_intents
               WHERE status = 'pending'
               ORDER BY created_at ASC LIMIT ?1"#,
        )?;
        statement
            .query_map([limit as i64], |row| {
                let source_id = SourceId::new(row.get::<_, String>(2)?)
                    .map_err(|error| sql_conversion(2, error))?;
                let article_hash = ArticleHash::new(row.get::<_, String>(3)?)
                    .map_err(|error| sql_conversion(3, error))?;
                Ok(DeliveryIntent {
                    run_id: row.get(0)?,
                    agent_id: row.get(1)?,
                    source_id,
                    article_hash,
                    started_at: parse_sql_date(row, 4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn mark_delivery_intent_queued(&self, run_id: &str, hash: &str) -> Result<()> {
        self.connection()?.execute(
            r#"UPDATE delivery_intents SET queued_at = ?1
               WHERE run_id = ?2 AND article_hash = ?3 AND status = 'pending'"#,
            params![Utc::now().to_rfc3339(), run_id, hash],
        )?;
        Ok(())
    }

    pub fn has_delivery_intent(&self, run_id: &str, hash: &str) -> Result<bool> {
        self.connection()?
            .query_row(
                r#"SELECT EXISTS(
                       SELECT 1 FROM delivery_intents
                       WHERE run_id = ?1 AND article_hash = ?2
                   )"#,
                params![run_id, hash],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn complete_delivery_intent(
        &self,
        run_id: &str,
        hash: &str,
        succeeded: bool,
    ) -> Result<()> {
        self.connection()?.execute(
            r#"UPDATE delivery_intents SET status = ?1, completed_at = ?2
               WHERE run_id = ?3 AND article_hash = ?4"#,
            params![
                if succeeded { "completed" } else { "failed" },
                Utc::now().to_rfc3339(),
                run_id,
                hash
            ],
        )?;
        Ok(())
    }
}
