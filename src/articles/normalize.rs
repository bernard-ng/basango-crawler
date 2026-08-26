//! Validation and canonicalization at the article boundary.

use std::collections::HashSet;

use crate::{
    domain::{Article, ArticleDraft, ArticleHash},
    error::{CrawlError, Result},
};

/// Convert a permissive source draft into the canonical stored representation.
pub fn normalize(draft: ArticleDraft) -> Result<Article> {
    let title = sanitize(&draft.title);
    let body = sanitize(&draft.body);
    if title.is_empty() {
        return Err(CrawlError::InvalidArticle("title cannot be empty".into()));
    }
    if body.is_empty() {
        return Err(CrawlError::InvalidArticle("body cannot be empty".into()));
    }
    // The URL hash is an identity value, not a security boundary. It lets
    // SQLite enforce idempotency when a URL is crawled more than once.
    let hash = ArticleHash::from_url(&draft.link);

    let mut seen = HashSet::new();
    let categories = draft
        .categories
        .iter()
        .map(|category| sanitize(category))
        .filter(|category| !category.is_empty())
        .filter(|category| seen.insert(category.to_lowercase()))
        .collect();

    Ok(Article {
        hash,
        title,
        body,
        link: draft.link,
        source_id: draft.source_id,
        categories,
        metadata: draft.metadata.filter(|metadata| !metadata.is_empty()),
        published_at: draft.published_at,
    })
}

fn sanitize(text: &str) -> String {
    // These invisible Unicode characters commonly arrive in copied news text.
    // Normalizing them once protects all output adapters.
    let normalized = text
        .replace(['\u{00a0}', '\u{202f}'], " ")
        .replace(['\u{200b}', '\u{200c}', '\u{200d}', '\u{feff}'], "")
        .replace("\r\n", "\n");

    let mut result = String::with_capacity(normalized.len());
    let mut previous_newline = false;
    for character in normalized.chars() {
        if character == '\n' {
            if !previous_newline {
                result.push(character);
            }
            previous_newline = true;
        } else {
            result.push(character);
            previous_newline = false;
        }
    }
    result.trim().to_owned()
}

#[cfg(test)]
#[path = "../../tests/unit/articles/normalize.rs"]
mod tests;
