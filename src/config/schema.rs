use serde_json::Value;
use zod_rs::prelude::*;

use crate::error::{CrawlError, Result};

pub(super) fn validate(value: &Value) -> Result<()> {
    let mut failures = Vec::new();
    if let Err(errors) = crawler_schema().safe_parse(value) {
        failures.push(errors.to_string());
    }

    if let Some(sources) = value.get("sources").and_then(Value::as_array) {
        for (index, source) in sources.iter().enumerate() {
            let result = match source.get("kind").and_then(Value::as_str) {
                Some("html") => html_source_schema().safe_parse(source),
                Some("wordpress") => wordpress_source_schema().safe_parse(source),
                _ => source_kind_schema().safe_parse(source),
            };
            if let Err(mut errors) = result {
                errors.prefix_path(index.to_string());
                errors.prefix_path("sources".into());
                failures.push(errors.to_string());
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(CrawlError::Configuration(format!(
            "schema validation failed:{}",
            failures.concat()
        )))
    }
}

fn crawler_schema() -> ObjectSchema {
    object()
        .optional_field("backend", ingestion_schema())
        .optional_field("ingestion", ingestion_schema())
        .optional_field("http", http_schema())
        .optional_field("paths", paths_schema())
        .optional_field("queue", queue_schema())
        .optional_field("runtime", runtime_schema())
        .field("sources", array(object()).min(1))
        .strict()
}

fn ingestion_schema() -> ObjectSchema {
    object()
        .optional_field("endpoint", string().url())
        .optional_field("token", string())
        .strict()
}

fn paths_schema() -> ObjectSchema {
    object()
        .optional_field("root", string())
        .optional_field("data", string())
        .optional_field("sqlite", string())
        .strict()
}

fn queue_schema() -> ObjectSchema {
    object()
        .optional_field("prefix", non_blank_string())
        .optional_field("queues", queue_names_schema())
        .optional_field("redis_url", string().regex(r"^rediss?://"))
        .optional_field("retention", retention_schema())
        .strict()
}

fn queue_names_schema() -> ObjectSchema {
    object()
        .optional_field("discovery", non_blank_string())
        .optional_field("articles", non_blank_string())
        .strict()
}

fn retention_schema() -> ObjectSchema {
    object()
        .optional_field("completed", number().int().nonnegative())
        .optional_field("failed", number().int().nonnegative())
        .strict()
}

fn http_schema() -> ObjectSchema {
    object()
        .optional_field("backoff", backoff_schema())
        .optional_field("follow_redirects", boolean())
        .optional_field("max_retries", number().int().nonnegative())
        .optional_field("respect_retry_after", boolean())
        .optional_field("rotate", boolean())
        .optional_field("timeout", number().int().positive())
        .optional_field("user_agent", non_blank_string())
        .optional_field("verify_ssl", boolean())
        .strict()
}

fn backoff_schema() -> ObjectSchema {
    object()
        .optional_field("initial", number().positive().finite())
        .optional_field("max", number().positive().finite())
        .optional_field("multiplier", number().positive().finite())
        .strict()
}

fn runtime_schema() -> ObjectSchema {
    object()
        .optional_field("direction", direction_schema())
        .optional_field("worker_concurrency", number().int().positive())
        .strict()
}

fn html_source_schema() -> ObjectSchema {
    source_base_schema("html")
        .field("pagination_template", non_blank_string())
        .field("selectors", selectors_schema())
        .optional_field("fetch_details", boolean())
        .strict()
}

fn wordpress_source_schema() -> ObjectSchema {
    source_base_schema("wordpress")
        .optional_field("metadata_strategy", metadata_strategy_schema())
        .strict()
}

fn source_kind_schema() -> ObjectSchema {
    object().field(
        "kind",
        union()
            .variant(literal("html"))
            .variant(literal("wordpress")),
    )
}

fn source_base_schema(kind: &'static str) -> ObjectSchema {
    object()
        .field("kind", literal(kind))
        .field("id", non_blank_string())
        .field("url", string().url())
        .optional_field("date_format", non_blank_string())
        .optional_field("rate_limit", boolean())
}

fn selectors_schema() -> ObjectSchema {
    object()
        .field("body", non_blank_string())
        .optional_field("categories", non_blank_string())
        .field("date", non_blank_string())
        .field("link", non_blank_string())
        .field("list", non_blank_string())
        .field("title", non_blank_string())
        .optional_field("pagination", non_blank_string())
        .strict()
}

fn direction_schema() -> UnionSchema<String> {
    union()
        .variant(literal("forward"))
        .variant(literal("backward"))
}

fn metadata_strategy_schema() -> UnionSchema<String> {
    union()
        .variant(literal("auto"))
        .variant(literal("yoast"))
        .variant(literal("rest"))
        .variant(literal("fetch"))
        .variant(literal("none"))
}

fn non_blank_string() -> StringSchema {
    string().min(1).regex(r"\S")
}
