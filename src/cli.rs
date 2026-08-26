//! Command-line interface and process-level wiring.
//!
//! Clap parses untrusted strings at the outermost boundary. Commands then use
//! typed ranges and options, so deeper modules do not repeatedly validate the
//! same input.

use std::{env, path::PathBuf};

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::{
    Crawler, CrawlerStatus,
    domain::{CrawlRequest, DateRange, PageRange, SourceId, UpdateDirection},
    error::CrawlError,
};

#[derive(Debug, Parser)]
#[command(
    name = "crawler",
    version,
    about = "Collect Congolese news from HTML and WordPress sources"
)]
struct Cli {
    /// Override the bundled JSON configuration file.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Crawl one source immediately in this process.
    #[command(alias = "sync")]
    Crawl(CrawlArgs),
    /// Place one or more source discovery jobs in BullMQ.
    Schedule(ScheduleArgs),
    /// Process BullMQ discovery, article, and delivery jobs until interrupted.
    Worker(WorkerArgs),
    /// Deliver pending or failed articles from the SQLite outbox.
    #[command(alias = "push")]
    Deliver(DeliverArgs),
    /// Show this agent's Redis queues, open runs, and SQLite outbox state.
    Status,
    /// Clear this agent's queues, run trackers, and SQLite outbox.
    #[command(alias = "reset")]
    ResetAgent,
    /// Print version information (also available as --version).
    Version,
}

#[derive(Debug, Args)]
struct CrawlArgs {
    /// Source identifier from the active configuration.
    #[arg(long)]
    source_id: SourceId,
    /// Inclusive page range in start:end form, for example 1:5.
    #[arg(long, value_parser = parse_page_range)]
    page_range: Option<PageRange>,
    /// Inclusive UTC date range, for example 2025-01-01:2025-01-31.
    #[arg(long, value_parser = parse_date_range)]
    date_range: Option<DateRange>,
    /// Optional configured category slug.
    #[arg(long)]
    category: Option<String>,
    /// Override the configured update direction for this crawl.
    #[arg(long, value_parser = parse_update_direction)]
    direction: Option<UpdateDirection>,
}

#[derive(Debug, Args)]
struct ScheduleArgs {
    /// Repeat the flag or pass comma-separated IDs.
    #[arg(long = "source-id", value_delimiter = ',')]
    source_ids: Vec<SourceId>,
    /// Inclusive page range in start:end form, for example 1:5.
    #[arg(long, value_parser = parse_page_range)]
    page_range: Option<PageRange>,
    /// Inclusive UTC date range, for example 2025-01-01:2025-01-31.
    #[arg(long, value_parser = parse_date_range)]
    date_range: Option<DateRange>,
    /// Optional configured category slug.
    #[arg(long)]
    category: Option<String>,
    /// Persist the update direction in each scheduled discovery job.
    #[arg(long, value_parser = parse_update_direction)]
    direction: Option<UpdateDirection>,
}

#[derive(Debug, Args)]
struct WorkerArgs {
    /// Queue suffix to process; repeat to select stages explicitly.
    #[arg(long, short = 'q')]
    queue: Vec<String>,
    /// Maximum number of jobs processed concurrently.
    #[arg(long)]
    concurrency: Option<usize>,
}

#[derive(Debug, Args)]
struct DeliverArgs {
    /// Only claim articles collected from this source.
    #[arg(long)]
    source_id: Option<SourceId>,
    /// Maximum number of outbox rows to claim.
    #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
    limit: usize,
    /// Retry failures previously classified as non-retryable (for example after fixing a payload).
    #[arg(long)]
    retry_all: bool,
}

pub async fn run() -> anyhow::Result<()> {
    initialize_logging();
    let cli = Cli::parse();

    if matches!(cli.command, Command::Version) {
        println!("crawler {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let crawler = match cli.config {
        Some(path) => Crawler::from_config_file(path),
        None => Crawler::from_environment(),
    }
    .context("could not initialize crawler")?;

    match cli.command {
        Command::Crawl(arguments) => {
            let report = crawler.crawl(arguments.into()).await?;
            tracing::info!(?report, "crawl completed");
        }
        Command::Schedule(arguments) => {
            schedule(&crawler, arguments).await?;
        }
        Command::Worker(arguments) => {
            let concurrency = arguments
                .concurrency
                .unwrap_or(crawler.config().runtime.worker_concurrency);
            crawler.work(arguments.queue, concurrency).await?;
        }
        Command::Deliver(arguments) => {
            let report = crawler
                .deliver_pending(
                    arguments.source_id.as_ref(),
                    arguments.limit,
                    arguments.retry_all,
                )
                .await?;
            tracing::info!(?report, "outbox delivery completed");
            if report.failed > 0 {
                bail!("failed to deliver {} article(s)", report.failed);
            }
        }
        Command::Status => print_status(&crawler.status().await),
        Command::ResetAgent => {
            let report = crawler.reset_agent().await?;
            tracing::info!(?report, "agent state reset");
        }
        Command::Version => unreachable!("handled before configuration loading"),
    }
    Ok(())
}

fn print_status(status: &CrawlerStatus) {
    println!("Crawler status");
    println!("  Agent:  {}", status.agent_id);
    println!();
    println!("SQLite");
    println!("  Path:   {}", status.sqlite_path.display());
    match &status.outbox {
        Ok(outbox) => {
            println!("  State:  available");
            println!(
                "  Rows:   {} total | {} pending | {} forwarded | {} failed | {} claimed",
                outbox.total, outbox.pending, outbox.forwarded, outbox.failed, outbox.claimed
            );
            println!("  Retryable failures: {}", outbox.retryable_failed);
            println!(
                "  Delivery intents: {} pending | {} failed",
                outbox.delivery_intents_pending, outbox.delivery_intents_failed
            );
        }
        Err(error) => println!("  State:  unavailable ({error})"),
    }

    println!();
    println!("Redis");
    match &status.redis {
        Ok(redis) => {
            println!("  State:  connected");
            for queue in &redis.queues {
                println!("  {}", queue.name);
                println!(
                    "    workers {} | waiting {} | active {} | delayed {} | failed {} | completed {}",
                    queue.workers,
                    queue.waiting + queue.paused,
                    queue.active,
                    queue.delayed,
                    queue.failed,
                    queue.completed
                );
                if queue.prioritized > 0 || queue.waiting_children > 0 {
                    println!(
                        "    prioritized {} | waiting for children {}",
                        queue.prioritized, queue.waiting_children
                    );
                }
            }

            println!("  Open runs: {}", redis.open_runs.len());
            for run in &redis.open_runs {
                println!(
                    "    {} | {} | started {}",
                    run.source_id,
                    run.run_id,
                    run.started_at.to_rfc3339()
                );
                println!(
                    "      discovered {} | processed {} | persisted {} | delivered {} | failed {}",
                    run.discovered, run.processed, run.persisted, run.delivered, run.failed
                );
                println!(
                    "      delivery jobs {} expected | {} processed",
                    run.deliveries_expected, run.deliveries_processed
                );
            }
        }
        Err(error) => println!("  State:  unavailable ({error})"),
    }
}

async fn schedule(crawler: &Crawler, arguments: ScheduleArgs) -> anyhow::Result<()> {
    let source_ids = if arguments.source_ids.is_empty() {
        env::var("BASANGO_CRAWLER_SOURCE_IDS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::parse)
            .collect::<Result<Vec<SourceId>, _>>()?
    } else {
        arguments.source_ids
    };
    if source_ids.is_empty() {
        bail!("pass --source-id or set BASANGO_CRAWLER_SOURCE_IDS");
    }

    for source_id in source_ids {
        let id = crawler
            .schedule(CrawlRequest {
                source_id: source_id.clone(),
                page_range: arguments.page_range,
                date_range: arguments.date_range,
                category: arguments.category.clone(),
                direction: arguments.direction,
            })
            .await?;
        tracing::info!(job_id = id, %source_id, "scheduled source discovery");
    }
    Ok(())
}

impl From<CrawlArgs> for CrawlRequest {
    fn from(value: CrawlArgs) -> Self {
        Self {
            source_id: value.source_id,
            page_range: value.page_range,
            date_range: value.date_range,
            category: value.category,
            direction: value.direction,
        }
    }
}

fn parse_page_range(value: &str) -> Result<PageRange, String> {
    PageRange::parse(value).map_err(|error| error.to_string())
}

fn parse_date_range(value: &str) -> Result<DateRange, String> {
    DateRange::parse(value).map_err(|error| error.to_string())
}

fn parse_update_direction(value: &str) -> Result<UpdateDirection, String> {
    value.parse().map_err(|error: CrawlError| error.to_string())
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("'{value}' is not a positive integer"))?;
    if parsed == 0 {
        return Err("value must be at least 1".into());
    }
    Ok(parsed)
}

fn initialize_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Tests or embedding applications may already have a subscriber. `try_init`
    // avoids panicking when global logging was initialized elsewhere.
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[cfg(test)]
#[path = "../tests/unit/cli.rs"]
mod tests;
