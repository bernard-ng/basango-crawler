# Basango Crawler

The Rust crawler is Basango's production collector for Congolese news. It replaces the former TypeScript crawler and runs as an independent service beside the Basango API.

It discovers articles from configured HTML and WordPress sources, normalizes them, saves them to a durable local outbox, and forwards them to the canonical ingestion API. Agent heartbeats and run signals feed Basango's realtime ingestion operations dashboard.

## Features

- **Rust-native pipeline**: typed configuration, requests, articles, errors, and queue payloads.
- **HTML and WordPress sources**: CSS-selector adapters and WordPress REST collection.
- **Direct and queued execution**: immediate crawls plus BullMQ scheduling and concurrent workers.
- **Durable delivery**: SQLite outbox with atomic claims, retryable failures, and idempotency keys.
- **Resilient HTTP**: timeouts, exponential backoff, jitter, `Retry-After`, redirects, and user-agent rotation.
- **Flexible collection windows**: source, page, date, category, and forward/backward update filters.
- **Operational visibility**: idempotent heartbeats and lifecycle signals for Basango's ingestion dashboard.
- **Graceful workers**: shared concurrency limits, BullMQ stalled-job recovery, and clean shutdown.

## Architecture

```text
HTML / WordPress sources
          │
          ▼
 discovery + collection
          │
          ▼
 normalize and validate
          │
          ▼
    SQLite outbox ───────POST /ingest/articles──────► Basango API
          │                                              │
          └──────────── retry until delivered ────────────┘

 ingestion signals ─────POST /ingest/signals───────► operations read model
                                                        │
 dashboard ◄── tRPC snapshot + authenticated SSE ───────┘
```

Direct crawls stream article drafts through the pipeline in one process. Queued execution separates discovery and article collection with BullMQ:

```text
schedule → discovery queue → worker → article queue → worker → outbox → API
```

The crawler owns collection, retries, queues, and its local durability boundary. The Basango API owns canonical article storage, signal projection, and persistent operational history. The dashboard reads the projection; it does not interpret crawler messages itself.

## Prerequisites

- Rust 1.85 or newer
- Redis for `schedule` and `worker`
- A Basango API endpoint and crawler token for article delivery and dashboard events

Redis and the API are optional for direct offline collection. Without an API endpoint, articles remain pending in SQLite until `deliver` is run later.

## Installation

```bash
git clone https://github.com/bernard-ng/basango-rs.git
cd basango-rs
cp .env.example .env
cargo build --release
```

The release binary is written to `target/release/crawler`.

## Configuration

Configuration is loaded in this order:

```text
bundled config/crawler.json < optional external JSON file < environment variables
```

The bundled JSON contains the source catalog and default HTTP, queue, runtime, and storage settings. Use `--config` or `BASANGO_CRAWLER_CONFIG_PATH` to load a different file.

Configuration is organized by capability under `src/config/`. `zod-rs` validates the raw JSON structure with strict, composable schemas and path-aware errors before Serde creates the Rust types. The same schema validates programmatically constructed configurations after environment overrides. A separate semantic validator only owns relationships that cannot be expressed as field schemas, such as distinct queue names, conditional ingestion credentials, backoff ordering, and unique source IDs.

### Ingestion API and monitoring

```bash
# Base URL of the Basango API, without an endpoint suffix
BASANGO_API_CRAWLER_ENDPOINT=https://api.basango.example
BASANGO_API_CRAWLER_TOKEN=replace-with-the-api-crawler-token

# Required stable identity; use a unique value on every Raspberry Pi
BASANGO_CRAWLER_AGENT_ID=crawler-lubumbashi-01
```

With the API configured, the crawler sends:

- articles to `POST /ingest/articles`;
- lifecycle signals and heartbeats to `POST /ingest/signals`;
- update-window lookups to `POST /ingest/sources/publication-bounds`.

`BASANGO_CRAWLER_AGENT_ID` is mandatory. Crawler startup fails when it is absent, preventing two devices from silently sharing an identity. Article collection continues if signal reporting is temporarily unavailable, and failed article delivery remains durable in the SQLite outbox.

### Signal protocol

The crawler emits a small discriminated protocol instead of loosely shaped event payloads:

| Signal | Meaning |
|---|---|
| `agent.heartbeat` | The worker process is reachable |
| `agent.reset` | This agent's queues and local outbox were intentionally cleared |
| `run.preparing` | A direct source run is resolving its inputs |
| `run.started` | Collection has started |
| `run.progress` | Absolute discovered, persisted, delivered, and failed totals |
| `run.completed` | The run completed with final totals and duration |
| `run.failed` | The run stopped with final totals, duration, and an error |

Every message has a UUID `signalId`, stable `agentId`, and `emittedAt` timestamp. Run messages also carry `runId` and `sourceId`. The API deduplicates by signal ID and projects absolute totals, so retries cannot double-count work.

### Storage and HTTP

```bash
BASANGO_CRAWLER_DATA_PATH=data
BASANGO_CRAWLER_SQLITE_PATH=data/crawler.db
BASANGO_CRAWLER_FETCH_USER_AGENT="Basango/0.1 (+https://basango.ngandu.dev)"
BASANGO_CRAWLER_FETCH_MAX_RETRIES=3
BASANGO_CRAWLER_FETCH_RESPECT_RETRY_AFTER=true
BASANGO_CRAWLER_UPDATE_DIRECTION=forward
```

### Queues

```bash
BASANGO_CRAWLER_REDIS_URL=redis://localhost:6379/0
BASANGO_CRAWLER_QUEUE_DISCOVERY=discovery
BASANGO_CRAWLER_QUEUE_ARTICLES=articles
BASANGO_CRAWLER_RETAIN_COMPLETED=3600
BASANGO_CRAWLER_RETAIN_FAILED=86400
```

The effective BullMQ queue names are automatically prefixed with the required agent ID. For example, `basango-pi-01` uses `basango-pi-01-discovery` and `basango-pi-01-articles`. This lets 5–10 Raspberry Pis safely share one Redis server while each worker consumes only its own jobs. Keep the configured queue values as the short suffixes shown above.

See [`.env.example`](.env.example) and [`config/crawler.json`](config/crawler.json) for all supported values and source examples.

## Usage

### Direct crawling

Use direct mode for one source, backfills, debugging, and one-off collection:

```bash
cargo run -- crawl --source-id radiookapi.net
cargo run -- crawl --source-id radiookapi.net --page-range 1:5
cargo run -- crawl --source-id radiookapi.net --date-range 2025-01-01:2025-01-31
cargo run -- crawl --source-id 7sur7.cd --category politique
cargo run -- crawl --source-id 7sur7.cd --category politique --direction backward
```

### Queued crawling

Schedule one or more sources in BullMQ:

```bash
cargo run -- schedule --source-id radiookapi.net
cargo run -- schedule --source-id radiookapi.net --source-id 7sur7.cd
```

Start workers for both queues:

```bash
cargo run -- worker
cargo run -- worker --concurrency 5
cargo run -- worker --queue discovery --queue articles --concurrency 5
```

Workers publish an agent heartbeat every 15 seconds. Stop them gracefully with `Ctrl-C`.

### Resetting one agent

Stop the worker first, then clear only the current agent's queues, queued-run trackers, and SQLite outbox:

```bash
cargo run -- reset-agent
```

The command derives its scope from `BASANGO_CRAWLER_AGENT_ID`; it does not touch another Raspberry Pi's queues or the canonical articles stored by the Basango API.

When upgrading the original single-agent deployment, the old queues were not scoped. After stopping every old worker, remove them once with `cargo run -- reset-agent --include-legacy-queues`. Never use that flag after multiple agents begin sharing Redis.

### Delivering the outbox

Retry pending or failed API deliveries:

```bash
cargo run -- deliver --limit 100
cargo run -- deliver --source-id radiookapi.net --limit 50
cargo run -- deliver --retry-all --limit 100
```

Use `--retry-all` after fixing a client-side payload or authentication problem
to retry failures that were previously classified as non-retryable.

### External configuration

Every command accepts a configuration override:

```bash
cargo run -- --config /path/to/crawler.json crawl --source-id radiookapi.net
```

## CLI reference

| Command | Purpose |
|---|---|
| `crawl` (`sync`) | Collect one source immediately |
| `schedule` | Enqueue one or more source discovery jobs |
| `worker` | Process discovery and article queues |
| `deliver` (`push`) | Retry durable outbox deliveries |
| `reset-agent` (`reset`) | Empty this agent's queues, run trackers, and SQLite outbox |
| `version` | Print the crawler version |

Common crawl flags:

| Option | Description | Example |
|---|---|---|
| `--source-id` | Source identifier from the active configuration | `--source-id radiookapi.net` |
| `--page-range` | Inclusive page range | `--page-range 1:5` |
| `--date-range` | Inclusive UTC date window | `--date-range 2025-01-01:2025-01-31` |
| `--category` | Configured source category | `--category politique` |
| `--direction` | Override update direction for this crawl/job | `--direction backward` |

## Realtime ingestion operations

Run the Basango database migration and open **Ingestion** in the Basango dashboard. The operations view shows:

- online and offline crawler agents;
- active and recent direct crawl runs;
- discovered, persisted, delivered, and failed article counts;
- failures and their latest errors;
- a live lifecycle activity feed.

The dashboard receives lightweight server-sent invalidations and reloads the durable tRPC snapshot. Periodic polling covers reconnects and multi-instance deployments. An agent is considered offline after 45 seconds without a heartbeat or lifecycle signal.

## Deployment

An idempotent Raspberry Pi installer and systemd units are in [`deploy/`](deploy/). Run the installer as root and provide an executable or `.tar.gz` asset URL from a GitHub release:

```bash
sudo ./deploy/install.sh https://github.com/bernard-ng/basango-rs/releases/download/v0.1.0/crawler-linux-aarch64.tar.gz
```

On the first run it creates the service account, persistent directories, mandatory agent configuration, worker service, and scheduler timer. Later runs preserve `.env`, replace the binary after validating it, refresh the units, and restart the services.

Pushing a `v*` Git tag publishes native Linux archives for Raspberry Pi `aarch64` and `x86_64`; the resulting release asset URL can be passed directly to the installer.

Build and validate before deployment:

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

## Migration from the TypeScript crawler

The TypeScript implementation has been removed from `basango/apps/crawler`. Operational ownership now lives here:

- source configuration: `config/crawler.json`;
- crawler processes and scheduling: this binary and `deploy/` units;
- ingestion and signal contracts: `@basango/domain` in the Basango monorepo;
- signal projection and operations queries: `@basango/db` and the Basango API;
- run visibility: the Basango dashboard's **Ingestion** page.

The former TypeScript event names and `/crawler/events` route are intentionally not compatibility aliases. Rust and the API now share the `agent.*` / `run.*` vocabulary above.
