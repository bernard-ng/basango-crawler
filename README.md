# Basango Crawler
## Towards a scalable and intelligent system for Congolese News curation

[![Release](https://github.com/bernard-ng/basango-crawler/actions/workflows/release.yml/badge.svg)](https://github.com/bernard-ng/basango-crawler/actions/workflows/release.yml)
[![Quality](https://github.com/bernard-ng/basango-crawler/actions/workflows/quality.yml/badge.svg)](https://github.com/bernard-ng/basango-crawler/actions/workflows/quality.yml)
--- 

Rust crawler for collecting Congolese news and sending articles to the Basango API. It supports direct crawls and Redis-backed queued workers.

### Install

No repository clone is required. Choose the archive matching the machine.

Raspberry Pi 5 and ARM64:

```bash
curl -fsSL https://raw.githubusercontent.com/bernard-ng/basango-crawler/refs/heads/main/deploy/install.sh \
  | sudo bash -s -- https://github.com/bernard-ng/basango-crawler/releases/latest/download/crawler-linux-aarch64.tar.gz
```

Intel/AMD 64-bit Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/bernard-ng/basango-crawler/refs/heads/main/deploy/install.sh \
  | sudo bash -s -- https://github.com/bernard-ng/basango-crawler/releases/latest/download/crawler-linux-x86_64.tar.gz
```

The installer asks for the agent ID, API connection, and Redis URL. It installs and starts only the worker service. It does not schedule crawls.

Configuration is stored in `/opt/crawler/.env`. Every machine must have a unique `BASANGO_CRAWLER_AGENT_ID`.

### Worker

```bash
sudo systemctl status basango-crawler-worker.service
sudo journalctl -fu basango-crawler-worker.service
```

The worker starts automatically at boot. Stopping it gracefully drains active work and leaves incomplete runs open so the next worker process can resume them. Repeated Redis connection failures make the process exit, allowing systemd to restart it without closing the affected runs.

### Schedule crawls

Run scheduling manually or from cron:

```bash
cd /opt/crawler
sudo -u basango ./crawler schedule --source-id 7sur7.cd
sudo -u basango ./crawler schedule --source-id 7sur7.cd --category sport --direction backward
```

The worker runs discovery, article parsing, and API delivery concurrently. Articles are persisted to SQLite before a delivery job is queued, so a restart can safely resume unfinished delivery. Systemd is not used for scheduling.

### Direct crawl

Use direct mode for debugging or a one-time backfill:

```bash
cd /opt/crawler
sudo -u basango ./crawler crawl --source-id 7sur7.cd --category sport
sudo -u basango ./crawler crawl --source-id 7sur7.cd --category sport --direction backward
```

### Maintenance

Show the current agent's Redis queues, open runs, and SQLite outbox:

```bash
cd /opt/crawler
sudo -u basango ./crawler status
```

Audit the accounting fields for one active or recently completed run without changing Redis or queue state:

```bash
cd /opt/crawler
sudo -u basango ./crawler reconcile-run --run-id RUN_ID
```

Current trackers report discovered, processed, persisted, skipped, and delivery counts separately. Older trackers remain readable, but missing legacy fields are reported as unknown rather than inferred.

Manually retry pending article deliveries (normally the delivery queue handles these):

```bash
cd /opt/crawler
sudo -u basango ./crawler deliver --limit 100
```

Reset this agent's queues, run trackers, and SQLite database:

```bash
sudo systemctl stop basango-crawler-worker.service
cd /opt/crawler
sudo -u basango ./crawler reset-agent
sudo systemctl start basango-crawler-worker.service
```

### Local development

Requirements: Rust 1.85 or newer and Redis.

```bash
cp .env.example .env
cargo build
cargo test --all-targets
```

Source configuration lives in [`config/crawler.json`](config/crawler.json). See [`.env.example`](.env.example) for additional settings and [`deploy/README.md`](deploy/README.md) for deployment notes.

## Contributors

<a href="https://github.com/bernard-ng/basango/graphs/contributors" title="show all contributors">
  <img src="https://contrib.rocks/image?repo=bernard-ng/basango-crawler" alt="contributors"/>
</a>
