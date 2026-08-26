# Linux binary deployment

No repository clone is required. Run the current installer directly from GitHub with the release matching the machine's processor.

Raspberry Pi 5 and other ARM64 machines (`aarch64`):

```bash
curl -fsSL https://raw.githubusercontent.com/bernard-ng/basango-crawler/refs/heads/main/deploy/install.sh \
  | sudo bash -s -- https://github.com/bernard-ng/basango-crawler/releases/latest/download/crawler-linux-aarch64.tar.gz
```

Intel and AMD 64-bit machines (`x86_64`):

```bash
curl -fsSL https://raw.githubusercontent.com/bernard-ng/basango-crawler/refs/heads/main/deploy/install.sh \
  | sudo bash -s -- https://github.com/bernard-ng/basango-crawler/releases/latest/download/crawler-linux-x86_64.tar.gz
```

The installer reads first-run configuration questions from the terminal, so the interactive setup still works when the script is piped from GitHub.

From an existing local repository checkout, the equivalent command accepts the same architecture-specific release URL:

```bash
sudo ./deploy/install.sh https://github.com/bernard-ng/basango-crawler/releases/latest/download/crawler-linux-aarch64.tar.gz
```

You can also omit the argument and answer the URL prompt, or set `BASANGO_CRAWLER_BINARY_URL` for unattended updates. The installer:

- validates the downloaded executable before replacing the installed binary;
- creates the `basango` system account and `/var/lib/crawler` state directory;
- creates `/opt/crawler/.env` only once and preserves it during updates;
- requires a unique `BASANGO_CRAWLER_AGENT_ID` for every Pi;
- installs, enables, and starts only the worker service;
- keeps the previous binary at `/opt/crawler/crawler.previous` when an update changes it.

Pushing a `v*` Git tag runs the release workflow, which publishes native `aarch64` (Raspberry Pi) and `x86_64` Linux archives to the GitHub release.

Each agent ID prefixes its discovery, article, and delivery queue names, so multiple Pis can safely share Redis. The worker consumes all three concurrently and reconciles SQLite delivery records after a restart. The installer does not schedule crawls. Run `crawler schedule` yourself or configure cron later with the sources and cadence assigned to that device.

To reset a Pi, stop its worker before clearing its scoped queues and SQLite outbox:

```bash
sudo systemctl stop basango-crawler-worker.service
cd /opt/crawler
sudo -u basango ./crawler reset-agent
sudo systemctl start basango-crawler-worker.service
```
