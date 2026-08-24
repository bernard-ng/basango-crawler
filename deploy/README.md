# Raspberry Pi binary deployment

Run the installer on each Pi with an executable or `.tar.gz` asset URL from a GitHub release:

```bash
sudo ./deploy/install.sh https://github.com/bernard-ng/basango-rs/releases/download/v0.1.0/crawler-linux-aarch64.tar.gz
```

You can also omit the argument and answer the URL prompt, or set `BASANGO_CRAWLER_BINARY_URL` for unattended updates. The installer:

- validates the downloaded executable before replacing the installed binary;
- creates the `basango` system account and `/var/lib/crawler` state directory;
- creates `/opt/crawler/.env` only once and preserves it during updates;
- requires a unique `BASANGO_CRAWLER_AGENT_ID` for every Pi;
- installs and enables the worker service and scheduler timer;
- keeps the previous binary at `/opt/crawler/crawler.previous` when an update changes it.

Pushing a `v*` Git tag runs the release workflow, which publishes native `aarch64` (Raspberry Pi) and `x86_64` Linux archives to the GitHub release.

Each agent ID prefixes its BullMQ queue names, so multiple Pis can safely share Redis. The scheduler reads `BASANGO_CRAWLER_SOURCE_IDS`, allowing every device to own a different source shard.

To reset a Pi, stop its worker before clearing its scoped queues and SQLite outbox:

```bash
sudo systemctl stop basango-crawler-worker.service basango-crawler-schedule.timer
cd /opt/crawler
sudo -u basango ./crawler reset-agent
sudo systemctl start basango-crawler-worker.service basango-crawler-schedule.timer
```

For the one-time upgrade from unscoped queue names, add `--include-legacy-queues` after stopping all old workers. Do not use that flag once multiple agents share the Redis server.
