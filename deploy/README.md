# Binary deployment

Build an optimized binary:

```bash
cargo build --release
```

Install `target/release/crawler` and `.env` under `/opt/crawler`. The default
source configuration is already embedded in the binary. Store the SQLite
outbox under `/var/lib/crawler` by setting:

```text
BASANGO_CRAWLER_SQLITE_PATH=/var/lib/crawler/crawler.db
```

Then install the three systemd unit files in this directory, reload systemd, and enable the worker and timer. The scheduler reads `BASANGO_CRAWLER_SOURCE_IDS`, allowing each machine to own a different source shard.
