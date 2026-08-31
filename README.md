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

To completely uninstall an agent, including its configuration and local SQLite data:

```bash
curl -fsSL https://raw.githubusercontent.com/bernard-ng/basango-crawler/refs/heads/main/deploy/uninstall.sh \
  | sudo bash
```

The uninstaller asks for confirmation before removing the worker service, crawler files, local state, and installer-created system account. Pass `--yes` when running it unattended.

### Worker

```bash
sudo systemctl status basango-crawler-worker.service
sudo journalctl -fu basango-crawler-worker.service
```

The worker starts automatically at boot. Stopping it gracefully drains active work and leaves incomplete runs open so the next worker process can resume them. Repeated Redis connection failures make the process exit, allowing systemd to restart it without closing the affected runs.

### Schedule crawls (Initial)

Schedule every registered source that does not require an indexed category:

```bash
cd /opt/crawler
sudo -u basango ./crawler schedule --source-id radiookapi.net --direction=backward
sudo -u basango ./crawler schedule --source-id mediacongo.net --direction=backward
sudo -u basango ./crawler schedule --source-id beto.cd --direction=backward
sudo -u basango ./crawler schedule --source-id newscd.net --direction=backward
sudo -u basango ./crawler schedule --source-id b-onetv.cd --direction=backward
sudo -u basango ./crawler schedule --source-id bukavufm.com --direction=backward
sudo -u basango ./crawler schedule --source-id changement7.net --direction=backward
sudo -u basango ./crawler schedule --source-id congoactu.net --direction=backward
sudo -u basango ./crawler schedule --source-id congoindependant.com --direction=backward
sudo -u basango ./crawler schedule --source-id congoquotidien.com --direction=backward
sudo -u basango ./crawler schedule --source-id cumulard.cd --direction=backward
sudo -u basango ./crawler schedule --source-id environews-rdc.net --direction=backward
sudo -u basango ./crawler schedule --source-id freemediardc.info --direction=backward
sudo -u basango ./crawler schedule --source-id geopolismagazine.org --direction=backward
sudo -u basango ./crawler schedule --source-id habarirdc.net --direction=backward
sudo -u basango ./crawler schedule --source-id kilalopress.net --direction=backward
sudo -u basango ./crawler schedule --source-id laprunellerdc.cd --direction=backward
sudo -u basango ./crawler schedule --source-id lesmedias.net --direction=backward
sudo -u basango ./crawler schedule --source-id lesvolcansnews.net --direction=backward
sudo -u basango ./crawler schedule --source-id netic-news.net --direction=backward
sudo -u basango ./crawler schedule --source-id scooprdc.net --direction=backward
sudo -u basango ./crawler schedule --source-id journaldekinshasa.com --direction=backward
sudo -u basango ./crawler schedule --source-id lepotentiel.cd --direction=backward
sudo -u basango ./crawler schedule --source-id acturdc.com --direction=backward
sudo -u basango ./crawler schedule --source-id matininfos.net --direction=backward
```

The four sources with indexed categories require one job per category. Schedule them in batches:

```bash
cd /opt/crawler

for category in politique economie sante culture sport societe une; do
  sudo -u basango ./crawler schedule --source-id 7sur7.cd --category "$category"
done

for category in actualite/politique actualite/securite actualite/economie actualite/societe sport culture femme justice santé afrique; do
  sudo -u basango ./crawler schedule --source-id actualite.cd --category "$category"
done

for category in actu afrique android breves communication covid19 culture editions edito education environnement factcheck featured featured-2 hitech health interview justice medias nation non-classe politique portrait presidentielle security societe sondage sport spec-elect world ecofin tribune; do
  sudo -u basango ./crawler schedule --source-id africanewsrdc.net --category "$category"
done

for category in actualites politique securite societe culture environnement economie; do
  sudo -u basango ./crawler schedule --source-id infordc.com --category "$category"
done
```

The worker runs discovery, article parsing, and API delivery concurrently. Articles are persisted to SQLite before a delivery job is queued, so a restart can safely resume unfinished delivery. Systemd is not used for scheduling.

### Direct crawl (Updates)

Use direct mode for debugging or a one-time backfill. Crawl every registered source that does not require an indexed category:

```bash
cd /opt/crawler
sudo -u basango ./crawler crawl --source-id radiookapi.net
sudo -u basango ./crawler crawl --source-id mediacongo.net
sudo -u basango ./crawler crawl --source-id beto.cd
sudo -u basango ./crawler crawl --source-id newscd.net
sudo -u basango ./crawler crawl --source-id angazainstitute.ac.cd
sudo -u basango ./crawler crawl --source-id b-onetv.cd
sudo -u basango ./crawler crawl --source-id bukavufm.com
sudo -u basango ./crawler crawl --source-id changement7.net
sudo -u basango ./crawler crawl --source-id congoactu.net
sudo -u basango ./crawler crawl --source-id congoindependant.com
sudo -u basango ./crawler crawl --source-id congoquotidien.com
sudo -u basango ./crawler crawl --source-id cumulard.cd
sudo -u basango ./crawler crawl --source-id environews-rdc.net
sudo -u basango ./crawler crawl --source-id freemediardc.info
sudo -u basango ./crawler crawl --source-id geopolismagazine.org
sudo -u basango ./crawler crawl --source-id habarirdc.net
sudo -u basango ./crawler crawl --source-id kilalopress.net
sudo -u basango ./crawler crawl --source-id laprunellerdc.cd
sudo -u basango ./crawler crawl --source-id lesmedias.net
sudo -u basango ./crawler crawl --source-id lesvolcansnews.net
sudo -u basango ./crawler crawl --source-id netic-news.net
sudo -u basango ./crawler crawl --source-id scooprdc.net
sudo -u basango ./crawler crawl --source-id journaldekinshasa.com
sudo -u basango ./crawler crawl --source-id lepotentiel.cd
sudo -u basango ./crawler crawl --source-id acturdc.com
sudo -u basango ./crawler crawl --source-id matininfos.net
```

Crawl all indexed categories in batches:

```bash
cd /opt/crawler

for category in politique economie sante culture sport societe une; do
  sudo -u basango ./crawler crawl --source-id 7sur7.cd --category "$category"
done

for category in actualite/politique actualite/securite actualite/economie actualite/societe sport culture femme justice santé afrique; do
  sudo -u basango ./crawler crawl --source-id actualite.cd --category "$category"
done

for category in actu afrique android breves communication covid19 culture editions edito education environnement factcheck featured featured-2 hitech health interview justice medias nation non-classe politique portrait presidentielle security societe sondage sport spec-elect world ecofin tribune; do
  sudo -u basango ./crawler crawl --source-id africanewsrdc.net --category "$category"
done

for category in actualites politique securite societe culture environnement economie; do
  sudo -u basango ./crawler crawl --source-id infordc.com --category "$category"
done
```

### Direct crawl cron

Create a script that runs a direct crawl for every registered source. Sources with indexed categories are crawled once per category:

```bash
sudo tee /opt/crawler/crawl-all.sh >/dev/null <<'EOF'
#!/bin/sh
set -u

cd /opt/crawler || exit 1
result=0

for source in \
  radiookapi.net \
  mediacongo.net \
  beto.cd \
  newscd.net \
  angazainstitute.ac.cd \
  b-onetv.cd \
  bukavufm.com \
  changement7.net \
  congoactu.net \
  congoindependant.com \
  congoquotidien.com \
  cumulard.cd \
  environews-rdc.net \
  freemediardc.info \
  geopolismagazine.org \
  habarirdc.net \
  kilalopress.net \
  laprunellerdc.cd \
  lesmedias.net \
  lesvolcansnews.net \
  netic-news.net \
  scooprdc.net \
  journaldekinshasa.com \
  lepotentiel.cd \
  acturdc.com \
  matininfos.net
do
  ./crawler crawl --source-id "$source" || result=1
done

crawl_categories() {
  source=$1
  shift
  for category in "$@"; do
    ./crawler crawl --source-id "$source" --category "$category" || result=1
  done
}

crawl_categories 7sur7.cd \
  politique economie sante culture sport societe une
crawl_categories actualite.cd \
  actualite/politique actualite/securite actualite/economie actualite/societe \
  sport culture femme justice santé afrique
crawl_categories africanewsrdc.net \
  actu afrique android breves communication covid19 culture editions edito \
  education environnement factcheck featured featured-2 hitech health interview \
  justice medias nation non-classe politique portrait presidentielle security \
  societe sondage sport spec-elect world ecofin tribune
crawl_categories infordc.com \
  actualites politique securite societe culture environnement economie

exit "$result"
EOF

sudo chown root:basango /opt/crawler/crawl-all.sh
sudo chmod 0750 /opt/crawler/crawl-all.sh
```

Register the script in system cron to run every day at 00:00, 06:00, 12:00, and 18:00. The lock prevents a new direct crawl from starting if the previous batch is still running:

```bash
sudo tee /etc/cron.d/basango-crawler-direct-crawl >/dev/null <<'EOF'
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

0 0,6,12,18 * * * basango flock -n /var/lib/crawler/direct-crawl.lock /opt/crawler/crawl-all.sh
EOF

sudo chmod 0644 /etc/cron.d/basango-crawler-direct-crawl
```

Cron uses the server's local timezone.

### Maintenance

Show the current agent's Redis queues, open runs, and SQLite outbox:

```bash
cd /opt/crawler
sudo -u basango ./crawler status
```

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

## Contributors

<a href="https://github.com/bernard-ng/basango/graphs/contributors" title="show all contributors">
  <img src="https://contrib.rocks/image?repo=bernard-ng/basango-crawler" alt="contributors"/>
</a>
