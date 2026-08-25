#!/usr/bin/env bash
set -Eeuo pipefail

INSTALL_DIR=/opt/crawler
STATE_DIR=/var/lib/crawler
SERVICE_USER=basango
ENV_FILE="${INSTALL_DIR}/.env"
BINARY_PATH="${INSTALL_DIR}/crawler"
WORKER_UNIT=/etc/systemd/system/basango-crawler-worker.service

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

prompt_required() {
  local prompt=$1
  local value=
  while [[ -z ${value//[[:space:]]/} ]]; do
    read -r -p "$prompt" value </dev/tty || die "cannot read installer input from /dev/tty"
  done
  printf '%s' "$value"
}

prompt_secret() {
  local prompt=$1
  local value=
  while [[ -z $value ]]; do
    read -r -s -p "$prompt" value </dev/tty || die "cannot read installer input from /dev/tty"
    printf '\n' >&2
  done
  printf '%s' "$value"
}

dotenv_value() {
  local value=$1
  [[ $value != *$'\n'* && $value != *$'\r'* ]] || die "configuration values cannot contain newlines"
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//\$/\\\$}
  printf '"%s"' "$value"
}

write_initial_config() {
  local agent_id api_endpoint api_token redis_url
  agent_id=$(prompt_required 'Unique agent ID (example: basango-pi-01): ')
  read -r -p 'Basango API base URL: ' api_endpoint </dev/tty || die "cannot read installer input from /dev/tty"
  if [[ -n $api_endpoint ]]; then
    api_token=$(prompt_secret 'Basango crawler API token: ')
  else
    api_token=
  fi
  read -r -p 'Redis URL [redis://localhost:6379/0]: ' redis_url </dev/tty || die "cannot read installer input from /dev/tty"
  redis_url=${redis_url:-redis://localhost:6379/0}

  umask 027
  {
    printf 'BASANGO_CRAWLER_AGENT_ID=%s\n' "$(dotenv_value "$agent_id")"
    printf 'BASANGO_API_CRAWLER_ENDPOINT=%s\n' "$(dotenv_value "$api_endpoint")"
    printf 'BASANGO_API_CRAWLER_TOKEN=%s\n' "$(dotenv_value "$api_token")"
    printf 'BASANGO_CRAWLER_REDIS_URL=%s\n' "$(dotenv_value "$redis_url")"
    printf 'BASANGO_CRAWLER_SQLITE_PATH=%s\n' "$(dotenv_value "${STATE_DIR}/crawler.db")"
    printf 'RUST_LOG=info\n'
  } >"$ENV_FILE"
}

ensure_agent_id() {
  if grep -Eq "^[[:space:]]*BASANGO_CRAWLER_AGENT_ID[[:space:]]*=[[:space:]]*(\"[^\"]+\"|'[^']+'|[^\"'[:space:]][^[:space:]]*)[[:space:]]*$" "$ENV_FILE"; then
    return
  fi
  local agent_id
  agent_id=$(prompt_required 'Unique agent ID (missing from existing config): ')
  printf 'BASANGO_CRAWLER_AGENT_ID=%s\n' "$(dotenv_value "$agent_id")" >>"$ENV_FILE"
}

write_worker_unit() {
  local unit_tmp
  unit_tmp=$(mktemp -d)

  cat >"${unit_tmp}/basango-crawler-worker.service" <<'UNIT'
[Unit]
Description=Basango crawler worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=basango
Group=basango
WorkingDirectory=/opt/crawler
EnvironmentFile=/opt/crawler/.env
ExecStart=/opt/crawler/crawler worker
Restart=always
RestartSec=10
KillSignal=SIGINT
TimeoutStopSec=45
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/var/lib/crawler
StateDirectory=crawler

[Install]
WantedBy=multi-user.target
UNIT

  install -m 0644 "${unit_tmp}/basango-crawler-worker.service" "$WORKER_UNIT"
  rm -rf "$unit_tmp"
}

[[ ${EUID} -eq 0 ]] || die "run this installer as root (for example: sudo ./deploy/install.sh)"
[[ $(uname -s) == Linux ]] || die "this installer supports Linux/systemd hosts"
require_command curl
require_command systemctl
require_command install
require_command useradd
require_command groupadd

binary_url=${BASANGO_CRAWLER_BINARY_URL:-${1:-}}
if [[ -z $binary_url ]]; then
  binary_url=$(prompt_required 'GitHub release binary URL: ')
fi
[[ $binary_url == https://github.com/* ]] || die "binary URL must be an HTTPS github.com release URL"

download_dir=$(mktemp -d)
downloaded_asset="${download_dir}/release-asset"
candidate_binary=$downloaded_asset
trap 'rm -rf "$download_dir"' EXIT
printf 'Downloading crawler release...\n'
curl --fail --location --show-error --silent --retry 3 --output "$downloaded_asset" "$binary_url"
chmod 0755 "$candidate_binary"
if ! "$candidate_binary" --version >/dev/null 2>&1; then
  require_command tar
  candidate_binary="${download_dir}/crawler"
  tar -xOzf "$downloaded_asset" crawler >"$candidate_binary" 2>/dev/null || die "release asset is neither a crawler executable nor a .tar.gz containing crawler"
  chmod 0755 "$candidate_binary"
  "$candidate_binary" --version >/dev/null 2>&1 || die "crawler release is not executable on this host; verify the Raspberry Pi architecture"
fi

if ! getent group "$SERVICE_USER" >/dev/null 2>&1; then
  groupadd --system "$SERVICE_USER"
fi
if ! getent passwd "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_USER" --home-dir "$STATE_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

install -d -m 0755 -o root -g root "$INSTALL_DIR"
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$STATE_DIR"

if [[ ! -f $ENV_FILE ]]; then
  write_initial_config
else
  ensure_agent_id
fi
chown root:"$SERVICE_USER" "$ENV_FILE"
chmod 0640 "$ENV_FILE"

systemctl stop basango-crawler-worker.service 2>/dev/null || true
backup_created=false
if [[ -f $BINARY_PATH ]] && ! cmp -s "$candidate_binary" "$BINARY_PATH"; then
  cp -p "$BINARY_PATH" "${BINARY_PATH}.previous"
  backup_created=true
fi
install -m 0755 -o root -g root "$candidate_binary" "${BINARY_PATH}.new"
mv -f "${BINARY_PATH}.new" "$BINARY_PATH"

write_worker_unit
systemctl daemon-reload
systemctl enable basango-crawler-worker.service >/dev/null
if ! systemctl restart basango-crawler-worker.service; then
  if [[ $backup_created == true && -f ${BINARY_PATH}.previous ]]; then
    printf 'Worker failed to start; restoring the previous binary.\n' >&2
    install -m 0755 -o root -g root "${BINARY_PATH}.previous" "$BINARY_PATH"
    systemctl restart basango-crawler-worker.service || true
  fi
  die "crawler worker failed to start; inspect it with systemctl status basango-crawler-worker.service"
fi

printf 'Installed %s to %s\n' "$("$BINARY_PATH" --version)" "$BINARY_PATH"
printf 'Agent configuration: %s\n' "$ENV_FILE"
printf 'Worker status: systemctl status basango-crawler-worker.service\n'
