#!/usr/bin/env bash
set -Eeuo pipefail

INSTALL_DIR=/opt/crawler
STATE_DIR=/var/lib/crawler
SERVICE_USER=basango
SERVICE_NAME=basango-crawler-worker.service
WORKER_UNIT="/etc/systemd/system/${SERVICE_NAME}"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

confirm_removal() {
  local answer=
  printf 'This will permanently remove the Basango crawler, its configuration, and all local agent data.\n' >&2
  read -r -p 'Continue? [y/N] ' answer </dev/tty || die "cannot read confirmation from /dev/tty (use --yes for unattended removal)"
  [[ $answer == [yY] || $answer == [yY][eE][sS] ]] || {
    printf 'Uninstall cancelled.\n'
    exit 0
  }
}

remove_service_account() {
  local passwd_entry home shell
  passwd_entry=$(getent passwd "$SERVICE_USER" || true)
  if [[ -n $passwd_entry ]]; then
    IFS=: read -r _ _ _ _ _ home shell <<<"$passwd_entry"
    if [[ $home != "$STATE_DIR" || $shell != /usr/sbin/nologin ]]; then
      printf 'Keeping user %s because it does not match the account created by the installer.\n' "$SERVICE_USER" >&2
      return
    fi
    userdel "$SERVICE_USER"
  fi

  if getent group "$SERVICE_USER" >/dev/null 2>&1; then
    if ! groupdel "$SERVICE_USER"; then
      printf 'Keeping group %s because it is still in use.\n' "$SERVICE_USER" >&2
    fi
  fi
}

assume_yes=false
case ${1:-} in
  --yes|-y)
    assume_yes=true
    ;;
  '')
    ;;
  *)
    die "usage: $0 [--yes]"
    ;;
esac
[[ $# -le 1 ]] || die "usage: $0 [--yes]"

[[ ${EUID} -eq 0 ]] || die "run this uninstaller as root (for example: sudo ./deploy/uninstall.sh)"
[[ $(uname -s) == Linux ]] || die "this uninstaller supports Linux/systemd hosts"
require_command systemctl
require_command getent
require_command userdel
require_command groupdel
require_command rm

if [[ $assume_yes != true ]]; then
  confirm_removal
fi

printf 'Stopping and disabling the crawler worker...\n'
systemctl disable --now "$SERVICE_NAME" >/dev/null 2>&1 || true
rm -f -- "$WORKER_UNIT"
systemctl daemon-reload
systemctl reset-failed "$SERVICE_NAME" >/dev/null 2>&1 || true

printf 'Removing crawler files and local agent data...\n'
rm -rf -- "$INSTALL_DIR" "$STATE_DIR"

printf 'Removing the crawler service account...\n'
remove_service_account

printf 'Basango crawler uninstalled.\n'
