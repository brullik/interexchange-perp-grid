#!/usr/bin/env bash
set -euo pipefail

source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
destdir="${DESTDIR:-}"
os_release_path="${IPEG_OS_RELEASE_PATH:-/etc/os-release}"

if [[ ! -r "$os_release_path" ]]; then
  echo "Ubuntu release metadata is unavailable" >&2
  exit 2
fi
# shellcheck disable=SC1090
source "$os_release_path"
if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "24.04" ]]; then
  echo "ipeg bootstrap requires Ubuntu 24.04" >&2
  exit 2
fi
if [[ -z "$destdir" && "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "ipeg bootstrap requires root" >&2
  exit 3
fi

prefix() {
  printf '%s%s' "$destdir" "$1"
}

if [[ -z "$destdir" && "${IPEG_SKIP_PACKAGE_INSTALL:-false}" != "true" ]]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y ca-certificates docker.io docker-compose-v2
fi

install -d -m 0755 "$(prefix /opt/ipeg)" "$(prefix /opt/ipeg/scripts)" \
  "$(prefix /opt/ipeg/config)" "$(prefix /etc/ipeg)" \
  "$(prefix /var/lib/ipeg)" "$(prefix /var/log/ipeg)" \
  "$(prefix /usr/local/sbin)" "$(prefix /etc/systemd/system)"
install -m 0644 "$source_root/docker-compose.yml" "$(prefix /opt/ipeg/docker-compose.yml)"
install -m 0755 "$source_root/scripts/shadow-deploy.sh" "$(prefix /opt/ipeg/scripts/)"
install -m 0755 "$source_root/scripts/shadow-upgrade.sh" "$(prefix /opt/ipeg/scripts/)"
install -m 0755 "$source_root/scripts/shadow-rollback.sh" "$(prefix /opt/ipeg/scripts/)"
install -m 0755 "$source_root/scripts/ipegctl" "$(prefix /usr/local/sbin/ipegctl)"
install -m 0644 "$source_root/deploy/ipeg.service" "$(prefix /etc/systemd/system/ipeg.service)"

if [[ -z "$destdir" ]]; then
  if ! getent group ipeg >/dev/null; then
    groupadd --system ipeg
  fi
  if ! id ipeg >/dev/null 2>&1; then
    useradd --system --gid ipeg --home-dir /var/lib/ipeg --shell /usr/sbin/nologin ipeg
  fi
  chown -R root:ipeg /opt/ipeg /var/lib/ipeg /var/log/ipeg
  chmod 0750 /var/lib/ipeg /var/log/ipeg
  systemctl daemon-reload
  systemctl enable ipeg.service
fi
echo "ipeg bootstrap complete"
