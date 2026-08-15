# Deployment и эксплуатация

## Нормативная среда

- Ubuntu 24.04 LTS;
- Germany VPS по умолчанию;
- Docker Engine + Compose plugin;
- systemd unit `ipeg.service`;
- user `ipeg`, no login shell optional, member docker only when необходим;
- root используется только bootstrap/upgrade system files;
- application container non-root, read-only root filesystem, all capabilities dropped;
- named volumes for state/data/logs;
- `/etc/ipeg/ipeg.env` mode 0600.

## `ipegctl`

Обязательные idempotent команды:

```text
ipegctl bootstrap
ipegctl deploy --image <digest>
ipegctl doctor
ipegctl status
ipegctl start-shadow
ipegctl qualification-start
ipegctl qualification-status
ipegctl qualification-finalize
ipegctl owner-onboard
ipegctl canary-arm
ipegctl canary-status
ipegctl emergency-flatten
ipegctl update --image <digest>
ipegctl rollback
ipegctl backup
ipegctl logs
```

## Upgrade transaction

```text
preflight
→ backup SQLite WAL + manifest
→ pull exact digest
→ stop new entries
→ reconcile/flat or close-only
→ deploy
→ migrate copy
→ health + supervisor + data smoke
→ commit upgrade
```

Failure автоматически выполняет rollback к предыдущему digest и предыдущему state
backup. Если active exposure существует, upgrade запрещён кроме emergency security
patch, который запускается в recovery-only.

## Observability

- structured JSON logs with secret redaction;
- Prometheus text endpoint/local scrape optional, без внешней платформы;
- health включает service/supervisor/private cache/market data/storage;
- disk alerts at 70/85/95%;
- Parquet retention 30 дней hot, старые данные compact/archive;
- daily SQLite backup, pre-upgrade backup обязательно;
- Telegram alert при restart, quarantine, stale private state, risk block, disk pressure.

## Availability

Watchdog/systemd/Docker restart. После restart:

```text
supervisor recovery first
shadow entry suspended if live action active
stable FLAT or QUARANTINED
then normal service
```

## Region benchmark

Codex реализует benchmark, но Germany остаётся production default. Переезд в Japan
разрешён только если:

```text
weighted Wave1 order/data p95 improves >=20%
and no single Wave1 p99 worsens >50%
```
