# Runbook владельца: software RC, qualification epoch и recovery

## Текущий запрет

Software-only контур готовится как `SOFTWARE_RELEASE_V1_RC`; это не live-разрешение. Реальные ордера запрещены до завершения внешних owner actions, точной 24-часовой qualification, независимых live gates и отдельного решения владельца о минимальном canary. Не добавляйте production credentials и не включайте live для software-проверок: CI обязан завершаться с `production_submit_calls=0`.

## 0. One-command Ubuntu 24.04 bootstrap и автономный runtime

На чистом Ubuntu 24.04 из exact release checkout выполните:

```bash
sudo scripts/ipegctl bootstrap
sudo ipegctl owner-onboard
sudo ipegctl deploy --image ghcr.io/brullik/interexchange-perp-grid@sha256:<digest> \
  --release-sha <full-main-sha>
sudo ipegctl doctor
sudo ipegctl status
```

`owner-onboard` работает только в локальном TTY, записывает Telegram/Wave1 credentials в
`/etc/ipeg/ipeg.env` mode `0600` и принудительно оставляет `IPEG_MODE=shadow` и
`IPEG_LIVE_ENABLED=false`. Не вставляйте значения из wizard в GitHub, чат или artifacts.
Systemd unit `ipeg.service` держит контейнер и встроенный `AutonomousOrchestrator` запущенными
после выхода Codex: он idempotently начинает/возобновляет exact immutable qualification epoch,
публикует blockers через `ipegctl status`, финализирует только collection epoch и никогда не
включает canary/live. `ipegctl canary-arm` остаётся fail-closed до отдельного
`LIVE_CANARY_CONSENT`.

Fail-closed действует при stale/несинхронизированных данных, sequence gap, неполном raw private snapshot, неизвестном состоянии ордера, недоступном risk engine, несовпадении journal/exchange, нестабильном FLAT или неопределённой возможности emergency venue. Один процесс `app` непрерывно владеет единственным Telegram poller и `LiveSafetySupervisor`; ручной перезапуск или повторный `canary-run` не является способом recovery.

## 1. Точная сборка и локальная проверка

На чистом checkout `codex/multi-instrument-shadow`:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps --no-build-isolation
python scripts/check_lock.py --lock requirements.lock --pyproject pyproject.toml --verify-installed
make verify
```

Воспроизводимая release-сборка принимает только чистый полный commit SHA, создаёт image, SBOM и machine-readable preflight:

```bash
scripts/release-build.sh registry.example/interexchange-perp-grid:$(git rev-parse HEAD)
```

`release-preflight` должен вернуть `passed=true`, точный `release_sha` и image digest; режим должен оставаться `shadow`, `live_enabled=false`, unbounded market execution запрещён. Не публикуйте tag как deployment identity: для VPS используйте только registry reference вида `IMAGE@sha256:<64 hex>`.

## 2. Deploy, upgrade, rollback и backup

Первичное shadow-развёртывание из immutable registry image:

```bash
cp .env.example .env
chmod 0600 .env
bash scripts/shadow-deploy.sh \
  registry.example/interexchange-perp-grid@sha256:<64-hex-digest> \
  <full-40-char-release-sha>
docker compose ps
docker compose exec -T app interexchange-grid health --config /app/config/defaults.yaml
```

Ожидается healthy `app`, `mode=shadow`, `live_orders_allowed=false`, supervisor `IDLE`/`FLAT_NO_ACTIVE_ACTION`. Upgrade сначала делает online SQLite backup и только затем меняет immutable image:

```bash
bash scripts/shadow-upgrade.sh <NEW_IMAGE@sha256:DIGEST> <NEW_FULL_SHA>
```

Rollback использует ранее проверенные digest/SHA и также делает backup текущего состояния:

```bash
bash scripts/shadow-rollback.sh <PREVIOUS_IMAGE@sha256:DIGEST> <PREVIOUS_FULL_SHA>
```

Отдельный backup и проверяемое восстановление:

```bash
docker compose exec -T app interexchange-grid backup-state \
  --config /app/config/defaults.yaml \
  --target /app/state/backups/manual.sqlite3
docker compose stop app
docker compose run --rm app interexchange-grid restore-state \
  --config /app/config/defaults.yaml \
  --backup /app/state/backups/manual.sqlite3
docker compose up --detach --wait app
```

Deploy отказывает, если `.env` отсутствует, отслеживается Git или имеет mode не `0600`. Успешная identity атомарно записывается в игнорируемый `.ipeg-deployment-state`. Upgrade делает backup; при плохом health автоматически останавливает новый image, восстанавливает SQLite backup, поднимает предыдущие digest/SHA и всё равно возвращает ненулевой код исходной ошибки. Если rollback также не прошёл, deployment остаётся fail-closed и требует ручной проверки.

После любого deploy/upgrade/rollback/restore проверьте `health`, supervisor outcome и фактические private orders/positions. При активном journal supervisor автоматически входит в `RECOVERY_ONLY`; не создавайте новый pair action.

## 3. Shadow и immutable qualification epoch

Shadow должен непрерывно работать минимум 24 часа. Для точного направленного маршрута нужны не менее 10 000 уникальных синхронизированных L2-событий с policy continuity и не менее трёх funding checkpoint. Bybit + OKX остаётся лишь предпочтительной парой: выбирается только реально квалифицированный маршрут. `BOOK_SEQUENCE_UNKNOWN` не квалифицируется.

Сначала соберите exact-head replay proof:

```bash
git status --short
interexchange-grid replay-proof \
  --repo-root . \
  --config config/defaults.yaml \
  --output state/replay-proof.json
```

Для каждой комбинации route/release/source/config/image откройте отдельный epoch. Команда идемпотентна только для полностью совпадающей identity; любое изменение закрывает старый epoch и обнуляет длительность/счётчики:

```bash
docker compose exec -T app interexchange-grid qualification-epoch-start \
  --route 'BTC:binanceusdm>okx' \
  --container-image-digest "$IPEG_CONTAINER_IMAGE_DIGEST" \
  --repo-root /app \
  --config /app/config/defaults.yaml
```

Сохраните возвращённый `epoch_id`. Пока epoch имеет статус `RUNNING`, непрерывный shadow сам связывает с ним наблюдения. После достижения policy закройте его; после `FINALIZED` новые наблюдения в него не принимаются:

```bash
docker compose exec -T app interexchange-grid qualification-epoch-status \
  --epoch-id <EPOCH_ID> --config /app/config/defaults.yaml
docker compose exec -T app interexchange-grid qualification-epoch-finalize \
  --epoch-id <EPOCH_ID> --config /app/config/defaults.yaml
```

`qualification-epoch-status` возвращает elapsed/remaining duration, completion ratio, точные per-venue required/current/remaining synchronized snapshots и funding checkpoints, quality/error counters, unresolved order/exposure и полный список blockers. Не выполняйте finalize, пока `ready_to_finalize=false`; это означает только готовность закрыть observation epoch. `qualification_ready` остаётся false до привязки replay evidence и устранения всех runtime blockers. Само истечение 24 часов не является qualification.

Только для finalized epoch соберите runtime evidence и qualification evidence:

```bash
docker compose exec -T app interexchange-grid qualification-runtime \
  --epoch-id <EPOCH_ID> \
  --route 'BTC:binanceusdm>okx' \
  --container-image-digest "$IPEG_CONTAINER_IMAGE_DIGEST" \
  --replay-proof /app/state/replay-proof.json \
  --output /app/state/qualification-runtime.json \
  --repo-root /app --config /app/config/defaults.yaml

docker compose exec -T app interexchange-grid qualify \
  --runtime-evidence /app/state/qualification-runtime.json \
  --evidence /app/state/qualification.json \
  --repo-root /app --config /app/config/defaults.yaml
```

Ожидается `accepted=true`, exact epoch FK и совпадающие route/release/source/config/image/data hashes. Изменение любого identity field, direction или файла immutable Parquet manifest инвалидирует qualification. Все три Wave 1 private preflight, включая фактические amount step/minimum/depth/fee emergency venue, обязаны пройти до записи canary intent.

## 4. Risk stage и canary после внешних gates

Этот раздел не является разрешением запускать C5. Владелец самостоятельно создаёт restricted credentials для выделенных Wave 1 subaccounts: только чтение account/orders/positions и futures trading, IP allowlist; withdrawal, transfer, wallet/address-book и API-key management запрещены. Секреты существуют только в VPS `.env` mode `0600`, никогда в Git/логах/evidence. Это внешнее действие невозможно выполнить средствами Codex. Проверка: `private-probe --venue <venue>` возвращает успешные обязательные capability checks для каждой Wave 1 venue; до этого live остаётся выключенным.

Текущий persisted stage и точная locked risk table:

```bash
docker compose exec -T app interexchange-grid risk-stage-status \
  --config /app/config/defaults.yaml
```

Promotion допускает только один соседний переход `shadow -> canary -> pilot_a -> pilot_b -> wave1_prod -> full`, требует current exact qualification, image digest, actor и точную фразу `PROMOTE:<target>`. Нельзя пропускать stage, откатывать stage этой командой или менять limits вне `RUNTIME_POLICY.yaml`. Сам первый переход является live-money решением владельца и не выполняется Codex без отдельного разрешения.

После отдельного live-money разрешения владельца первый переход выполняется так:

```bash
docker compose exec -T app interexchange-grid risk-stage-promote \
  --expected-current shadow --target canary --actor OWNER \
  --confirmation PROMOTE:canary \
  --qualification /app/state/qualification.json \
  --container-image-digest sha256:<EXACT_DIGEST> \
  --repo-root /app --config /app/config/defaults.yaml
```

Не останавливайте `app`. В единственном работающем Telegram poller получите challenge и подтвердите owner gate:

```text
/challenge
/confirm_live <одноразовый challenge>
```

В пределах TTL поставьте ровно один intent в durable journal:

```bash
docker compose exec -T \
  -e IPEG_MODE=live -e IPEG_LIVE_ENABLED=true \
  app interexchange-grid canary-run \
  --confirmation I_ACCEPT_LIVE_CANARY_RISK \
  --qualification /app/state/qualification.json \
  --repo-root /app --config /app/config/defaults.yaml
```

Команда не отправляет ордера: ожидается `orders_sent=0`, `terminal_state=PREPARED`, `recovery_action=QUEUED_FOR_LIVE_SAFETY_SUPERVISOR`. После durable commit единственный supervisor владеет submit, monitoring и recovery. Следите через:

```bash
docker compose exec -T app interexchange-grid health --config /app/config/defaults.yaml
```

Успех — только exchange-verified стабильный `FLAT`: минимум два последовательных полных raw private snapshot, quiet period и неизменный event watermark. `HEDGED` допустим только после private-confirmed позиций и не является terminal success. При kill/restart supervisor сначала восстанавливает тот же action без повторного owner/qualification entry gate и не разрешает новый.

Перед следующим соседним promotion сохраните JSON результата текущего stage с точными полями `stage`, `stable_flat_verified=true`, `active_action_count=0`, затем привяжите его hash к state:

```bash
docker compose exec -T app interexchange-grid risk-stage-complete \
  --stage canary --actor OWNER --evidence /app/state/canary-result.json \
  --config /app/config/defaults.yaml
```

Без этого неизменяемого результата следующий `risk-stage-promote` fail closed. Для каждого последующего stage повторяются qualification/current-image проверки, отдельное owner confirmation, фактический прогон и stable-FLAT completion; одна qualification не может автоматически провести все stages.

## 5. Аварийное управление

Telegram live-control обслуживается тем же единственным poller:

```text
/status
/positions
/balances
/pnl
/challenge
/cancel_all_live <challenge>
/close_all_live <challenge>
/emergency_flatten <challenge>
/kill <challenge>
```

Emergency flatten не зависит от qualification file и отменяет все активные ордера выделенных subaccounts, затем закрывает фактические позиции по их собственным symbol/instrument metadata. Для независимого CLI unlock `IPEG_EMERGENCY_UNLOCK_SECRET` заранее хранится только на VPS, а `IPEG_EMERGENCY_UNLOCK` передаётся на один вызов:

```bash
read -s IPEG_EMERGENCY_UNLOCK
export IPEG_EMERGENCY_UNLOCK
docker compose exec -T -e IPEG_EMERGENCY_UNLOCK app \
  interexchange-grid emergency-flatten \
  --confirmation I_CONFIRM_EMERGENCY_FLATTEN_ALL_LIVE_EXPOSURE \
  --config /app/config/defaults.yaml
unset IPEG_EMERGENCY_UNLOCK
```

При `BLOCKED`, `QUARANTINED`, incomplete private state или timeout live остаётся выключенным. Наблюдаемый результат для разблокировки — полные raw private snapshots всех трёх аккаунтов с нулём orders/positions, стабильный FLAT barrier и supervisor `IDLE`. До этого новый pair action запрещён.
