# Runbook владельца: shadow, квалификация и минимальный canary

## Текущий запрет

Статус проекта — `C4_REWORK_IMPLEMENTED_PENDING_INDEPENDENT_REVIEW`. C5 и реальные ордера запрещены до зелёного CI на точном финальном commit и отдельного независимого повторного ревью. Сейчас не добавляйте production credentials и не включайте live.

При любом неизвестном состоянии действует fail-closed: новые входы запрещены, live остаётся выключенным, открытые позиции и ордера проверяются непосредственно на всех трёх биржах.

## 1. Проверка и shadow

На чистом checkout ветки `codex/fast-track-mvp`:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
make verify
cp .env.example .env
docker compose up --build -d
docker compose ps
docker compose exec app interexchange-grid shadow-status
```

Ожидаемый результат: `make verify` проходит, контейнер healthy, режим `shadow`, live выключен. Shadow должен непрерывно работать не менее 24 часов; практически оставьте 25+ часов. Для каждой стороны будущего точного направленного маршрута нужны не менее 10 000 уникальных синхронизированных L2-событий, непрерывность по policy и минимум три различных funding checkpoint.

Маршрут выбирается только из фактически прошедших наблюдений и записывается как `BASE:long_venue>short_venue`, например `BTC:binanceusdm>okx`. Направление нельзя менять после квалификации. Маршрут с `BOOK_SEQUENCE_UNKNOWN`, stale/unsynchronised data, gap или неизвестной комиссией не квалифицируется. В частности, Bybit + OKX запрещён, пока Bybit сообщает `BOOK_SEQUENCE_UNKNOWN`. Третья Wave 1 биржа выбирается автоматически как emergency venue и также обязана пройти live preflight; при её неопределённом состоянии canary не отправит ордера.

## 2. Воспроизводимое qualification evidence

Эти действия выполняются только на точном чистом commit. Сначала создайте тестовое доказательство матрицы replay/fault/restart:

```bash
git status --short
interexchange-grid replay-proof \
  --repo-root . \
  --config config/defaults.yaml \
  --output state/replay-proof.json
```

Команда откажет при изменённых/untracked source, tests или config. Она запускает детерминированную матрицу, сохраняет JUnit рядом с JSON и связывает оба файла с commit SHA, source hash и config hash.

Соберите образ из того же checkout, узнайте точный image ID и передайте proof в state-volume:

```bash
docker compose build --no-cache app
docker compose up -d app
export IPEG_RELEASE_SHA="$(git rev-parse HEAD)"
export IPEG_CONTAINER_IMAGE_DIGEST="$(docker inspect --format '{{.Image}}' "$(docker compose ps -q app)")"
docker compose cp state/replay-proof.json app:/app/state/replay-proof.json
docker compose cp state/replay-proof.junit.xml app:/app/state/replay-proof.junit.xml
```

После независимого одобрения C4 владелец создаёт restricted credentials только на двух сторонах выбранного маршрута для read-only/private fee qualification; перед canary нужны credentials всех трёх Wave 1 аккаунтов. Разрешены чтение account/positions/orders и futures trading. Withdrawal, transfer, wallet/address-book и управление API-ключами запрещены; IP allowlist обязателен, если доступен. Секреты хранятся только в VPS `.env`, который не попадает в Git.

Остановите запись Parquet, соберите runtime evidence без размещения ордеров, затем итоговую квалификацию:

```bash
docker compose stop app
docker compose run --rm \
  -e IPEG_RELEASE_SHA="$IPEG_RELEASE_SHA" \
  -e IPEG_CONTAINER_IMAGE_DIGEST="$IPEG_CONTAINER_IMAGE_DIGEST" \
  app interexchange-grid qualification-runtime \
  --route 'BTC:binanceusdm>okx' \
  --replay-proof /app/state/replay-proof.json \
  --output /app/state/qualification-runtime.json \
  --repo-root /app \
  --config /app/config/defaults.yaml

docker compose run --rm \
  -e IPEG_RELEASE_SHA="$IPEG_RELEASE_SHA" \
  app interexchange-grid qualify \
  --runtime-evidence /app/state/qualification-runtime.json \
  --evidence /app/state/qualification.json \
  --repo-root /app \
  --config /app/config/defaults.yaml

docker compose up -d app
```

Замените пример маршрута только на фактически наблюдавшийся направленный маршрут. Обе команды должны завершиться с кодом 0; итог содержит `"accepted": true`, точные route/commit/config/data/image hashes, immutable Parquet manifest, private taker fees, funding counts, replay/shadow statistics и нулевые unresolved order/exposure/error counters. Новые append-only Parquet-файлы разрешены, но изменение или исчезновение любого файла из qualification manifest, а также изменение source, config, image, направления маршрута или срока evidence инвалидирует canary.

## 3. Единственный минимальный canary — только после снятия запрета C5

До этого шага независимое ревью должно явно подтвердить C4. В `.env` задаются три комплекта restricted credentials, Telegram owner/token, `IPEG_RELEASE_SHA`, `IPEG_CONTAINER_IMAGE_DIGEST` и отдельный случайный `IPEG_LOCAL_UNLOCK_SECRET`; по умолчанию `IPEG_MODE=shadow` и `IPEG_LIVE_ENABLED=false` сохраняются.

Canary разрешает ровно один квалифицированный base/route, один tranche, minimum common notional, projected stressed loss не более 1 USDT, effective leverage не более 3x, free margin не менее 20%, без других позиций или ордеров. Направление берётся только из qualification evidence.

В работающем shadow Telegram выполните:

```text
/challenge
/confirm_live <одноразовый challenge>
```

Затем в пределах TTL остановите shadow polling и выполните ровно один запуск:

```bash
docker compose stop app
docker compose run --rm \
  -e IPEG_MODE=live \
  -e IPEG_LIVE_ENABLED=true \
  app interexchange-grid canary-run \
  --confirmation I_ACCEPT_LIVE_CANARY_RISK \
  --qualification /app/state/qualification.json \
  --repo-root /app \
  --config /app/config/defaults.yaml
```

Успехом считается только terminal state `FLAT` после приватной сверки всех ордеров и позиций на трёх биржах. Сам факт отправки двух ордеров не считается успехом. После команды немедленно верните shadow/live-disabled режим:

```bash
docker compose up -d app
docker compose exec app interexchange-grid shadow-status
```

## 4. Аварийное управление

Telegram live-control использует приватные данные бирж:

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

Для независимого CLI emergency unlock заранее хранится только `IPEG_EMERGENCY_UNLOCK_SECRET`. Значение `IPEG_EMERGENCY_UNLOCK` вводится на один вызов и не сохраняется:

```bash
read -s IPEG_EMERGENCY_UNLOCK
export IPEG_EMERGENCY_UNLOCK
docker compose run --rm \
  -e IPEG_EMERGENCY_UNLOCK \
  app interexchange-grid emergency-flatten \
  --confirmation I_CONFIRM_EMERGENCY_FLATTEN_ALL_LIVE_EXPOSURE \
  --qualification /app/state/qualification.json \
  --repo-root /app \
  --config /app/config/defaults.yaml
unset IPEG_EMERGENCY_UNLOCK
```

Emergency recovery использует активный durable journal даже при устаревшей квалификации. Если результат `FAILED_QUARANTINED`, live остаётся выключенным: вручную проверить все три аккаунта, отменить bot orders, закрыть остаточные позиции и подтвердить exchange-verified FLAT. Перезапуск не имеет права создать новый pair action, пока прежний не стал `FLAT`.
