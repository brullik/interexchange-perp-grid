# Зафиксированная архитектура продукта

Все решения в этом документе нормативны. Codex не выбирает альтернативную
архитектуру. Если официальная API-возможность отсутствует, биржа помещается в
quarantine; остальные части продукта продолжаются.

## 1. Форма приложения

```text
Python 3.12 asynchronous modular monolith
Docker Compose on one Ubuntu 24.04 VPS
SQLite WAL transactional state
Parquet market history
DuckDB replay/analytics
Telegram-only control plane
```

Запрещены до доказанной необходимости:

- Kafka/RabbitMQ;
- PostgreSQL;
- Kubernetes;
- микросервисы;
- Redis;
- web UI;
- ML-модель принятия решений;
- автоматические withdrawals/transfers.

## 2. Нормативные компоненты

### `InstrumentRegistry`

- только active linear USDT-settled perpetuals;
- canonical key: `(base, quote=USDT, settle=USDT, linear, perpetual)`;
- refresh каждые 6 часов и при reconnect/startup;
- live-фильтр listing age не менее 14 суток;
- ambiguous contract mapping запрещён;
- contract multiplier, tick, amount step, minimum notional обязательны.

### `UniverseService`

- строит все common instruments минимум на двух биржах;
- создаёт оба directed routes для каждой пары бирж;
- broad BBO включён для максимально доступной вселенной;
- отключённая биржа не останавливает остальные.

### `MarketDataSupervisor`

- native WebSocket или доказанный CCXT transport;
- BBO хранится как latest-value cache, старые непрочитанные BBO coalesce;
- L2 подписки только для ranked candidates и active routes;
- default L2 budget: 30 directed candidates одновременно плюс все active routes;
- decision debounce: 100 ms на route;
- reconnect с jittered exponential backoff 1–30 секунд;
- entry при stale/sequence/checksum/clock error запрещён.

### `PrivateStateCache`

Текущий per-symbol REST sweep запрещён в hot path.

Для Wave 1 реализовать:

1. account-wide REST snapshot при startup;
2. account-wide private order/position/account streams;
3. event watermark и sequence/updated-time checks;
4. periodic full reconciliation каждые 30 секунд;
5. немедленную REST reconciliation перед submit, после cancel/unknown/restart и
   перед terminal FLAT;
6. maximum cache age для entry: 2 секунды;
7. incomplete raw record → `UNKNOWN`, entry/FLAT запрещены.

Per-symbol enumeration разрешён только как diagnostic fallback и никогда не является
квалифицированным live hot path.

### `CandidateEngine`

Двухступенчатый pipeline:

```text
all BBO routes
→ cheap cost-aware prefilter
→ top ranked candidates
→ L2/funding/private-fee/full economics
```

BBO prefilter не открывает сделки. Он только назначает L2 budget.

### `RouteCalibrator`

- параметры отдельно для `(base, long venue, short venue, size bucket)`;
- rolling windows: 24h, 7d, 30d; более длинные данные сохраняются для stress;
- median/MAD, quantiles, adverse excursion, convergence time, liquidity, funding;
- parameter change cap 20% за 24 часа;
- regime shift блокирует новые входы.

### `PersistentPortfolioRiskBook`

- SQLite WAL;
- atomic transaction на reserve/release/reconcile;
- учитывает фактические exchange positions/open orders;
- до 10 routes, одна route на base, до 5 tranches;
- route stress <= 5 USDT, portfolio stress <= 50 USDT;
- effective leverage <= 3x;
- stressed free margin >= 20%;
- unresolved action/unknown state блокирует конфликтующие entry.

### `LivePortfolioSupervisor`

Generalize canary journal, не создавать второй execution engine.

- множество `pair_action_id`;
- одна активная normal route на base;
- per-route durable lock;
- global atomic portfolio-risk lock;
- priority: flatten > hedge > close > reconcile > existing position > new entry;
- restart восстанавливает все active actions;
- новые entries не обслуживаются, пока critical recovery queue не пуста.

### `TelegramControlPlane`

Один polling instance на весь процесс.

Обязательные команды:

```text
/status
/health
/opportunities
/routes
/positions
/orders
/pnl
/risk
/data_health
/exchanges
/qualification
/pause
/resume
/challenge
/cancel_all_live <token>
/close_all_live <token>
/emergency_flatten <token>
/kill <token>
```

Без private credentials read-команды возвращают shadow данные и явный
`PRIVATE_STATE_UNAVAILABLE`; бот не должен падать.

## 3. Event scheduling

Использовать bounded priority queues:

```text
P0 emergency flatten / liquidation prevention
P1 unmatched hedge / unknown-order reconciliation
P2 normal close / max-hold close
P3 private state and risk reconciliation
P4 new entry
P5 candidate L2
P6 broad BBO/history writes
```

При overload сначала уменьшаются P6/P5, затем полностью запрещается P4. P0–P3
никогда не отбрасываются.

## 4. Производительность

Target для одного VPS, не HFT:

- BBO-to-prefilter p95 <= 100 ms;
- qualified L2 decision p95 <= 250 ms;
- private event-to-state p95 <= 250 ms;
- order acknowledgement измеряется, но не имеет искусственного SLA;
- storage не блокирует risk-reduction path;
- memory bounded; latest-value caches имеют TTL и size limits.
