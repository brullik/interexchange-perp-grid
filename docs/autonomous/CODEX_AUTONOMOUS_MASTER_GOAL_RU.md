# MASTER GOAL ДЛЯ CODEX — автономно довести продукт до нормального готового состояния

Репозиторий: `brullik/interexchange-perp-grid`.

Исходная проверенная точка:

```text
branch: codex/fast-track-mvp
Draft PR: #1
head: 95a30f9821bae2c7515b07cba1f2ca51af2565de
CI: 31887616649
C4 artifact: 9247692764
```

## 0. Сначала импортируй пакет

Скопируй все файлы из `docs/autonomous/`, `config/` и `schemas/` этого пакета в
репозиторий. Корневой `CODEX_AUTONOMOUS_MASTER_GOAL_RU.md` также сохрани в
`docs/autonomous/CODEX_AUTONOMOUS_MASTER_GOAL_RU.md`.

Прочитай их полностью. Они являются нормативными и имеют приоритет над прежними
fast-track prompts в части следующих этапов.

Создай `AUTONOMOUS_STATUS.json` по schema и обновляй его после каждого значимого
commit/CI/merge/state transition.

## 1. Не задавай владельцу технических вопросов

Все решения зафиксированы. Не предлагай альтернативы и не останавливайся для выбора.
Если venue/API не соответствует контракту:

```text
fail closed → quarantine venue/function → continue all other work
```

Owner action разрешён только для физически внешнего доступа/секрета/финансового
согласия по `08_OWNER_ACTION_PROTOCOL.md`.

## 2. Немедленно исправь C4.3

Текущий C4 НЕ принят.

В `LiveCanaryCoordinator._verify_stable_flat` и
`LiveControlService._stable_report` сейчас отбрасывается
`FlatBarrierResult.verified`. Исправь строго по
`01_C4_3_STABLE_FLAT_HOTFIX.md`.

Не выполняй другие рефакторинги до зелёного C4.3 proof.

Создай exact required scenario manifest SF-001…SF-008. Artifact:

```text
c4-3-proof-<FULL_SHA>
```

Обязательные assertions:

```text
false_success_when_barrier_unverified = 0
production_submit_calls = 0
failures = errors = skips = 0
```

Запусти отдельного adversarial reviewer, который читает spec, diff и tests, но не
использует вывод implementer. Исправь все P0/P1.

Когда C4.3 proof и review PASS:

1. обнови PR №1;
2. переведи его в Ready;
3. squash-merge в main;
4. создай tag `v0.1.0-rc1`;
5. создай immutable GHCR image и release manifest.

Если GitHub permission не позволяет merge/tag/visibility, создай точный
`OWNER_ACTION.json`, но продолжи всю независимую работу в новой ветке от verified
head.

## 3. После C4.3 выполняй весь master plan без остановки

Строго следуй `05_AUTONOMOUS_MASTER_PLAN.md`:

```text
Wave1 production private/data core
→ multi-instrument shadow
→ persistent multi-route live engine
→ Bitget
→ KuCoin Classic
→ BingX
→ MEXC public/read-only
→ deployment/qualification automation
→ software v1 release candidate
```

Не начинай с семи поверхностных adapters. Каждый следующий venue использует уже
готовый общий engine.

## 4. Зафиксированные реализации

### Private state

Запрещён per-symbol REST sweep в hot path. Для Binance USD-M, Bybit и OKX:

- native account-wide startup snapshot;
- account-wide orders/positions/account private streams;
- in-memory cache + persistent event watermark;
- periodic REST reconciliation 30s;
- pre-submit/post-cancel/post-restart/terminal reconciliation;
- cache age <=2s для entry;
- raw completeness fail-closed.

### Market universe

- все common active linear USDT perpetuals;
- BBO broad cache;
- top 30 candidates получают L2;
- active routes всегда имеют L2;
- debounce 100ms;
- listing age >=14d для live.

### Strategy/risk

Реализуй формулы и stage profiles только из
`03_LOCKED_STRATEGY_RISK_EXECUTION.md` и `RUNTIME_POLICY.yaml`.

### Multi-route engine

Generalize существующий durable journal/coordinator:

- много active pair actions;
- one route per base;
- up to 10 routes and 5 tranches;
- persistent atomic portfolio risk;
- per-route locks;
- priority recovery queue;
- restart recovery all actions;
- no new entry while critical recovery pending.

### Exchanges

Используй exact matrix `04_EXCHANGE_IMPLEMENTATION_MATRIX.yaml`.

- KuCoin UTA live запрещён; Classic Futures only.
- Bitget WS order placement только при доказанном permission, иначе REST IOC.
- MEXC live disabled, пока official order/cancel marked Under maintenance.
- Unknown venue capability никогда не превращается в optimistic support.

### Deployment

Один Ubuntu 24.04 VPS, Germany default, Docker Compose + systemd, `ipegctl`, exact
image digest, backup/rollback. Не внедряй Kubernetes/PostgreSQL/Kafka/web UI.

## 5. Автономное GitHub выполнение

Для каждого phase:

1. scoped branch;
2. implementation;
3. exact manifest tests;
4. adversarial review;
5. release verification;
6. Draft PR;
7. Ready + squash merge только после locked gates;
8. следующий phase без ожидания владельца.

Не объединяй unrelated phases в один гигантский diff после PR №1.

## 6. Не останавливайся на SOFTWARE_COMPLETE, если доступ уже есть

Если в environment уже доступны VPS connection и external secrets, используй их
только через предусмотренный secure onboarding и продолжай:

```text
deploy exact image
→ start exact qualification epoch
→ shadow 24h
→ finalize qualification
→ request short-lived LIVE_CANARY_CONSENT
→ canary
→ staged pilots
```

Ты не можешь обещать фоновую работу из текущей интерактивной сессии. Поэтому должен
реализовать и установить на VPS `AutonomousOrchestrator`/systemd, который продолжает
qualification, monitoring, promotion gates и recovery без работающего Codex.

Если external access отсутствует, не притворяйся, что deployment/live выполнены.
Заверши всё программное и создай один consolidated `ONE_TIME_ONBOARDING` owner action.

## 7. Запреты

- не размещать реальный ордер до отдельного live consent;
- не создавать/передавать secrets через Git/GitHub/artifacts/chat;
- не добавлять withdrawal/transfer;
- не повышать risk вне stage gate;
- не ослаблять data/economics/reconciliation;
- не использовать unbounded market для normal entry;
- не заявлять поддержку venue без capability evidence;
- не считать tests доказательством прибыльности;
- не требовать, чтобы бот обязательно торговал.

## 8. Конечная цель

`SOFTWARE_COMPLETE` означает:

- code merged to main;
- v1 release candidate image;
- all seven public integrations/capability states;
- Wave1 full engine;
- multi-instrument/multi-route/adaptive grid;
- 10-route/5-tranche persistent simulator and live architecture;
- Telegram;
- deployment/rollback/onboarding;
- all criteria in `FINAL_ACCEPTANCE_MANIFEST.json` PASS or explicit venue quarantine;
- no owner technical decisions remain.

`DONE` означает только реально работающий deployment, прошедший qualification,
canary и stage gates. Не фабрикуй DONE без внешних evidence.

## 9. Финальный ответ Codex

Верни:

```text
Current autonomous state:
Final main SHA:
Release tag:
Image digest:
Open/merged PRs:
Required checks:
Acceptance criteria passed/total:
C4.3 proof:
Wave1 status:
Wave2 status:
Wave3 status:
Deployment status:
Qualification status:
Live stage:
Production submit calls in CI:
Owner action: NONE | <exact file>
Known limitations:
Next action performed by installed orchestrator:
```

Не заканчивай ответ словами о плане. Выполни максимально возможную часть цели в
текущем запуске и оставь репозиторий/оркестратор в состоянии, которое продолжает
работу без ручного выбора решений.
