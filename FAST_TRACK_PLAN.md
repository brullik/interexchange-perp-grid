# Aggressive Fast Live V2 — единственный план

Codex обновляет только эти чекбоксы и `STATUS.md`. Не создавать другой roadmap.

## Правила ускорения

- Одна ветка `codex/aggressive-fast-live-v2`, один draft PR.
- Один полный Wave 1 route раньше широкого охвата.
- Не ждать длительных процессов и рыночной возможности во время кодинга.
- Не удалять безопасный working code ради чистоты; сначала убрать активные зависимости и доказать tests.
- Не менять execution/recovery/security foundation без regression test.
- Не переходить на VPS.

## A0 — baseline и установка контракта

- [x] Получить актуальный `origin/main`, проверить clean tree и записать exact baseline SHA.
- [x] Проверить отсутствие другого активного PR этой цели; продолжить его, если существует.
- [x] Установить `AGENTS.md`, `GOAL.md`, `FAST_TRACK_PLAN.md`, `ACCEPTANCE.md`, `config/AGGRESSIVE_FAST_LIVE_V2.yaml`.
- [x] Запустить baseline `make verify` и Windows-equivalent; отличить существующий failure от нового regression.
- [x] Создать/продолжить один draft PR.

**Exit:** baseline зафиксирован, contract files применены, existing behavior reproducible.

## A1 — полностью убрать длительную квалификацию из live path

- [x] Найти все runtime/CLI/script/config/test зависимости live от epoch, duration, observation count, funding checkpoints и qualification artifact.
- [x] Удалить эти зависимости из live guard, canary, risk-stage, orchestrator и laptop path.
- [x] Убрать `--qualification` из актуальных canary/pilot/promotion команд.
- [x] Остановить создание/возобновление qualification epochs и scheduled qualification tasks.
- [x] Сделать старые qualification artifacts неавторитетными: они не разрешают и не блокируют live.
- [x] Сохранить недеструктивную совместимость SQLite, если физическое удаление legacy tables создаёт лишний риск.
- [x] Добавить тесты, доказывающие отсутствие любого 12h/24h/count-based live gate.

**Exit:** live path не читает qualification state; focused tests green.

## A2 — FAST_LIVE_PREFLIGHT

- [x] Добавить typed preflight report и stable reason codes.
- [x] Привязать PASS к exact merged SHA/config/profile/native runtime/route/account/data generation/risk stage.
- [x] Проверять clean source, required checks, private capabilities, FLAT/orders/journal, clocks, data, metadata, fees, funding, depth, economics, margin and risk.
- [x] TTL 600 seconds, immediate invalidation on relevant change, single-use intent.
- [x] Записывать ignored `state/fast-live-preflight.json` без секретов.
- [x] Добавить positive и fail-closed tests каждого blocker.

**Exit:** preflight завершается немедленно и не накапливает длительность/счётчики.

## A3 — 1m reference-spread vertical slice

- [x] Добавить минимальную OHLC capability в текущий adapter boundary.
- [x] Синхронизировать closed UTC minutes без forward-fill.
- [x] Реализовать Open/High/Low/Close формулы из `GOAL.md` через `Decimal`/fixed point.
- [x] Агрегировать 5m/15m/1h/4h/1d только из 1m spread bars.
- [x] Persist Parquet manifest/hash и DuckDB query/replay.
- [x] Реализовать on-demand backfill сначала для одного Wave 1 route.
- [x] Добавить deterministic fixtures, missing-minute и direct-1h-prohibition tests.

**Exit:** один route имеет воспроизводимую 30-day модель без прямого старшего расчёта.

## A4 — historical model и пять уровней

- [x] Рассчитать mode/normal zone, directional extremes, q99/q99.9, episodes, convergence and adverse excursion.
- [x] Применить 30-day first-live minimum, 10 episodes, 70% convergence, regime block.
- [x] Создать пять уровней 20/40/60/80/100 и веса 10/15/20/25/30.
- [x] Persist arm/open/closed/rearm state каждого уровня и frozen model identity.
- [x] Исправить любой путь, который повторно использует только E1.
- [x] Реализовать sequential gap catch-up with fresh books between tranches.
- [x] Реализовать effective stop, reverse-grid exits и 0.25-step re-arm.
- [x] Добавить restart/replay tests полного пятиуровневого цикла.

**Exit:** open/add/reverse-close/re-arm/stop survive restart deterministically.

## A5 — economics, risk и parity

- [x] Применить 1.35x cost multiplier и 0.15 USDT normal minimum.
- [x] Учитывать положительный funding на 50%, отрицательный на 100%, stress 2x.
- [x] Требовать положительный convergence PnL без positive funding.
- [x] Рассчитать back-loaded sizing под 4.50/5 route и 45/50 portfolio limits.
- [x] Пересчитывать риск по actual fills перед следующей tranche.
- [x] Подключить executable stop к replay, shadow, canary и pilot.
- [x] Использовать один decision core; убрать упрощённую отдельную live-логику.
- [x] Сохранить IOC caps, actual-fill recovery, third venue, emergency flatten and stable-FLAT proofs.

**Exit:** all targeted/property/fault/restart tests green.

## A6 — Windows fast-live wrapper

- [x] Создать `scripts/laptop-fast-live.ps1` с `verify/onboard/preflight/canary/pilot/status/stop`.
- [x] Переиспользовать existing DPAPI/S4U/native runtime/Telegram/supervisor; не создавать second secret store.
- [x] Полностью убрать `qualify` action и dependency на `laptop-qualification.ps1` из нового пути.
- [x] Canary: one route/one tranche/min notional/<=1 USDT and separate owner consent.
- [x] Pilot: new preflight, separate owner consent, one route/up to five tranches/<=5 USDT.
- [x] После successful pilot stable-FLAT сразу создавать `state/laptop-fast-live-acceptance.json`; no 8h wait.
- [x] В `finally` всегда shadow/live=false и secret env cleanup.
- [x] Добавить Windows script/CLI contract tests.

**Exit:** native laptop runbook is one command surface with no long wait gate.

## A7 — exact-head verification, review и merge

- [ ] Запустить focused suites, `make verify`, security, C4 critical/C4.3, Docker smoke and Windows-equivalent.
- [x] Доказать `production_submit_calls=0` для CI/replay/shadow.
- [x] Проверить migrations/restart from current baseline state.
- [x] Обновить `STATUS.md` честным exact-head evidence; не переписывать историческую хронику.
- [x] Получить independent review, исправить P0/P1/P2 и material threads.
- [ ] Mark Ready и squash-merge при unchanged head and all required green.

**Exit:** one merged PR, exact merged SHA, no unresolved material defects.

## A8 — единственный owner action

После A0–A7 Codex завершает все независимые действия и выдаёт один owner action:

- [ ] локальный DPAPI/S4U onboarding restricted credentials;
- [ ] запуск `preflight`;
- [ ] отдельное подтверждение `canary`;
- [ ] после stable-FLAT отдельное подтверждение `pilot`.

Codex не просит секреты в чат и не принимает решение о реальных средствах за владельца.

## A9 — laptop acceptance и будущий VPS handoff

- [ ] После реального pilot проверить exact evidence и stable-FLAT.
- [ ] Создать ignored `state/laptop-fast-live-acceptance.json` с accepted=true.
- [ ] Подготовить, но не выполнять VPS handoff, который fail-closed без exact accepted artifact/release.
- [ ] Не подключаться к VPS и не переносить secrets.
