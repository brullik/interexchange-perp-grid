# Автономный master plan

Codex выполняет этапы последовательно, но независимые read-only/test задачи может
параллелить. Он не останавливается из-за отсутствия секретов, пока остаётся работа в
репозитории.

## Phase 0 — Repository control

1. Проверить visibility.
2. Попытаться перевести репозиторий в Private через доступный admin API.
3. Если permission отсутствует — создать единственный `OWNER_ACTION.json` с
   `REPOSITORY_MAKE_PRIVATE`, но продолжить код.
4. Сохранить PR №1 Draft до C4.3 proof.

## Phase 1 — C4.3 stable-FLAT hotfix

Реализовать `01_C4_3_STABLE_FLAT_HOTFIX.md` без других рефакторингов.

Exit:

- exact c4-3 proof green;
- unverified barrier never returns success;
- production submit calls = 0;
- adversarial review subagent PASS.

После PASS Codex может:

- перевести PR №1 в Ready;
- squash merge в main;
- создать annotated tag `v0.1.0-rc1`;
- собрать GHCR image, привязанный к main SHA и digest.

## Phase 2 — Production-grade Wave 1 data/private core

1. Заменить per-symbol private hot-path sweep native account-wide snapshot/cache.
2. Реализовать native Bybit orderbook sequence path.
3. Зафиксировать Binance/OKX/Bybit private state cache contracts.
4. Добавить rate-limit budgets, event watermarks, snapshot completeness.
5. Исправить Telegram shadow fallback без credentials.

Exit:

- account-wide state under bounded request rate;
- private cache restart/reconciliation chaos tests;
- broad public scan не деградирует при отсутствии private keys;
- p95 targets в synthetic load test.

## Phase 3 — Multi-instrument shadow product

1. InstrumentRegistry/UniverseService.
2. Broad BBO all common linear USDT perpetuals.
3. Candidate ranking and dynamic L2 scheduler.
4. Route-specific calibration for multiple bases/routes/size buckets.
5. Shadow portfolio with up to 10 routes and 5 tranches.
6. Telegram route/risk/PnL visibility.

Exit:

- не менее 100 common instruments в deterministic large-universe fixture;
- no unbounded queue/memory growth;
- 10 routes/50 tranches simulator stress;
- restart produces identical portfolio state.

## Phase 4 — Persistent multi-route live engine

1. Generalize live journal from one global active action to multiple actions.
2. Per-base and per-route durable lease.
3. Persistent PortfolioRiskBook.
4. LivePortfolioSupervisor for all active actions.
5. Paired tranche add/partial close/full close/max-hold.
6. Priority scheduler and overload behavior.

Exit:

- concurrent 10-route deterministic simulator;
- kill/restart at every transition;
- one route failure does not block risk reduction of others;
- no conflicting route on same base;
- global stress invariant always <= configured stage limit.

## Phase 5 — Wave 2 adapters

Order:

1. Bitget;
2. KuCoin Classic Futures.

Для каждой биржи:

```text
public capability → private read-only → simulator → shadow epoch
→ minimum canary-ready code → live disabled pending venue qualification
```

UTA KuCoin запрещён для production.

## Phase 6 — Wave 3 adapters

Order:

1. BingX;
2. MEXC public/read-only.

MEXC live execution остаётся disabled, пока official docs/runtime capability не
подтвердят production order/cancel.

## Phase 7 — Release/operations

1. Idempotent Ubuntu 24.04 bootstrap.
2. Dedicated user `ipeg`, root only for bootstrap.
3. Docker Compose + systemd.
4. `/etc/ipeg/ipeg.env` mode 0600.
5. GHCR image pinned by digest.
6. State backup before upgrade.
7. Automatic rollback on failed health/reconciliation.
8. Germany as default VPS region.
9. Region benchmark command; Japan migration только если weighted p95 improves >=20%
   and no Wave1 p99 worsens >50%.

Exit: clean VPS deploy/upgrade/rollback/restart proof.

## Phase 8 — Autonomous qualification orchestrator

State machine starts exact epoch and continuously reports:

- elapsed duration;
- per-route samples;
- data gaps;
- funding checkpoints;
- simulated PnL/MAE;
- code/config/image identity;
- blockers.

During 24h epoch deployed release is immutable. Codex may continue development on a
separate branch, but may not replace the qualifying image.

## Phase 9 — One-time owner onboarding

Only after all code and operations are complete, emit one consolidated action:

```text
ONE_TIME_ONBOARDING
```

It collects VPS, Telegram and restricted exchange credentials. No technical choices
are delegated to owner.

## Phase 10 — C5 and staged rollout

1. Exact one-route/one-tranche canary.
2. Stable exchange-verified FLAT.
3. Pilot A.
4. Pilot B.
5. Wave1 production.
6. Venue-by-venue Wave2/Wave3 canaries.
7. Full target profile.

Promotion is automatic only by gates in `06_ACCEPTANCE_GATES.md`.

## Phase 11 — Final product

Tag `v1.0.0` only when all software acceptance criteria pass. Operational status may
be one of:

```text
SOFTWARE_COMPLETE_WAITING_OWNER_ONBOARDING
OPERATING_SHADOW
OPERATING_CANARY
OPERATING_PILOT
OPERATING_PRODUCTION
```

`DONE` requires a running production deployment and successful stage gates; it
cannot be fabricated without external accounts and consent.
