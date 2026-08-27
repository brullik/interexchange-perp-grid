# Codex working contract — Aggressive Fast Live V2

This repository optimizes for the shortest verified path to laptop live trading. Read only:

1. `GOAL.md`
2. `config/AGGRESSIVE_FAST_LIVE_V2.yaml`
3. `FAST_TRACK_PLAN.md`
4. `ACCEPTANCE.md`
5. the `Current state` section and newest relevant entries of `STATUS.md`

Do not create a second plan, PRD, requirements matrix, ADR catalog, audit report, status file, or parallel implementation track.

## Baseline

Reuse the existing private execution, protected IOC translation, durable journal, reconciliation, residual-delta recovery, emergency hedge/close, stable-FLAT, Telegram authorization, Windows DPAPI/S4U handling, live guard, risk stages, CI proofs, and required branch checks.

Do not rewrite a working subsystem without a failing regression test proving that a narrow change is necessary.

## Required change boundary

Implement only what is necessary to:

- remove every long-running qualification dependency from live activation;
- add deterministic 1-minute OHLC reference-spread history and aggregation;
- add the historical normal/extreme model;
- add a persistent five-level back-loaded grid;
- combine reference levels with executable L2/VWAP economics;
- make replay, shadow, canary, and pilot use one decision core;
- provide one Windows-native fast-live wrapper;
- keep VPS blocked until laptop acceptance.

## Qualification removal rule

The live path must not read or depend on:

- qualification epochs;
- elapsed qualification duration;
- synchronized-observation counters;
- funding-checkpoint counters;
- qualification JSON/artifact acceptance;
- the previous 12h or 24h policy.

Replace those dependencies with `FAST_LIVE_PREFLIGHT`, which evaluates the exact current runtime and expires quickly. Legacy qualification implementation may remain only when deleting it would cause unnecessary broad risk, but it must be unreachable from live activation and unable to authorize or block orders. Add tests proving this.

Do not spend time deleting historical evidence or harmless unreachable code unless it breaks tests, security, migrations, or user-facing commands. Remove active invocations, arguments, guards, scheduled tasks, blockers, and status claims first.

## Delivery rules

- Start from current clean `origin/main`; record exact SHA.
- Use one branch: `codex/aggressive-fast-live-v2`.
- Use one continuously updated draft PR.
- Implement one complete Wave 1 route first; do not wait for all seven venues.
- Use current adapters and add only the smallest missing 1m OHLC capability.
- Fetch history on demand for eligible routes; do not backfill every market before the first route works.
- Keep Python 3.12, `asyncio`, typed boundaries, `Decimal`, SQLite WAL, Parquet, and DuckDB.
- Use one strategy/risk decision core in replay, shadow, canary, and pilot.
- Update only `STATUS.md` and plan checkboxes.
- Run focused tests after coherent changes and `make verify` before checkpoint commits.
- Do not wait for market opportunities or long-running processes during coding. Prove behavior through deterministic replay, then prepare the owner-run live path.
- Use subagents only for independent read-only inspection, test generation, or final review; never parallel-edit one module.

## Laptop-first

The first live runtime is native Windows on the owner's laptop. Extend existing scripts and security; do not create another secret store or operations framework.

Required wrapper: `scripts/laptop-fast-live.ps1` with actions:

- `verify`
- `onboard`
- `preflight`
- `canary`
- `pilot`
- `status`
- `stop`

There is no `qualify` action.

## Non-negotiable controls

- Paired long/short only; directional exposure is transient recovery state only.
- Linear USDT-settled perpetuals only.
- Reference capital 500 USDT.
- Normal sizing uses <=4.50 USDT projected route loss; hard projected route limit <=5.00 USDT.
- Normal portfolio admission uses <=45 USDT; hard projected portfolio limit <=50 USDT.
- At most 10 routes, one route per base, five tranches per route.
- Cross margin in bot-dedicated accounts/subaccounts.
- At least 20% local free margin after stress.
- Initial effective leverage <=3x and never a sizing input.
- Hard holding cap 24 hours.
- No withdrawal, transfer, wallet, address-book, or API-key-management functionality.
- Normal execution is protected aggressive taker with price/slippage cap; unbounded market is emergency-only.
- Stale, unsynchronized, sequence-broken, incomplete, unknown, unreconciled, or non-FLAT state blocks new risk.
- Configuration alone never activates live.
- Never fabricate market, fill, PnL, latency, preflight, canary, pilot, or review evidence.
- Never put secrets in Git, prompts, logs, tests, screenshots, or artifacts.

## Autonomy and completion

Do not ask the owner to choose reversible technical details. Make the smallest compatible choice and implement it.

After exact-head green CI and independent review, resolve all material threads, mark the single PR ready, and squash-merge automatically when permissions and branch protection allow and the head SHA is unchanged.

Stop only after all independent work is complete and the remaining action genuinely requires local credentials or explicit live-money consent. Produce one owner action, not a sequence of technical questions.
