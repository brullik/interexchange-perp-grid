# Codex working contract

This repository optimizes for the shortest safe path to a usable product. Read only these files before coding:

1. `GOAL.md`
2. `FAST_TRACK_PLAN.md`
3. `ACCEPTANCE.md`
4. `STATUS.md`

Then start implementing the current checkpoint. Do not create a second planning system, a requirements matrix, an ADR catalog, or extra status documents.

## Delivery rules

- Build one end-to-end vertical slice before broadening exchange coverage.
- Wave 1 is Binance USD-M, Bybit, and OKX. A live canary uses the first two qualified accounts; Bybit + OKX is the default pair.
- Use the unified CCXT Pro transport behind our own `ExchangeAdapter` interface. Add a native venue-specific override only when an automated capability probe or measured failure proves it is required.
- Keep a modular monolith on one VPS. Do not introduce Kafka, Redis, Celery, Kubernetes, or microservices in the MVP.
- Use Python 3.12, `asyncio`, typed domain models, `Decimal` for monetary arithmetic, SQLite WAL for transactional state, and Parquet + DuckDB for market history and replay.
- Prefer a working implementation and automated test over another document.
- Keep one feature branch and one draft PR. Commit each completed checkpoint to that branch and keep advancing the same PR.
- Update only `STATUS.md` after each checkpoint or material decision.
- Use subagents only for independent read-heavy work, test generation, or review. Never let parallel agents edit the same module.

## Non-negotiable safety

- Paired long/short only. A directional exposure may exist only transiently during execution recovery.
- Linear USDT-settled perpetuals only.
- No withdrawal, transfer, address-book, wallet, or API-key-management functionality.
- Live trading is disabled by default and must require all independent gates defined in `GOAL.md`.
- Standard execution is protected aggressive taker execution with a price/slippage cap. Unbounded market execution is emergency-only.
- Never size from the exchange's maximum leverage. Enforce pair, portfolio, local-margin, and effective-leverage limits.
- Fail closed on stale/unsynchronised data, sequence gaps, unknown order state, unavailable risk engine, failed reconciliation, or capability uncertainty.
- Never add secrets to Git, tests, logs, fixtures, prompts, screenshots, or evidence.
- Never fabricate exchange, latency, fill, profitability, qualification, or test evidence.

## Autonomy

Do not ask the owner to choose ordinary technical details. Choose the simplest reversible option consistent with `GOAL.md`, implement it, and record a one-line decision in `STATUS.md`.

Create an owner action only when work truly requires an external credential, account permission, Telegram token/chat ID, VPS access, regulatory/account eligibility, or an irreversible live-money decision. Continue every independent task before stopping.

Owner actions must contain:

- exact required action;
- why Codex cannot do it;
- exact validation command or observable result;
- fail-closed behavior until completion.

## Verification

Before every checkpoint commit run:

```bash
make verify
```

A checkpoint is complete only when its acceptance criteria are demonstrated by executable tests or reproducible commands. A statement that something works is not evidence.
