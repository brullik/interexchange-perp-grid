# Codex working contract — Aggressive Symbiosis V1

This repository optimizes for the shortest verified path to a laptop-tested live product. Before coding, read only:

1. `GOAL.md`
2. `config/AGGRESSIVE_SYMBIOSIS_V1.yaml`
3. `FAST_TRACK_PLAN.md`
4. `ACCEPTANCE.md`
5. the `Current state` section and newest relevant entries of `STATUS.md`

Do not reread the complete historical evidence in `STATUS.md` unless a regression or exact-hash check requires it. Do not create a second plan, PRD, requirements matrix, ADR catalog, audit report, or parallel status document.

## Change boundary

The existing private execution, protected IOC order translation, durable journal, reconciliation, residual-delta recovery, emergency hedge/close, stable-FLAT barrier, Telegram authorization, Windows DPAPI/S4U workflow, live guard, and risk-stage machinery are the baseline. Reuse and extend them. Do not rewrite them unless a new executable acceptance test proves a specific incompatibility.

The required new work is limited to:

- deterministic one-minute OHLC reference-spread history and aggregation;
- the historical-normal/extreme model;
- the five-level aggressive grid and its persistent state;
- hybrid reference-spread plus executable-L2 entry/exit decisions;
- aggressive economics and risk sizing under unchanged hard limits;
- replay/shadow/live parity;
- Windows-native laptop verification, qualification, canary, and pilot evidence.

## Delivery rules

- Start from the current clean `origin/main`; record its exact SHA before changing code.
- Use one branch, `codex/aggressive-symbiosis-v1`, and one continuously updated draft PR. Continue them if they already exist.
- Keep the Python 3.12 asynchronous modular monolith, typed domain boundaries, `Decimal`, SQLite WAL, Parquet, and DuckDB.
- Use current adapters and CCXT/venue overrides. Add only the smallest missing OHLC capability.
- Fetch historical 1m data on demand for eligible candidates; do not download every market before the first vertical slice works.
- Use one strategy decision core from replay through shadow and live. Do not maintain separate simplified live logic.
- Implement one complete Wave 1 route first. Do not block the strategy on seven-venue completeness.
- Update only `STATUS.md` and the checkboxes in `FAST_TRACK_PLAN.md` after the package files are installed.
- Prefer a targeted test and working vertical slice over another document.
- Run focused tests after each coherent change and `make verify` before every checkpoint commit.
- Keep required branch checks, security scans, and exact-head evidence intact.

## Laptop-first rule

The first runtime is native Windows on the owner's laptop. Docker is not a laptop prerequisite. Extend the existing laptop scripts rather than building a second operations framework.

No VPS bootstrap, upload, deploy, qualification, credential transfer, or live execution is permitted until an exact-code/config/runtime-bound `state/laptop-aggressive-acceptance.json` exists with `accepted=true`. Preparing a later VPS handoff is allowed; executing it is not.

## Non-negotiable safety

- Paired long/short only; directional exposure is transient recovery state only.
- Linear USDT-settled perpetuals only.
- Reference capital: 500 USDT.
- Modelled route risk: at most 4.50 USDT; hard projected route loss: at most 5.00 USDT.
- Modelled portfolio risk: at most 45 USDT; hard projected portfolio loss: at most 50 USDT.
- At most ten routes, one route per base asset, and five tranches per route.
- Cross margin in bot-dedicated accounts/subaccounts; at least 20% local free margin after stress.
- Initial live effective leverage at most 3x and never used as the sizing input.
- Hard holding cap 24 hours in the first live program.
- No withdrawal, transfer, wallet, address-book, or API-key-management functionality.
- Normal execution remains protected aggressive taker with a price/slippage cap. Unbounded market execution remains emergency-only.
- Stale, unsynchronised, incomplete, sequence-broken, unknown, unreconciled, or unqualified state blocks new risk.
- Never weaken a safety mechanism merely to create more trades.
- Never fabricate market, fill, qualification, profitability, latency, or live evidence.
- Never place secrets in Git, prompts, chat, tests, logs, screenshots, or artifacts.

## Autonomy

Do not ask the owner to choose ordinary technical details. Choose the smallest reversible implementation consistent with `GOAL.md`, implement it, and record one concise decision in `STATUS.md` only when material.

Use subagents only for independent read-only inspection, test generation, or final review. Never let parallel agents edit the same module.

Create one owner action only when external credentials or an explicit live-money authorization are genuinely required. Complete every independent task first. The owner action must include the exact action, why Codex cannot perform it, exact validation, and fail-closed behavior.

## Completion and merge

After all software-only and laptop-shadow acceptance criteria pass on the exact head, obtain an independent review, resolve every material thread, mark the single PR ready, and squash-merge it automatically if permissions and branch protection allow and the head SHA is unchanged.

Real-money laptop canary/pilot remains separately owner-confirmed. A successful software merge is not live authorization.
