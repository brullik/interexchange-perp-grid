# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** C4_REWORK_IMPLEMENTED_PENDING_INDEPENDENT_REVIEW
- **Current checkpoint:** C4 (P0 rework implemented locally; final-head CI and independent review pending)
- **Live orders:** impossible by default
- **Production credentials:** not present and not requested
- **Current Wave 1:** Binance USD-M, Bybit, OKX
- **Canary route:** none; selected only from a route-specific qualified allowlist
- **C5:** forbidden until every P0 blocker in the independent audit is closed and re-verified

## Checkpoints

| Checkpoint | Status | Evidence |
|---|---|---|
| C0 lean bootstrap | COMPLETE | [GitHub Actions run 31835084239](https://github.com/brullik/interexchange-perp-grid/actions/runs/31835084239): `make verify` and Docker health/restart smoke passed on commit `c240ea3` |
| C1 public market vertical slice | COMPLETE | [GitHub Actions run 31837867113](https://github.com/brullik/interexchange-perp-grid/actions/runs/31837867113): Linux `make verify` (29 tests) and Docker health/restart passed on `a790344`; live read-only scan found 656 common instruments and two eligible Binance USD-M/OKX directed BTC routes while Bybit failed closed with `BOOK_SEQUENCE_UNKNOWN`; Parquet/DuckDB replay contained 206 L2 levels across all three venues |
| C2 strategy/risk/simulator | COMPLETE | [GitHub Actions run 31839163485](https://github.com/brullik/interexchange-perp-grid/actions/runs/31839163485): Linux `make verify` (43 tests) and Docker health/restart passed on `0849413`; deterministic tests cover open/add/partial close/full close, profitable and losing four-leg PnL, funding, protected prices, partial/rejected/unknown orders, private staleness, venue outage, third-venue hedge, forced close, and property-based 5/50 USDT risk invariants |
| C3 usable shadow product | COMPLETE | [GitHub Actions run 31840533502](https://github.com/brullik/interexchange-perp-grid/actions/runs/31840533502): Linux `make verify` (54 tests) and Docker continuous-service health/restart passed on `aa3715d`; tests prove live-snapshot calibration/risk/paired simulated fills, restart ledger restore and reconciliation block, overload priority, Telegram owner/challenge audit, integrity-checked backup/restore, retention, and code/config/data-hash qualification |
| C4 live-canary-ready execution | REWORK_IMPLEMENTED_REVIEW_REQUIRED | Corrected implementation has local lint/type/124-test/doctor evidence; exact final-head Linux CI, replay artifact, and independent re-review remain mandatory |
| C5 owner-operated canary | FORBIDDEN | Must not start until corrected C4 passes every P0 criterion and independent review |
| C6 venue expansion | NOT_STARTED | — |

## Decisions made during implementation

Append only short entries:

```text
YYYY-MM-DD — decision — reason — affected modules
```

2026-08-14 — Persist service heartbeat and restart count in SQLite WAL — Docker health must prove the application loop is alive and restart-safe — `state.py`, `service.py`, CLI, Compose
2026-08-14 — Use `ccxt.pro.binance` future transport for Binance USD-M — the `binanceusdm` Pro class lacked the required WebSocket capabilities in an automated probe — `ccxt_pro.py`
2026-08-14 — Quarantine books with unknown sequence and continue with remaining qualified venues — fail-closed market data must not stop the Wave 1 process — `market_data.py`, `public_engine.py`
2026-08-14 — Calibrate median/MAD grids independently per directed route and size bucket with a 20% update bound — outliers and abrupt parameter jumps must not destabilise entries — `strategy.py`
2026-08-14 — Reserve route, portfolio, and venue risk atomically before simulated submission — every accepted action must preserve the 5/50 USDT, local-margin, leverage, route, and tranche limits — `risk.py`, `execution.py`
2026-08-15 — Start the real public evaluator beside the persisted heartbeat and isolate network failures — Docker health and risk controls must remain responsive while a venue is slow or quarantined — `service.py`, `shadow.py`
2026-08-15 — Restore the complete actual-fill ledger and require explicit reconciliation before entry — a restart may never invent or silently discard simulated exposure — `state.py`, `shadow.py`
2026-08-15 — Keep Telegram token environment-only and require an owner challenge for kill/close-all — control commands must be authenticated, short-lived, and audited — `telegram_control.py`
2026-08-15 — Keep one CCXT Pro private boundary and limit venue-specific code to protected IOC/client-ID parameters — measured contracts support the required Wave 1 private capabilities without native connectors — `adapters/private.py`, `private_execution.py`
2026-08-15 — Never resubmit an unknown client order ID; query positions and order history until reconciled — a timeout must not create a duplicate live leg — `private_execution.py`
2026-08-15 — Make `LiveCanaryExecutor` the only private submit boundary and require an exact owner phrase plus all independent gates — YAML/env flags alone must remain incapable of placing an order — `safety.py`, `canary_runtime.py`, CLI
2026-08-15 — Bind qualification to one exact directed route, release/image/config, immutable Parquet manifest, private fees, 24-hour continuity, three funding checkpoints, persisted shadow statistics, and hashed replay/JUnit evidence — row counts or mutable JSON cannot qualify a canary — `qualification.py`, `state.py`, `shadow.py`, `release_evidence.py`, CLI, CI
2026-08-15 — Recompute all four-leg live economics from private fees, schedule-aware funding, depth impact and explicit recovery reserves; derive IOC caps from the marginal consumed level — canary entry must remain profitable after current full stress — `live_economics.py`, `routes.py`, `private_execution.py`
2026-08-15 — Persist both exact requests and every transition in SQLite WAL before network submission, and resume only the same idempotent action — crashes and unknown acknowledgements must never duplicate a leg or permit a new pair — `live_journal.py`, `live_coordinator.py`, `canary_runtime.py`
2026-08-15 — Treat exchange account/orders/positions as reconciliation truth and require zero open positions plus zero bot orders for terminal FLAT — netting nonzero positions is not flat — `adapters/private.py`, `live_reconciliation.py`, `live_control.py`
2026-08-15 — Select canary direction only from exact qualification evidence and derive the remaining Wave 1 venue for emergency recovery — hard-coded Bybit/OKX direction is unsafe while Bybit sequence is unknown — `config.py`, `canary_runtime.py`, owner runbook
2026-08-15 — Checkout the exact PR head SHA in every CI job and name replay artifacts from that SHA — GitHub's synthetic pull-request merge commit cannot serve as final-head evidence — CI workflow

## Active blockers / owner actions

### C4 independent acceptance

No owner credential or live-money action is requested. C5 remains forbidden until the exact final head has green Linux CI with the replay artifact and an independent reviewer accepts every P0 item. Repository defaults remain shadow/live-disabled, production secrets remain absent, and tests never reach a production submit endpoint.

## Last verified command

```text
2026-08-15 local Windows equivalent of every Makefile verify target: PASS
- ruff format --check + ruff check: PASS (66 files)
- mypy --strict: PASS (66 source/test files)
- pytest: 124 passed in 11.08s
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false

GNU make is not installed on this Windows host. Exact `make verify`, Docker smoke,
hashed replay artifact, and final commit identity must pass in Linux GitHub Actions.
No production credentials were used and no real order was submitted.
```
