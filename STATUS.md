# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** C1_COMPLETE_C2_ACTIVE
- **Current checkpoint:** C2
- **Live orders:** impossible by default
- **Production credentials:** not present and not requested
- **Current Wave 1:** Binance USD-M, Bybit, OKX
- **Canary default:** Bybit + OKX, subject to runtime qualification

## Checkpoints

| Checkpoint | Status | Evidence |
|---|---|---|
| C0 lean bootstrap | COMPLETE | [GitHub Actions run 31835084239](https://github.com/brullik/interexchange-perp-grid/actions/runs/31835084239): `make verify` and Docker health/restart smoke passed on commit `c240ea3` |
| C1 public market vertical slice | COMPLETE | [GitHub Actions run 31837867113](https://github.com/brullik/interexchange-perp-grid/actions/runs/31837867113): Linux `make verify` (29 tests) and Docker health/restart passed on `a790344`; live read-only scan found 656 common instruments and two eligible Binance USD-M/OKX directed BTC routes while Bybit failed closed with `BOOK_SEQUENCE_UNKNOWN`; Parquet/DuckDB replay contained 206 L2 levels across all three venues |
| C2 strategy/risk/simulator | IN_PROGRESS | Adaptive strategy, risk reservation, ledger, and deterministic recovery replay are next |
| C3 usable shadow product | NOT_STARTED | — |
| C4 live-canary-ready execution | NOT_STARTED | — |
| C5 owner-operated canary | BLOCKED_BY_DESIGN | Requires owner credentials and explicit live consent |
| C6 venue expansion | NOT_STARTED | — |

## Decisions made during implementation

Append only short entries:

```text
YYYY-MM-DD — decision — reason — affected modules
```

2026-08-14 — Persist service heartbeat and restart count in SQLite WAL — Docker health must prove the application loop is alive and restart-safe — `state.py`, `service.py`, CLI, Compose
2026-08-14 — Use `ccxt.pro.binance` future transport for Binance USD-M — the `binanceusdm` Pro class lacked the required WebSocket capabilities in an automated probe — `ccxt_pro.py`
2026-08-14 — Quarantine books with unknown sequence and continue with remaining qualified venues — fail-closed market data must not stop the Wave 1 process — `market_data.py`, `public_engine.py`

## Active blockers / owner actions

None. Codex must continue C0–C4 without production secrets.

## Last verified command

```text
GitHub Actions run 31837867113 on a790344:
- make verify: PASS (Ruff, mypy, 29 pytest tests, doctor)
- docker-smoke: PASS (build, health, persisted restart count)
- live public-scan with IPEG_MAX_CLOCK_SKEW_MS=2000: PASS (656 common instruments; Binance USD-M + OKX quote-ready; Bybit quarantined with BOOK_SEQUENCE_UNKNOWN)
- Parquet/DuckDB evidence: PASS (206 L2 levels; deterministic replay across all three Wave 1 venues)
```
