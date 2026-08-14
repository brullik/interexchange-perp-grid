# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** C0_COMPLETE_C1_ACTIVE
- **Current checkpoint:** C1
- **Live orders:** impossible by default
- **Production credentials:** not present and not requested
- **Current Wave 1:** Binance USD-M, Bybit, OKX
- **Canary default:** Bybit + OKX, subject to runtime qualification

## Checkpoints

| Checkpoint | Status | Evidence |
|---|---|---|
| C0 lean bootstrap | COMPLETE | [GitHub Actions run 31835084239](https://github.com/brullik/interexchange-perp-grid/actions/runs/31835084239): `make verify` and Docker health/restart smoke passed on commit `c240ea3` |
| C1 public market vertical slice | IN_PROGRESS | Adapter/domain implementation is next |
| C2 strategy/risk/simulator | NOT_STARTED | — |
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

## Active blockers / owner actions

None. Codex must continue C0–C4 without production secrets.

## Last verified command

```text
GitHub Actions run 31835084239 on c240ea3:
- make verify: PASS (Ruff, mypy, 15 pytest tests, doctor)
- docker-smoke: PASS (build, health, persisted restart count)
```
