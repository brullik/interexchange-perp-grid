# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** FAST_TRACK_BOOTSTRAP_PACKAGE
- **Current checkpoint:** C0
- **Live orders:** impossible by default
- **Production credentials:** not present and not requested
- **Current Wave 1:** Binance USD-M, Bybit, OKX
- **Canary default:** Bybit + OKX, subject to runtime qualification

## Checkpoints

| Checkpoint | Status | Evidence |
|---|---|---|
| C0 lean bootstrap | NOT_STARTED | — |
| C1 public market vertical slice | NOT_STARTED | — |
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

## Active blockers / owner actions

None. Codex must continue C0–C4 without production secrets.

## Last verified command

```text
Not run yet after repository upload.
```
