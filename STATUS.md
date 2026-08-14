# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** PRODUCT_READY_LIVE_CANARY_READY
- **Current checkpoint:** C5 (owner-operated canary; blocked by design)
- **Live orders:** impossible by default
- **Production credentials:** not present and not requested
- **Current Wave 1:** Binance USD-M, Bybit, OKX
- **Canary default:** Bybit + OKX, subject to runtime qualification

## Checkpoints

| Checkpoint | Status | Evidence |
|---|---|---|
| C0 lean bootstrap | COMPLETE | [GitHub Actions run 31835084239](https://github.com/brullik/interexchange-perp-grid/actions/runs/31835084239): `make verify` and Docker health/restart smoke passed on commit `c240ea3` |
| C1 public market vertical slice | COMPLETE | [GitHub Actions run 31837867113](https://github.com/brullik/interexchange-perp-grid/actions/runs/31837867113): Linux `make verify` (29 tests) and Docker health/restart passed on `a790344`; live read-only scan found 656 common instruments and two eligible Binance USD-M/OKX directed BTC routes while Bybit failed closed with `BOOK_SEQUENCE_UNKNOWN`; Parquet/DuckDB replay contained 206 L2 levels across all three venues |
| C2 strategy/risk/simulator | COMPLETE | [GitHub Actions run 31839163485](https://github.com/brullik/interexchange-perp-grid/actions/runs/31839163485): Linux `make verify` (43 tests) and Docker health/restart passed on `0849413`; deterministic tests cover open/add/partial close/full close, profitable and losing four-leg PnL, funding, protected prices, partial/rejected/unknown orders, private staleness, venue outage, third-venue hedge, forced close, and property-based 5/50 USDT risk invariants |
| C3 usable shadow product | COMPLETE | [GitHub Actions run 31840533502](https://github.com/brullik/interexchange-perp-grid/actions/runs/31840533502): Linux `make verify` (54 tests) and Docker continuous-service health/restart passed on `aa3715d`; tests prove live-snapshot calibration/risk/paired simulated fills, restart ledger restore and reconciliation block, overload priority, Telegram owner/challenge audit, integrity-checked backup/restore, retention, and code/config/data-hash qualification |
| C4 live-canary-ready execution | COMPLETE | [GitHub Actions run 31842172015](https://github.com/brullik/interexchange-perp-grid/actions/runs/31842172015): Linux `make verify` (70 tests) and Docker health/restart passed on `059439a`; read-only production probes reported no missing CCXT Pro private capability for Bybit, OKX, or Binance USD-M; contract tests cover streams/account/orders/cancel/fees, protected IOC, idempotent unknown reconciliation, complete preflight, exact minimal canary policy, both venue-failure hedge directions, and zero submit calls before all live gates |
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
2026-08-14 — Calibrate median/MAD grids independently per directed route and size bucket with a 20% update bound — outliers and abrupt parameter jumps must not destabilise entries — `strategy.py`
2026-08-14 — Reserve route, portfolio, and venue risk atomically before simulated submission — every accepted action must preserve the 5/50 USDT, local-margin, leverage, route, and tranche limits — `risk.py`, `execution.py`
2026-08-15 — Start the real public evaluator beside the persisted heartbeat and isolate network failures — Docker health and risk controls must remain responsive while a venue is slow or quarantined — `service.py`, `shadow.py`
2026-08-15 — Restore the complete actual-fill ledger and require explicit reconciliation before entry — a restart may never invent or silently discard simulated exposure — `state.py`, `shadow.py`
2026-08-15 — Keep Telegram token environment-only and require an owner challenge for kill/close-all — control commands must be authenticated, short-lived, and audited — `telegram_control.py`
2026-08-15 — Keep one CCXT Pro private boundary and limit venue-specific code to protected IOC/client-ID parameters — measured contracts support the required Wave 1 private capabilities without native connectors — `adapters/private.py`, `private_execution.py`
2026-08-15 — Never resubmit an unknown client order ID; query positions and order history until reconciled — a timeout must not create a duplicate live leg — `private_execution.py`
2026-08-15 — Make `LiveCanaryExecutor` the only private submit boundary and require an exact owner phrase plus all independent gates — YAML/env flags alone must remain incapable of placing an order — `safety.py`, `canary_runtime.py`, CLI

## Active blockers / owner actions

### C5 owner action — one minimal live canary

1. Review and deploy commit `059439a` from draft PR #1 to one bot-dedicated VPS. Create bot-dedicated Bybit and OKX accounts/subaccounts in cross margin and one-way position mode. Keys must have trading/read permissions only, IP allowlisting where supported, and no withdrawal permission. Codex cannot perform account eligibility, VPS access, credential creation, or irreversible live-money consent.
2. Store only outside Git in the VPS `.env`: `IPEG_BYBIT_API_KEY`, `IPEG_BYBIT_API_SECRET`, `IPEG_OKX_API_KEY`, `IPEG_OKX_API_SECRET`, `IPEG_OKX_API_PASSWORD`, `IPEG_TELEGRAM_ENABLED=true`, `IPEG_TELEGRAM_BOT_TOKEN`, `IPEG_TELEGRAM_OWNER_CHAT_ID`, and a new random `IPEG_LOCAL_UNLOCK_SECRET`. Keep `IPEG_MODE=shadow` and `IPEG_LIVE_ENABLED=false` while qualifying.
3. Deploy and collect current-hash evidence:

   ```text
   docker compose up --build --detach --wait
   docker compose exec -T app interexchange-grid private-probe --venue bybit
   docker compose exec -T app interexchange-grid private-probe --venue okx
   docker compose exec -T app interexchange-grid qualify --config /app/config/defaults.yaml --repo-root /app --evidence /app/state/qualification.json
   ```

   Observable result: both probes have `missing: []`; qualification has `accepted: true` and `QUALIFICATION_PASSED`. If either fails, that venue is not eligible and canary submission remains denied.
4. In the owner Telegram chat send `/challenge`, then `/confirm_live <TOKEN>`. Within the configured 120-second TTL, execute exactly one owner-confirmed minimum-notional pair:

   ```text
   docker compose exec -T -e IPEG_MODE=live -e IPEG_LIVE_ENABLED=true app interexchange-grid canary-run --config /app/config/defaults.yaml --repo-root /app --qualification /app/state/qualification.json --confirmation I_ACCEPT_LIVE_CANARY_RISK
   ```

   Observable result: `submitted: true`, exactly one configured base/route/tranche, and both returned orders contain actual fill state. Any stale/wrong hash, missing unlock/challenge, non-allowlisted route, account/data/risk/reconciliation failure, pause/kill, or unknown order returns `submitted: false` and a stable reason code.
5. Observe `/status`, `/positions`, `/pnl`, `/data_health`, exchange order/position panels, and structured logs. To disable, issue a fresh `/challenge` then `/kill <TOKEN>`, restore `IPEG_MODE=shadow` and `IPEG_LIVE_ENABLED=false`, restart Compose, and revoke the trading keys.

Fail-closed until completion: repository defaults stay shadow/live-disabled; credentials are absent; no Telegram live confirmation or local unlock exists; `canary-run` refuses before network without the exact owner phrase and refuses before submit unless every independent gate passes.

## Last verified command

```text
GitHub Actions run 31842172015 on 059439a:
- make verify: PASS (Ruff, mypy, 70 pytest tests, doctor)
- docker-smoke: PASS (continuous shadow process build, health, persisted restart count)
- read-only private-probe: PASS on Bybit, OKX, and Binance USD-M (`missing: []`; no credentials or orders)
- guarded canary runner: PASS (wrong owner phrase exits before network; config-only and every missing independent gate produce zero private submissions)
```
