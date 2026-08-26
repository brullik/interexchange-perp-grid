# Aggressive Fast Live V2 — executable acceptance

Prose is not evidence. Every PASS must be demonstrated by tests, machine-readable output, or reproducible commands.

All existing protected-main required checks and existing execution/recovery/security/C4 test suites from the baseline remain mandatory and may not be deleted or weakened.

## BL — baseline non-regression

- **BL-01** — Python 3.12 install and `make verify` pass locally and in CI.
- **BL-02** — required checks `verify`, `security`, `c4-critical-proof`, `c4-3-proof`, and `docker-smoke` pass on exact head.
- **BL-03** — default mode is shadow, live is false, and config alone cannot submit an order.
- **BL-04** — no secret is tracked or printed; no withdrawal/transfer API exists.
- **BL-05** — protected IOC caps, actual-fill reconciliation, unknown-result recovery, emergency hedge/close, restart recovery, and stable-FLAT regression suites pass.

## QR — removal of long-running qualification

- **QR-01** — live guard, canary, pilot, risk-stage promotion, and supervisor do not require a qualification file or epoch.
- **QR-02** — no active live decision reads elapsed qualification duration, synchronized-observation count, or funding-checkpoint count.
- **QR-03** — no current laptop command starts, resumes, finalizes, or waits for a qualification epoch.
- **QR-04** — `--qualification` is absent from the current fast-live canary/pilot/promotion command contracts.
- **QR-05** — an old accepted qualification artifact cannot authorize an otherwise blocked order.
- **QR-06** — a missing, stale, rejected, or malformed qualification artifact cannot block a valid fast-live preflight.
- **QR-07** — old SQLite columns/tables, if retained for compatibility, are ignored by live decisions and migrations are nondestructive.
- **QR-08** — automated tests prove that no 12h/24h timer or sample-count threshold remains in the activation path.

## FP — FAST_LIVE_PREFLIGHT

- **FP-01** — preflight returns one typed PASS/FAIL report with stable reason codes and numerical breakdown.
- **FP-02** — PASS binds exact merged SHA, config hash, strategy profile hash, native runtime manifest, route/direction, account identities, data generations, and risk stage.
- **FP-03** — dirty source, wrong hash, invalid config/profile, or wrong runtime fails closed.
- **FP-04** — missing/unknown private capability, fee, funding, metadata, position mode, margin mode, trading permission, or emergency venue fails closed.
- **FP-05** — any open order, non-FLAT position, unknown journal action, reconciliation mismatch, or unstable FLAT fails closed.
- **FP-06** — stale/unsynchronized/sequence-broken BBO or L2, excessive clock skew, or insufficient executable depth fails closed.
- **FP-07** — invalid history/model, regime block, non-positive economics, or projected risk/margin/leverage breach fails closed.
- **FP-08** — PASS expires after 600 seconds, after any bound identity/generation change, and after one entry intent.
- **FP-09** — preflight itself never authorizes or submits an order and does not require owner live consent.
- **FP-10** — report is ignored by Git, contains no secret, and completes without accumulating hours or counters.

## SB — reference spread bars

- **SB-01** — only closed synchronized UTC 1m bars are paired.
- **SB-02** — missing venue minute creates an invalid spread minute; no forward-fill occurs.
- **SB-03** — Open equals `10000*ln(Open_A/Open_B)`.
- **SB-04** — High equals `10000*ln(High_A/Low_B)`.
- **SB-05** — Low equals `10000*ln(Low_A/High_B)`.
- **SB-06** — Close equals `10000*ln(Close_A/Close_B)`.
- **SB-07** — 5m/15m/1h/4h/1d aggregate only completed 1m spread bars.
- **SB-08** — direct construction from exchange 1h/1d candles is rejected by tests.
- **SB-09** — identical input manifest produces byte/logically identical spread output and hash.
- **SB-10** — on-demand Wave 1 backfill, Parquet persistence, DuckDB query, and replay work from a clean checkout.

## HG — historical model and grid

- **HG-01** — the first laptop route requires at least 30 complete days; expansion gates can require more without reintroducing a timed runtime wait.
- **HG-02** — normal mode, deterministic ties, normal zone, positive/negative extremes, q99/q99.9, episodes, convergence, and adverse excursion are reproducible.
- **HG-03** — traded direction requires at least 10 completed episodes and >=70% convergence within 24h.
- **HG-04** — 24h/7d/30d regime drift blocks new entries but never blocks risk reduction.
- **HG-05** — entry levels are exactly 20/40/60/80/100% of the directional normal-to-extreme range, subject only to an executable cost floor.
- **HG-06** — tranche weights are exactly 10/15/20/25/30 and sum to one.
- **HG-07** — each level opens once per arm cycle; remaining above E1 cannot repeatedly open E1 or substitute for E2–E5.
- **HG-08** — a multi-level gap opens at most one tranche per decision cycle with fresh books/economics/risk between tranches.
- **HG-09** — model identity freezes after first tranche and survives restart until full FLAT.
- **HG-10** — reference/adaptive effective stop is executable and cannot be moved farther after entry.
- **HG-11** — reverse-grid exits are tranche-aware; deeper tranches close first where targets are reached.
- **HG-12** — a closed level re-arms only after >=0.25-step retreat and a new crossing.
- **HG-13** — deterministic replay proves open five, partial reverse close, re-arm, stop, full close, and restart recovery.

## ER — economics and risk

- **ER-01** — normal entry uses cost multiplier 1.35 and minimum expected net profit 0.15 USDT.
- **ER-02** — canary minimum 0.01 USDT is restricted to canary stage and cannot leak into pilot/normal operation.
- **ER-03** — four fees, entry/exit impact, slippage, latency, partial-fill, emergency, reconciliation, liquidation-distance, and funding costs are included.
- **ER-04** — positive funding is credited at 50%, adverse funding at 100%, and stress uses 2x.
- **ER-05** — convergence PnL without positive funding must be positive.
- **ER-06** — unknown fee/funding/depth/metadata/data blocks entry.
- **ER-07** — normal sizing keeps modelled route risk <=4.50 and hard projected route risk <=5.00 after every accepted action.
- **ER-08** — normal admission keeps modelled portfolio risk <=45 and hard projected portfolio risk <=50.
- **ER-09** — actual fills and rounded common base quantity drive post-fill risk and next-tranche admission.
- **ER-10** — one route per base, max 10 routes, max 5 tranches, >=20% local free margin, <=3x leverage, and 24h hold are enforced.

## PP — parity and execution

- **PP-01** — one strategy decision core is used by replay, shadow, canary, and pilot.
- **PP-02** — replay/shadow/live use the same level number, frozen model, target, stop, funding treatment, and risk formula.
- **PP-03** — live orders use executable L2/VWAP and protected price caps, not reference candle highs/lows.
- **PP-04** — partial fill, rejected leg, unknown result, stale data, venue outage, residual delta, third-venue hedge, emergency close, and restart remain deterministic and fail closed.
- **PP-05** — normal risk reduction remains available when new entries are blocked.
- **PP-06** — CI/replay/shadow evidence records `production_submit_calls=0`.

## LP — laptop fast live

- **LP-01** — `scripts/laptop-fast-live.ps1` exposes exactly `verify/onboard/preflight/canary/pilot/status/stop`; no `qualify` action.
- **LP-02** — wrapper composes current DPAPI/S4U/native runtime/Telegram/supervisor and does not create a second secret store.
- **LP-03** — `finally` always resets shadow/live=false, clears transient unlock variables, and leaves emergency control usable.
- **LP-04** — canary requires current single-use PASS preflight, local unlock, Telegram challenge, exact owner phrase, one route, one tranche, min notional, and hard risk <=1 USDT.
- **LP-05** — successful canary requires actual exchange evidence and stable-FLAT; no multi-hour post-FLAT wait is checked.
- **LP-06** — pilot requires a new PASS preflight and separate owner phrase; one route, up to five tranches, hard risk <=5 USDT, and normal economics.
- **LP-07** — replay proves all five levels; live pilot must complete at least one genuine paired round-trip but must not force unprofitable levels.
- **LP-08** — successful pilot stable-FLAT immediately creates ignored `state/laptop-fast-live-acceptance.json` with exact hashes and no 8h delay.
- **LP-09** — owner credentials and confirmations are entered locally and never exposed to Codex/GitHub/logs/artifacts.

## VP — VPS block

- **VP-01** — no task in this goal connects to, uploads to, deploys on, or changes a VPS.
- **VP-02** — any future VPS handoff fails closed without exact accepted laptop artifact and exact merged release identity.
