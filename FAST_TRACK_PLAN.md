# Fast-track implementation plan

This is the only implementation plan. Codex updates checkboxes and `STATUS.md`; it does not create replacement plans.

## Operating method

- One branch: `codex/fast-track-mvp`.
- One draft PR, continuously updated through the checkpoints.
- Every checkpoint leaves a runnable product, not disconnected scaffolding.
- Build the synthetic/replay path before depending on live exchange availability.
- Do not wait for long-running qualification inside a coding session. Implement the qualification runner and its evidence output, then continue independent work.
- Quarantine a failing venue and continue with the remaining qualified venues.

## C0 — lean bootstrap

- [x] Package installs on Python 3.12.
- [x] `make verify` passes.
- [x] Typed configuration loads from YAML + environment variables.
- [x] SQLite WAL state store is initialised transactionally.
- [x] Structured logging, reason-code model, metrics skeleton, and CLI exist.
- [x] Live guard rejects every default/test/CI configuration.
- [x] Docker Compose starts a real application process with health reporting.

**Exit evidence:** CI run, `doctor` output, live-guard tests, restart smoke test.

## C1 — Wave 1 public market vertical slice

- [ ] Own `ExchangeAdapter` interface and normalised domain models.
- [ ] CCXT Pro implementations for Binance USD-M, Bybit, and OKX.
- [ ] Runtime capability probes and per-venue quarantine.
- [ ] Instrument discovery and exact linear-USDT-perpetual matching.
- [ ] Broad BBO subscriptions; candidate/open-route L2 subscriptions.
- [ ] Sequence/freshness/clock-skew controls.
- [ ] Funding, mark/index, contract metadata, and fee-source status.
- [ ] Directed executable-VWAP route calculation.
- [ ] Normalised Parquet recorder and DuckDB query smoke test.

**Exit evidence:** one command prints fresh common routes and executable spreads from at least two available Wave 1 venues; deterministic fixtures cover all three.

## C2 — complete strategy, risk, and simulator

- [ ] Deterministic event replay with controllable latency and disconnects.
- [ ] Robust adaptive-grid calibration per directed route and size bucket.
- [ ] Four-leg fee, funding, slippage, and stress-cost model.
- [ ] Tranche ledger and paired-action state machine.
- [ ] Atomic pair/global/local-margin risk reservation.
- [ ] Simulated partial fills, rejected leg, unknown order state, third-venue hedge, and forced close.
- [ ] Every signal returns a stable reason code and numerical decision breakdown.
- [ ] Property tests preserve risk and accounting invariants.

**Exit evidence:** a deterministic replay demonstrates open → add → partial close → full close and every major recovery path without exceeding configured projected risk.

## C3 — usable shadow product and Telegram operations

- [ ] Real-time shadow evaluator runs continuously on Wave 1 data.
- [ ] Telegram provides status, opportunities, simulated positions/PnL, data health, balances when available, `/pause`, `/resume`, `/close_all_simulated`, and `/kill`.
- [ ] Owner-only command allowlist and challenge confirmation for dangerous commands.
- [ ] State survives process/container restart.
- [ ] Reconciliation blocks entries until state is consistent.
- [ ] Overload policy prioritises open positions and disables new entries first.
- [ ] Docker healthcheck, rotation/retention, backup, and recovery commands work.
- [ ] Qualification runner writes a code/config/data-hash-bound result.

**Exit evidence:** clean Docker deployment operates in shadow, produces Telegram/CLI visibility, survives injected restart/feed failure, and resumes without inventing positions.

## C4 — live-canary-ready private execution

- [ ] Private streams, balances, positions, orders, cancel, and fee retrieval for Bybit and OKX; Binance USD-M alternate.
- [ ] Protected aggressive taker translation for each venue.
- [ ] Idempotent client order IDs and unknown-result reconciliation.
- [ ] Account/margin/position-mode preflight and isolated venue quarantine.
- [ ] Multi-factor live guard and one-route/one-tranche canary allowlist.
- [ ] Test-environment integration where safely available; otherwise contract fixtures plus a read-only production capability probe.
- [ ] Emergency close and pre-qualified third-venue hedge paths.
- [ ] No withdrawal/transfer endpoint is exposed by the application.

**Exit evidence:** all `CR-*` criteria pass without production secrets; an exact owner-action file lists the minimum credentials and VPS steps for the canary.

## C5 — owner-operated canary

This checkpoint requires owner credentials and explicit consent. Codex prepares but does not invent its evidence.

- [ ] Deploy to the lower-latency qualified VPS region.
- [ ] Add restricted, IP-allowlisted, no-withdrawal API credentials outside Git.
- [ ] Run current-hash shadow qualification.
- [ ] Enable exactly one base asset, one route, one tranche, and minimum valid notional.
- [ ] Confirm live challenge in Telegram.
- [ ] Observe actual fills, fees, funding, reconciliation, restart, and emergency controls.
- [ ] Disable live and produce an honest canary report before expansion.

## C6 — venue expansion

Only after C4 is complete and the C5 design has no critical defect:

- [ ] Add Bitget and KuCoin Futures.
- [ ] Add MEXC and BingX subject to runtime account/API capability.
- [ ] Run the same contract suite for every new venue.
- [ ] Add native transport overrides only for measured defects.
- [ ] Keep a venue removable without affecting open positions elsewhere.
