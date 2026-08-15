# Fast-track acceptance criteria

Acceptance is executable. Codex may improve tests and commands but may not replace these outcomes with prose.

## Bootstrap (`B-*`)

- **B-01** — `make verify` passes on Python 3.12 in CI and locally.
- **B-02** — default configuration is `shadow`; all default live-guard evaluations deny orders with a reason code.
- **B-03** — invalid risk relationships, unsupported products, negative values, or missing safety fields fail startup.
- **B-04** — the app starts with Docker Compose, reports health, and shuts down cleanly.
- **B-05** — no repository file contains an actual credential; `.env` and runtime data are ignored.
- **B-06** — application APIs expose no withdrawal or transfer operation.

## Product Ready (`PR-*`)

- **PR-01** — Wave 1 adapters are hidden behind one typed interface; domain code contains no raw CCXT/exchange payloads.
- **PR-02** — deterministic fixtures prove exact matching of linear USDT perpetual contracts and rejection of inverse, dated, spot, USDC, and ambiguous contracts.
- **PR-03** — at runtime at least two available Wave 1 venues can stream fresh BBO; unavailable venues are quarantined without stopping the process.
- **PR-04** — L2 books detect sequence/freshness faults and block entry until resynchronised.
- **PR-05** — executable VWAP honours depth, lot/tick rules, contract multipliers, and minimum notional.
- **PR-06** — directed route calculations distinguish A-long/B-short from B-long/A-short.
- **PR-07** — funding schedule/rate, fee source, and data quality are included in each decision; unknown values reject entry.
- **PR-08** — market data is recorded to partitioned Parquet and can be queried/replayed deterministically through DuckDB.
- **PR-09** — adaptive parameters are separate per directed route and size bucket, robust to outliers, versioned, and bounded against abrupt change.
- **PR-10** — every tranche has paired fills, actual quantity, costs, target, stop assumptions, and lifecycle state.
- **PR-11** — replay demonstrates full profitable and losing cycles with correct four-leg PnL and funding.
- **PR-12** — property tests prove pair projected stress <= 5 USDT and portfolio projected stress <= 50 USDT after every accepted action.
- **PR-13** — tests prove max 10 routes, one normal route per base, max five tranches, >=20% stressed local free margin, and <=3x initial effective leverage.
- **PR-14** — partial fill, rejected second leg, unknown result, stale private stream, venue outage, emergency hedge, and forced close have deterministic recovery tests.
- **PR-15** — normal execution intent always carries a worst acceptable price/slippage cap; unbounded market is emergency-only.
- **PR-16** — a restart with open simulated activity restores state, reconciles, and blocks new entries until consistent.
- **PR-17** — overload testing shows close/hedge/reconciliation continues while new entries are disabled.
- **PR-18** — every evaluated signal emits a stable reason code plus inputs, projected PnL/cost, and risk breakdown.
- **PR-19** — Telegram owner allowlisting works; pause/resume/kill/status operations are authenticated and audited.
- **PR-20** — `docker compose up --build` launches a continuous, non-stub Wave 1 shadow product from a clean checkout.

## Live Canary Ready (`CR-*`)

- **CR-01** — Bybit and OKX private capabilities are implemented and contract-tested; Binance USD-M is an alternate.
- **CR-02** — account mode, symbol availability, permissions, fee, margin, position mode, clock, and API trading availability are preflighted at runtime.
- **CR-03** — idempotent order submission and unknown-result reconciliation cannot create a duplicate order in tests.
- **CR-04** — actual fill quantity, not requested quantity, drives hedge and ledger state.
- **CR-05** — changing a YAML/env flag alone cannot activate live orders.
- **CR-06** — CI, tests, replay, shadow, stale qualification, wrong hashes, absent challenge, or non-allowlisted route always deny live orders.
- **CR-07** — canary policy restricts operation to one base, one directed route, one tranche, and minimum valid notional.
- **CR-08** — emergency close and third-venue hedge are tested under first-venue and second-venue failures.
- **CR-09** — repository and application contain no withdrawal/transfer implementation or permission request.
- **CR-10** — an owner can follow one runbook to deploy, add restricted secrets outside Git, qualify, unlock, observe, and disable the canary.

## Full target (`FT-*`)

- **FT-01** — Bitget, KuCoin Futures, MEXC, and BingX each pass the same capability/contract suite before being enabled.
- **FT-02** — a venue can be quarantined/removed without corrupting other routes or blocking risk reduction.
- **FT-03** — VPS region selection is based on reproducible p50/p95/p99 feed/API/private-event measurements, not assumption.
- **FT-04** — expansion does not regress any `PR-*` or `CR-*` criterion.
