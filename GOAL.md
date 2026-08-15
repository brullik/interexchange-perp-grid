# Interexchange Perpetual Grid — fast-track product contract

## 1. Mission

Build an autonomous, auditable system that monitors equal linear USDT perpetual futures across exchanges and trades temporary executable-price divergence through paired positions:

- long on the cheaper venue;
- short on the more expensive venue;
- add up to five risk-sized tranches as divergence expands;
- close paired tranches as the executable spread converges;
- include fees, funding, market impact, latency, partial-fill risk, and emergency-exit cost in every decision.

This is convergence trading, not guaranteed or risk-free arbitrage.

## 2. Fastest delivery path

The first usable product is an end-to-end vertical slice, not seven incomplete connectors.

### Product Ready — autonomous target without private credentials

A single Docker deployment on one VPS must:

1. stream and normalise live public data from Binance USD-M, Bybit, and OKX;
2. discover equivalent linear USDT perpetuals and directed venue routes;
3. maintain fresh BBO for broad coverage and L2 depth for candidates/open routes;
4. calculate executable VWAP spreads for actual tranche size;
5. obtain funding and fee inputs, marking unknown values as a hard entry block;
6. record normalised data to Parquet and transactional state to SQLite WAL;
7. calibrate an adaptive grid from replay/live observations;
8. run the complete paired execution state machine against a deterministic simulator and in real-time shadow mode;
9. expose status, opportunities, simulated fills, PnL, risk, pause, and kill controls through Telegram;
10. recover after restart through persisted state and reconciliation logic;
11. emit explicit reason codes for every accepted or rejected signal.

### Live Canary Ready — code target before owner credentials

The same product must include private-data, order, cancel, position, balance, fee, and reconciliation paths for Bybit and OKX, with Binance USD-M as the first alternate. These paths must be testable through mocks, deterministic replay, and an exchange test environment where safely available.

Actual live activation is an owner action performed only after all automated gates pass.

### Expansion target

After a successful canary architecture exists, add:

1. Bitget and KuCoin Futures;
2. MEXC and BingX;
3. venue-specific native transport only where measured data proves CCXT Pro is insufficient.

A failed or unavailable venue is quarantined and must not block the rest of the product.

## 3. Fixed owner parameters

| Parameter | Requirement |
|---|---|
| Reference total capital | 500 USDT |
| Projected stressed loss per route | <= 5 USDT |
| Projected stressed portfolio loss | <= 50 USDT |
| Concurrent normal routes | <= 10 and may dynamically be lower or zero |
| Normal routes per base asset | 1 |
| Tranches per route | <= 5 |
| Contracts | Linear USDT-settled perpetuals only |
| Position construction | Paired long/short only |
| Margin mode | Cross in a bot-dedicated account/subaccount |
| Local free-margin floor | >= 20% after stress |
| Initial live effective leverage | <= 3x per venue |
| Configured exchange leverage | May be high, but never determines size |
| Holding period | Dynamic, hard cap 24 hours in first live stage |
| Cost multiplier | Configurable, default 2.0x stressed total cost |
| Automatic withdrawals/transfers | Forbidden in MVP |
| Emergency hedge | Allowed on a pre-qualified third venue |
| Interface | Telegram |
| Deployment | One VPS; Germany or Japan chosen by measured p95/p99 |
| HFT competition | Out of scope |

## 4. Minimal architecture

Use a Python 3.12 asynchronous modular monolith:

```text
CCXT Pro / venue override
        ↓
market + private adapter boundary
        ↓
in-memory normalised books and account state
        ↓
route evaluator → strategy → risk reservation
        ↓
paired execution coordinator / simulator
        ↓
SQLite WAL state + Parquet history + DuckDB replay
        ↓
Telegram control and structured metrics/logs
```

### Required architectural choices

- `ccxt`/CCXT Pro is the initial transport accelerator and must be pinned in the resolved dependency lock.
- The domain layer must not import exchange-specific response objects.
- Every venue exposes the same explicit capability report. Unsupported or unknown capability means disabled functionality, not an optimistic fallback.
- Monetary values, quantities, prices, funding, and fees use `Decimal` or integer fixed-point representations. Floating point is not allowed in risk/PnL decisions.
- Exchange timestamps are UTC; freshness uses local monotonic time. Clock skew is measured and surfaced.
- Hot-path decisions remain in memory. Storage writes are batched/asynchronous and may never block risk reduction.
- SQLite is sufficient for the MVP on one VPS. Migrate only after measured contention or scale justifies it.
- No web UI, distributed queue, service mesh, or orchestration platform in the MVP.

## 5. Executable spread and PnL

A directed route is `(instrument, long_venue, short_venue)`.

For normalised base quantity `q`:

```text
entry_long_price  = VWAP ask on long venue for q
entry_short_price = VWAP bid on short venue for q
entry_spread      = entry_short_price - entry_long_price

exit_long_price   = VWAP bid on long venue for q
exit_short_price  = VWAP ask on short venue for q
exit_spread       = exit_short_price - exit_long_price

gross_convergence_pnl = normalised_contract_value(q) × (entry_spread - exit_spread)
```

The implementation must correctly normalise contract multipliers, lot steps, price steps, minimum notional, and position units before comparing venues.

```text
stressed_total_cost =
    four-leg fees
  + entry and exit market impact
  + expected and stressed funding over the holding horizon
  + latency reserve
  + partial-fill / emergency-hedge reserve
  + reconciliation and forced-exit reserve

expected_net_pnl = gross_convergence_pnl - stressed_total_cost
```

A tranche may open only when both are true:

```text
expected_gross_pnl >= cost_multiplier × stressed_total_cost
expected_net_pnl   >= calibrated_minimum_profit_usdt
```

Unknown fees, funding schedule, contract metadata, depth, or data freshness blocks the entry.

## 6. Adaptive grid

Use one method but separate parameters per directed route and size bucket. Do not use one grid step for all instruments.

The calibrator must use robust statistics from rolling windows and long-tail stress history, including:

- median and MAD;
- empirical entry/exit quantiles;
- time-to-convergence by spread bucket;
- adverse excursion after entry;
- depth and slippage by size;
- funding over realistic holding horizons;
- regime-change and data-quality flags.

A practical initial rule may be:

```text
grid_step = max(cost_floor, liquidity_floor, robust_volatility_multiple)
```

subject to tick/lot rounding, minimum profit, maximum risk, and a stability limit on parameter changes.

Each tranche owns its actual two-leg fills, quantity, costs, target close level, stop assumptions, and state. Closing is paired and tranche-aware.

## 7. Risk model

Before reserving a new tranche, calculate the projected route loss at the defined emergency exit:

```text
open-tranche loss at route stop
+ all remaining closing fees
+ funding stress
+ exit slippage and market impact stress
+ unmatched-leg / emergency-hedge reserve
+ liquidation-distance reserve
<= 5 USDT
```

Also enforce:

- aggregate projected portfolio stress <= 50 USDT;
- maximum 10 normal routes;
- one normal route per base asset;
- maximum five tranches per route;
- venue effective leverage <= 3x during first live stage;
- at least 20% local free margin after venue-specific stress;
- enough depth to close both sides under stress;
- no new route while an unresolved unmatched leg or unknown order state exists;
- no reliance on profit held at another venue to prevent local liquidation.

Risk must be reserved atomically before order submission and released/reconciled from actual fills.

## 8. Execution contract

Normal execution intent is `PROTECTED_AGGRESSIVE_TAKER`:

1. calculate executable VWAP and a worst acceptable price;
2. submit both legs concurrently using marketable limit IOC or the safest venue-equivalent primitive;
3. reconcile actual fills, never requested quantity;
4. immediately cancel stale remainders;
5. hedge only the actual residual delta;
6. use idempotent client order IDs;
7. query private stream, order endpoint, and positions before retrying an unknown result.

Unbounded market orders are allowed only for `EMERGENCY_HEDGE`, `EMERGENCY_CLOSE`, or `LIQUIDATION_PREVENTION` when remaining directional exposure is assessed as more dangerous than slippage.

Minimum paired-action states:

```text
CREATED → PRECHECKED → RISK_RESERVED → ORDERS_SENT
→ PARTIALLY_HEDGED → HEDGED → CLOSING → CLOSED
```

Error/recovery states must include unknown order status, one-leg fill, stale data, venue outage, emergency third-venue hedge, and manual quarantine.

## 9. Data quality and overload behavior

Entries are fail-closed when any relevant feed is stale, unsynchronised, sequence-broken, reconnecting, clock-skewed beyond policy, or missing sufficient depth.

At overload:

1. open positions, close/hedge actions, private streams, and reconciliation have highest priority;
2. candidate L2 subscriptions are reduced;
3. broad BBO coverage may be reduced;
4. new entries are disabled before risk management is degraded.

## 10. Live activation gates

Live orders must be physically impossible unless all gates are true:

- runtime mode is `live`;
- `live_enabled` is true;
- CI/test/simulation flags are false;
- a local, non-repository unlock secret is present;
- Telegram owner confirmation matches a short-lived challenge;
- the route and venues are on the canary allowlist;
- capability, account-mode, margin, fee, funding, clock, data-quality, reconciliation, and risk preflights pass;
- shadow qualification has passed under the current code/config hashes;
- no kill switch, pause, stale state, unresolved owner action, or unknown order exists.

Changing configuration alone must not be sufficient to activate live trading.

API credentials must have trading permissions only, IP allowlisting where supported, and no withdrawal permission.

## 11. Definition of done

### Product Ready

All `PR-*` criteria in `ACCEPTANCE.md` pass. A clean machine can run:

```bash
cp .env.example .env
# add optional Telegram values; no exchange secrets required

docker compose up --build
```

and receive a working shadow product for Wave 1 rather than a stub or documentation-only result.

### Live Canary Ready

All `CR-*` criteria pass using mocks/replay and available test environments. Production credentials remain absent from Git. The only remaining owner work is to supply restricted credentials, deploy on the selected VPS, run qualification, and explicitly unlock one minimal canary route.

### Full target

All seven venues are supported only after the canary architecture is proven. No venue expansion may weaken Wave 1 behavior.

## 12. Explicit non-goals for the fast track

- guaranteed profit or guaranteed maximum realised loss;
- high-frequency or colocated trading;
- automatic inter-venue withdrawals or transfers;
- web/mobile UI;
- portfolio margin, inverse, coin-margined, USDC, options, or spot products;
- machine-learning prediction before a deterministic baseline works;
- seven native adapters before the first canary-ready vertical slice;
- long qualification waits hard-coded into development. Qualification is sample/quality based and runs independently on the VPS.
