# Interexchange Perpetual Grid — Aggressive Symbiosis V1 product contract

## 1. Mission

Extend the current product into an aggressive but bounded interexchange convergence trader. It must combine:

1. deterministic historical reference-spread bars built from synchronized 1-minute OHLC data;
2. long-horizon normal state and historical positive/negative extremes;
3. a persistent five-level back-loaded grid;
4. current robust 24h/7d/30d regime statistics;
5. real executable L2/VWAP prices, private fees, funding schedules, slippage, and market impact;
6. the existing protected paired execution, journal, reconciliation, recovery, Telegram, and live-safety machinery.

The system should use capital and recurring spread oscillations more aggressively than the current adaptive-only strategy, while retaining the hard projected-loss limits and fail-closed execution controls.

This is statistical convergence trading. Profit and a realised maximum loss cannot be guaranteed.

## 2. Preserve the proven baseline

Do not replace the existing architecture. Preserve and regression-test:

- the typed `ExchangeAdapter` boundary and current venue transports;
- broad BBO plus candidate/open-route L2 subscriptions;
- protected aggressive taker IOC execution with price caps;
- actual-fill-driven hedge logic;
- idempotent client order IDs;
- durable SQLite WAL action/tranche journal;
- restart reconciliation and unknown-result handling;
- residual-delta top-up, reduction, third-venue hedge, and emergency flatten;
- atomic risk reservation and stable-FLAT release;
- Telegram owner authentication and live challenge;
- Windows native runtime manifest, DPAPI/S4U secret handling, qualification, and laptop pilot workflow;
- exact-code/config/data/runtime evidence and branch protection checks.

Change those components only where a new acceptance test proves that the aggressive strategy cannot be connected without a narrow extension.

## 3. Fixed owner limits

| Parameter | Requirement |
|---|---|
| Reference total capital | 500 USDT |
| Modelled route loss used for normal sizing | <= 4.50 USDT |
| Hard projected route loss | <= 5.00 USDT |
| Modelled portfolio loss used for normal admission | <= 45.00 USDT |
| Hard projected portfolio loss | <= 50.00 USDT |
| Normal routes | <= 10 |
| Routes per base asset | 1 |
| Tranches per route | 5 |
| Contracts | Linear USDT-settled perpetuals only |
| Position construction | Paired long/short only |
| Margin mode | Cross in bot-dedicated accounts/subaccounts |
| Local free margin after venue stress | >= 20% |
| Initial effective leverage | <= 3x per venue |
| Maximum holding time | 24 hours from the first still-open tranche |
| Automatic withdrawals/transfers | Forbidden |
| Emergency third-venue hedge | Allowed only on a pre-qualified venue |
| First runtime | Native Windows laptop |
| VPS use | Forbidden until laptop acceptance artifact is accepted |

The 0.50 USDT route difference and 5.00 USDT portfolio difference are execution reserves, not additional normal trading capacity.

## 4. Canonical instrument and pair identity

A contract is eligible only when both venues expose the same economically normalized:

- base asset;
- linear perpetual product type;
- USDT settlement and margin;
- contract multiplier and base-quantity interpretation;
- active, non-delisting status.

Normalize aliases such as `PEPE` versus `1000PEPE` before comparison. Ambiguous mapping blocks the pair.

For reference bars, a venue pair has a canonical order `(A, B)` determined by lexical normalized venue ID. The order never changes with price. Directed executable routes are derived from the sign of the reference divergence:

- positive divergence: short A and long B;
- negative divergence: long A and short B.

The canonical reference pair and the directed live route must be explicitly linked in persisted state.

## 5. One-minute source bars

### 5.1 Input

Use only closed 1-minute OHLC bars from both exchanges. Each source bar must contain:

- UTC interval start;
- open, high, low, close;
- source venue and normalized instrument identity;
- completeness/data-quality status;
- contract metadata version.

A minute is `[t, t + 60 seconds)`. Pair only bars with exactly the same UTC interval start.

Reject a minute when either side is missing, incomplete, non-positive, non-finite, duplicated without an identical payload, tied to a different contract specification, or affected by a known discontinuity. Never forward-fill or substitute the previous price.

### 5.2 Canonical reference-spread bar

For the fixed canonical pair `(A, B)`, express spread in basis points:

```text
open  = 10000 × ln(open_A  / open_B)
high  = 10000 × ln(high_A  / low_B)
low   = 10000 × ln(low_A   / high_B)
close = 10000 × ln(close_A / close_B)
```

Use `Decimal` or a deterministic fixed-point equivalent with an explicitly tested precision/rounding policy.

`high_A / low_B` and `low_A / high_B` are deterministic synthetic minute envelopes. The two extremes may have occurred at different instants. Therefore these bars are valid for historical geometry, charts, stress, and trigger arming, but they are never evidence that the spread was executable. Entry and exit remain gated by synchronized L2/VWAP.

### 5.3 Higher intervals

Build every intraday and daily reference-spread bar only from completed 1-minute reference-spread bars:

```text
open_T  = open of the first 1m spread bar
high_T  = maximum high of all 1m spread bars
low_T   = minimum low of all 1m spread bars
close_T = close of the last 1m spread bar
```

Required intervals are 5m, 15m, 1h, 4h, and 1d. A day is `00:00:00–23:59:59 UTC`.

If any required minute is missing, the higher bar is `INCOMPLETE` and cannot enter the trading model. It may be displayed only with its incomplete status. Direct calculation from exchange 5m/15m/1h/4h/1d bars is forbidden.

### 5.4 Acquisition and storage

Add the smallest public-history capability to the current adapter boundary. Prefer the current common transport; add a native override only for a measured capability defect.

History download must be:

- on-demand for eligible candidates and open routes;
- resumable and idempotent;
- rate-limit aware;
- bounded in concurrency;
- cached by venue, instrument, contract version, and minute;
- stored in Parquet with a DuckDB query/replay path;
- integrity checked before becoming trade-eligible.

Do not block the first vertical slice on downloading every symbol from every venue.

## 6. Historical reference model

### 6.1 Windows

Target 180 complete days. Live eligibility requires at least 90 complete days. A 30-day minimum may be used only for shadow experimentation and never for live entry.

Calculate positive and negative directions separately.

### 6.2 Normal state

Round valid 1-minute closes to 1 basis-point buckets. The normal state `S0` is the modal bucket.

Deterministic tie-breaks:

1. bucket nearest the exact median;
2. smaller absolute value;
3. smaller numeric value.

Define:

```text
d_t = abs(close_t - S0)
normal_half_width = max(2 bps, q10(d_t))
normal_low  = S0 - normal_half_width
normal_high = S0 + normal_half_width
```

### 6.3 Historical extremes

After data-quality rejection only:

```text
H_plus  = maximum valid 1m reference-spread high
H_minus = minimum valid 1m reference-spread low
R_plus  = H_plus - S0
R_minus = S0 - H_minus
```

Do not silently delete a valid statistical outlier. Data errors must be rejected through explicit quality evidence and a reason code.

Disable a direction when its range is non-positive or its required historical evidence is absent.

### 6.4 Convergence episodes

An episode begins after the reference spread was in the normal zone and then reaches the first grid level. It ends at the first of:

- return to the normal zone;
- the hard 24-hour horizon;
- unavailable/incomplete data.

A direction is eligible for normal live trading only with at least ten completed historical episodes and a convergence rate of at least 70% within 24 hours. A canary still requires positive executable economics and may use only a route that passes the current qualification policy.

Persist per-level convergence time, adverse excursion, censoring, and episode count.

### 6.5 Current-regime guard

Keep the current robust 24h, 7d, and 30d median/MAD/quantile model as a regime and long-tail guard, not as the sole grid definition.

For each direction, reject new entry when the current 7-day median has drifted from `S0` by more than both:

```text
0.25 × the direction's historical range
and
3 × current robust sigma
```

Also retain existing data-quality, depth, funding, and bounded parameter-change gates. A model update may not change live parameters by more than 20% per day.

Freeze `S0`, normal zone, extremes, levels, weights, stop, economics version, and route identity when the first tranche opens. Do not move the stop farther during an active route.

## 7. Aggressive five-level grid

For positive divergence:

```text
E_plus[i] = S0 + (i / 5) × R_plus, i = 1..5
```

For negative divergence:

```text
E_minus[i] = S0 - (i / 5) × R_minus, i = 1..5
```

Use the fixed tranche weights:

```text
level 1: 10%
level 2: 15%
level 3: 20%
level 4: 25%
level 5: 30%
```

These weights deliberately back-load size into deeper divergence and replace equal 20% sizing for this profile.

Reference stops:

```text
reference_stop_plus  = S0 + 1.15 × R_plus
reference_stop_minus = S0 - 1.15 × R_minus
```

The effective stop is the farther risk-reducing boundary of the reference stop and the current adaptive long-tail stop:

- positive route: maximum of the two positive stop values;
- negative route: minimum of the two negative stop values.

If no valid adaptive stop exists, use the reference stop. A farther stop reduces permitted size; it never expands the 5 USDT risk limit.

## 8. Persistent grid state machine

Persist each level independently with at least:

```text
ARMED
ENTRY_PENDING
OPEN
EXIT_PENDING
CLOSED_WAIT_REARM
DISABLED
```

Each level owns:

- model/version/hash;
- route direction;
- reference trigger;
- actual two-leg fills and normalized base quantity;
- allocated weight and reserved stress;
- executable entry spread;
- reverse-grid target;
- stop and maximum holding deadline;
- fees, funding, slippage, realised/unrealised PnL;
- rearm boundary and state.

Rules:

1. A level may open once before rearm.
2. The next entry is the first unfilled crossed level, never always level 1.
3. At most one tranche is submitted in one decision cycle.
4. If the market gaps through several levels, process them sequentially only after the previous tranche is `HEDGED`, fresh L2 is obtained, and economics/risk are recalculated.
5. Earlier levels are not enlarged to compensate for a rejected later level.
6. No sixth tranche is permitted.
7. State survives restart exactly; incompatible legacy state blocks entry rather than being reinterpreted.

## 9. Hybrid entry gate

A reference level only arms an entry. A tranche opens only when every gate passes in this order:

1. current state/reconciliation is known and healthy;
2. reference level is crossed in the correct direction;
3. the crossing is present in three fresh synchronized L2 decisions spanning at least 500 ms;
4. the level is eligible and not already filled;
5. both books contain sufficient protected executable depth for the normalized base quantity;
6. actual account fees and all required funding schedules are known;
7. current-regime and historical-convergence gates pass;
8. expected economics pass;
9. route, portfolio, local-margin, liquidation-distance, and effective-leverage risk pass;
10. atomic risk reservation succeeds;
11. the existing durable paired execution coordinator accepts the action.

Use actual L2 VWAP bid/ask for the full proposed tranche. Never use the reference OHLC high/low as an order price.

## 10. Aggressive economics

For each tranche calculate:

```text
stressed_total_cost =
    four-leg actual/private taker fees
  + entry and exit market impact
  + entry and exit slippage
  + latency reserve
  + partial-fill and unmatched-leg reserve
  + emergency-hedge reserve
  + reconciliation/forced-exit reserve
  + funding contribution and funding stress
  + liquidation-distance reserve
```

Funding treatment:

- charge 100% of expected adverse net funding;
- credit only 50% of expected favorable net funding;
- stress adverse funding at 2.0x;
- calculate each venue's events from its own next timestamp and interval;
- unknown or stale fee/funding information blocks entry.

The convergence component excluding favorable funding must remain positive.

Normal entry requires both:

```text
expected_gross_convergence_pnl >= 1.35 × stressed_total_cost
expected_net_pnl                >= 0.15 USDT
```

The minimum-notional canary may use `0.01 USDT` expected net profit solely to validate the live execution path. It does not qualify the normal strategy economics.

## 11. Risk sizing

For a proposed full route nominal `N`, calculate the loss of each weighted tranche from its actual/planned entry to the frozen effective stop, then add all closing and recovery costs:

```text
projected_route_loss(N) =
    weighted spread loss to stop
  + remaining close fees
  + stressed exit impact/slippage
  + stressed funding
  + unmatched-leg/emergency-hedge reserve
  + reconciliation/forced-exit reserve
  + liquidation-distance reserve
```

Choose the maximum rounded common base quantity such that:

```text
projected_route_loss <= 4.50 USDT
projected_portfolio_loss <= 45.00 USDT
```

After each actual fill and every quantity/price rounding, recalculate from actual state. New risk is rejected or reduced if the modelled limits would be exceeded.

Immediate risk reduction starts when either hard boundary is reached:

```text
projected route loss >= 5.00 USDT
projected portfolio loss >= 50.00 USDT
```

The live execution may realise more than the calculated limit during a gap, outage, or failed fill. The system must report this honestly and use the existing emergency paths.

Leverage only determines margin consumption. It never determines position size.

## 12. Reverse-grid exit and rearm

Exit priority is fixed:

1. emergency, unknown, unreconciled, or liquidation-danger state;
2. effective reference stop or hard projected-loss boundary;
3. hard holding deadline;
4. adverse funding that destroys remaining expected profit;
5. tranche reverse-grid target;
6. consideration of another entry level.

For a positive route, define the tranche target:

```text
target_plus[i] = max(
    normal_high,
    actual_entry_spread - max(grid_step, stressed_cost_move + minimum_profit_move)
)
```

For a negative route:

```text
target_minus[i] = min(
    normal_low,
    actual_entry_spread + max(grid_step, stressed_cost_move + minimum_profit_move)
)
```

Close the tranche only from executable L2 exit prices and only through the existing paired close/reconciliation path.

After a tranche is fully closed and stable-FLAT for that allocation, it enters `CLOSED_WAIT_REARM`. It becomes `ARMED` only after the reference spread has retreated inward by at least `0.25 × grid_step`; it may reopen only after a fresh outward recross of its own level and complete re-evaluation.

The hard route holding deadline is 24 hours from the first still-open tranche. Per-level historical convergence p90 may impose an earlier deadline.

## 13. Route selection

Evaluate all eligible directed routes for one base asset and admit only one. Use:

```text
score =
    convergence_probability × expected_net_profit
    ------------------------------------------------
    projected_stress × max(expected_holding_hours, 0.25)
```

Deterministic tie-breaks:

1. greater executable depth;
2. lower total slippage;
3. lower decision/data latency;
4. lower total fee;
5. lower adverse funding;
6. lexical route ID.

A route with non-positive expected net profit, insufficient history, or any unknown critical input is ineligible regardless of score.

## 14. Replay, shadow, and live parity

The same immutable decision input and strategy evaluator must drive:

- deterministic replay;
- simulator;
- real-time shadow;
- laptop live canary/pilot;
- later VPS live.

Adapters may differ; strategy semantics may not. Replay must model conservative ordering when multiple levels, target, and stop are touched inside one minute. For ambiguous OHLC-only ordering, choose the worse result for the strategy unless finer event data proves the sequence.

Every decision emits a stable reason code and a complete numerical breakdown for reference level, executable spread, target, stop, cost, funding, risk, route score, and state transition.

## 15. Laptop-first implementation and verification

### Stage A — software/replay

On the current Windows laptop:

- bind an exact CPython 3.12 environment and dependency lock;
- run all existing verification and security checks;
- build deterministic 1m reference-spread fixtures;
- prove five-level open, catch-up, reverse close, rearm, stop, funding exit, restart, and recovery in replay/simulator;
- prove no production submit occurs.

Do not wait for long qualification while coding; use short synthetic profiles for implementation tests.

### Stage B — live public shadow

Use the existing Windows-native service/S4U workflow to:

- download/resume the required 1m history for a narrow Wave 1 candidate set;
- build the exact reference model;
- run the hybrid strategy on live public BBO/L2/funding data;
- persist qualification bound to code, config, history manifest, route, and runtime hashes.

The existing owner-authorized 12-hour laptop exception may be reused only if its current policy and one-time evidence rules still accept the new exact head. Never shorten below the already authorized policy. Otherwise use the standard 24-hour policy.

### Stage C — minimum live canary

After all independent work is complete, request one owner action for restricted credentials and explicit live consent. The canary remains:

- one base;
- one directed route;
- one tranche;
- minimum valid notional;
- at most 1 USDT hard projected route/portfolio loss;
- exact qualification and all current live gates;
- stable-FLAT and eight hours of post-trade service evidence.

### Stage D — laptop pilot

Risk-stage promotion remains owner-confirmed. The laptop must support the fixed progression:

| Stage | Routes | Tranches per route | Pair hard loss | Portfolio hard loss |
|---|---:|---:|---:|---:|
| canary | 1 | 1 | 1 USDT | 1 USDT |
| pilot_a | 1 | 5 | 5 USDT | 5 USDT |
| pilot_b | 2 | 5 | 5 USDT | 10 USDT |
| wave1_prod | 3 | 5 | 5 USDT | 15 USDT |
| full | 10 | 5 | 5 USDT | 50 USDT |

A stage may advance only after exact evidence, stable-FLAT, no unresolved action, and explicit owner promotion. No automatic risk escalation from canary to full is permitted.

### Stage E — laptop acceptance and VPS block

Create `state/laptop-aggressive-acceptance.json` only when:

- exact software/replay/shadow evidence passes;
- at least one owner-authorized real paired canary completed and stable-FLAT was proven;
- no critical unresolved defect remains;
- the artifact binds exact code, config, strategy profile, history manifest, qualification, Windows runtime manifest, and canary report hashes.

Until that artifact has `accepted=true`, every VPS deploy/bootstrap command and runtime promotion must fail closed with an explicit reason. The current goal may prepare a later handoff but must not deploy to VPS.

## 16. Minimal user interface

Extend the existing CLI/Telegram output only as needed to show:

- strategy profile/version;
- normal state and normal zone;
- positive/negative historical extremes;
- five levels, weights, state, and actual fills;
- effective stop and remaining route/portfolio risk;
- expected/realised fees, funding, slippage, and PnL;
- current qualification and laptop acceptance status;
- exact rejection reason.

Do not add a web UI.

Provide one Windows wrapper, `scripts/laptop-aggressive.ps1`, with modes:

```text
verify
shadow
qualify
canary
pilot
status
stop
```

It must orchestrate the existing scripts/services rather than duplicate their implementations. Modes that require secrets or live consent must fail with one exact owner action when absent.

## 17. Definition of done

### Software Ready

- all existing baseline `B-*`, `PR-*`, and `CR-*` acceptance remains green;
- all new `AS-*` criteria pass;
- `make verify` and required branch checks are green on the exact head;
- deterministic replay proves the full five-level lifecycle and all risk/recovery invariants;
- Windows native `verify`, `shadow`, and qualification workflows work without Docker;
- live remains physically impossible without the existing independent unlock gates;
- no VPS action has occurred.

### Laptop Live Accepted

- one exact owner-authorized minimum-notional paired canary has completed;
- actual four-leg fills, fees, funding, stable-FLAT, restart/recovery behavior, and post-FLAT service evidence are honest and accepted;
- `state/laptop-aggressive-acceptance.json` has `accepted=true` and exact hashes;
- all identified defects have been fixed and reverified on the exact final head;
- VPS remains untouched and is the subject of a later, separate goal.

## 18. Permitted stop conditions for Codex

Codex may stop only when:

1. the complete software-only goal is accepted and the only remaining work requires owner credentials or explicit live-money consent; or
2. laptop live acceptance is complete.

Before an owner action, finish every independent code, test, review, documentation update, and local non-secret verification. Do not stop for ordinary design choices, temporary venue failure, or a long-running qualification that can be launched through the existing durable laptop workflow.
