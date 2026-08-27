# Interexchange Perpetual Grid — Aggressive Fast Live V2

## 1. Mission

Create the shortest practical path from the current repository to a laptop-tested live interexchange convergence trader.

The product combines:

1. synchronized 1-minute OHLC reference-spread bars;
2. long-horizon normal state and positive/negative extremes;
3. a five-level back-loaded grid;
4. current 24h/7d/30d robust regime statistics;
5. executable L2/VWAP prices, actual fees, funding schedules, market impact, and slippage;
6. the existing protected paired execution, journal, reconciliation, recovery, Telegram, and Windows security foundation.

The strategy is intentionally more aggressive than the current adaptive-only path, but route and portfolio projected-loss limits remain unchanged.

This is statistical convergence trading. Profit and maximum realized loss are not guaranteed.

## 2. Long-running qualification is removed

The previous time/sample-based qualification is not part of this product contract.

No live decision may require or inspect:

- a qualification epoch;
- 12 or 24 hours of elapsed observation;
- 10,000 synchronized observations;
- a minimum number of funding checkpoints;
- a qualification acceptance file;
- a qualification hash or qualification age.

Old qualification evidence has no authority: it cannot enable live and cannot block live.

The replacement is a bounded, immediate `FAST_LIVE_PREFLIGHT`. It checks the exact current code, strategy profile, runtime, account, route, market data, economics, and risk. It has no accumulation period. It returns PASS or a precise fail-closed reason during the current invocation and expires after 10 minutes or immediately after any relevant change.

This removal does not disable essential trading controls. Freshness, sequence, depth, fees, funding, margin, FLAT, unknown-order, risk, paired-execution, local unlock, Telegram challenge, and explicit owner-consent checks remain mandatory.

## 3. Preserve the existing foundation

Reuse and regression-test:

- the typed exchange adapter boundary;
- broad BBO and candidate/open-route L2;
- protected aggressive taker IOC orders with caps;
- actual-fill-driven hedge and ledger state;
- idempotent client order IDs;
- durable SQLite WAL journal and restart recovery;
- unknown-result reconciliation;
- residual-delta top-up/reduction and third-venue emergency hedge;
- emergency flatten and stable-FLAT;
- atomic risk reservation;
- Telegram owner authentication and challenge;
- Windows DPAPI/S4U secret handling and native runtime manifest;
- current CI/security/C4 proof suites and protected-main checks.

Make only narrow changes required by the new strategy and activation path.

## 4. Fixed owner limits

| Parameter | Requirement |
|---|---|
| Reference capital | 500 USDT |
| Modelled route loss for sizing | <=4.50 USDT |
| Hard projected route loss | <=5.00 USDT |
| Modelled portfolio admission | <=45.00 USDT |
| Hard projected portfolio loss | <=50.00 USDT |
| Routes | <=10 |
| Routes per base asset | 1 |
| Tranches per route | 5 |
| Contracts | Linear USDT-settled perpetuals only |
| Position | Paired long/short only |
| Margin | Cross, bot-dedicated account/subaccount |
| Local free margin | >=20% after stress |
| Initial effective leverage | <=3x |
| Holding | Dynamic, hard cap 24h |
| Withdrawals/transfers | Forbidden |
| First live runtime | Native Windows laptop |
| VPS | Blocked until accepted laptop artifact |

## 5. Reference-spread bars

### 5.1 Input

Use only closed, synchronized 1-minute OHLC bars with UTC minute identity `[t, t+60s)`.

No forward-fill. If either venue lacks the minute, the spread minute is invalid. A higher interval containing a missing required minute is `INCOMPLETE` and non-tradeable.

### 5.2 Canonical venue order

For each unordered venue pair, assign `A/B` by stable lexical venue ID. The order never changes with price.

### 5.3 Formula

For normalized prices:

```text
Open  = 10000 × ln(Open_A / Open_B)
High  = 10000 × ln(High_A / Low_B)
Low   = 10000 × ln(Low_A / High_B)
Close = 10000 × ln(Close_A / Close_B)
```

These are deterministic synthetic bar bounds. They are not proof of simultaneous executable intraminute prices.

### 5.4 Aggregation

Build 5m, 15m, 1h, 4h, and 1d only from completed 1m spread bars:

```text
Open_T  = first Open_1m
High_T  = max High_1m
Low_T   = min Low_1m
Close_T = last Close_1m
```

Direct calculation from exchange 1h/1d candles is forbidden.

## 6. Historical model

Target backfill is 180 days. The fastest laptop live path may use 30 complete days for the first route; 90 days are required before Wave 1 production expansion and 180 days remain the target.

For each direction separately calculate:

- normal value: mode of 1 bps buckets, with deterministic median tie-break;
- normal zone: at least ±2 bps and otherwise the configured low quantile of absolute distance;
- positive and negative valid extremes;
- q99/q99.9 long-tail values;
- completed divergence episodes;
- convergence probability within 24h;
- p90 convergence time;
- adverse excursion;
- 24h/7d/30d median, MAD, depth, funding, and regime flags.

First laptop live eligibility requires at least:

- 30 complete days;
- 10 completed episodes in the traded direction;
- at least 70% convergence within 24h;
- no active regime-drift block;
- valid current L2 economics.

These are historical-model conditions, not a time-based live qualification run.

## 7. Five-level aggressive grid

Let `S0` be the normal value and `H` the validated directional extreme.

```text
E1 = S0 + 0.20 × (H-S0)
E2 = S0 + 0.40 × (H-S0)
E3 = S0 + 0.60 × (H-S0)
E4 = S0 + 0.80 × (H-S0)
E5 = S0 + 1.00 × (H-S0)
```

Use the mirrored formula for the negative direction.

Tranche weights:

```text
10% / 15% / 20% / 25% / 30%
```

Rules:

- one level opens once per arm cycle;
- level N cannot open before its threshold is crossed;
- a gap through several levels is processed sequentially, one tranche per decision cycle;
- each catch-up tranche requires new fresh books, new economics, and new risk calculation;
- the model is frozen after the first open tranche until the route is fully FLAT;
- no sixth tranche;
- no moving the stop farther after entry.

## 8. Stop and exit

Reference stop buffer is 15% beyond the validated extreme. Effective stop is the farther protective boundary of the reference stop and current adaptive long-tail stop.

Stop is executable in replay, shadow, canary, and pilot. It is not only a sizing assumption.

Exit priority:

1. emergency/unknown state;
2. hard projected loss or route stop;
3. 24h hard hold;
4. adverse funding makes remaining trade non-positive;
5. tranche reverse-grid target;
6. next entry level.

Reverse-grid exit closes deeper tranches first. A closed level re-arms only after a retreat of at least 0.25 grid step and a new crossing.

## 9. Economics

Normal entry requires:

```text
expected_gross_pnl >= 1.35 × stressed_total_cost
expected_net_pnl   >= 0.15 USDT
```

Canary may use `0.01 USDT` minimum net profit only to validate the execution path.

Cost includes:

- all four trading fees;
- entry/exit L2 market impact;
- slippage;
- latency reserve;
- partial-fill and emergency-hedge reserve;
- reconciliation/forced-exit reserve;
- expected and stressed funding.

Positive funding contributes at 50%. Adverse funding contributes at 100% and is stressed at 2x. Convergence PnL excluding positive funding must remain positive.

Unknown fee, funding schedule, metadata, depth, or data quality blocks entry.

## 10. Risk sizing

Normal route size is the maximum normalized common base quantity satisfying:

```text
spread loss to effective stop
+ four-leg fees
+ entry/exit impact and slippage
+ adverse funding stress
+ residual-delta/emergency/reconciliation reserves
<= 4.50 USDT
```

The hard projected route check remains `<=5.00 USDT` after actual fills and before every new tranche. Portfolio equivalents are 45/50 USDT.

Configured exchange maximum leverage never determines size.

## 11. FAST_LIVE_PREFLIGHT

A preflight is run immediately before canary/pilot and is bound to:

- exact clean merged source SHA;
- exact config and strategy-profile hashes;
- exact native Windows runtime manifest;
- selected route and direction;
- contract metadata and normalized quantity;
- current account identities and modes;
- current private/public data generations;
- current risk stage.

It must verify:

1. exact-head required CI/proof checks are green;
2. source tree is clean and profile/config are valid;
3. app contains no withdrawal/transfer capability;
4. live defaults remain false and config alone cannot unlock orders;
5. restricted credentials load locally without disclosure;
6. both entry venues pass private capability/account/fee/position/margin/order preflight;
7. emergency venue capability is known or entry is blocked;
8. accounts are exchange-verified FLAT with zero open orders and no unknown journal action;
9. clocks, BBO, L2, sequence, freshness, depth, funding, and metadata are valid;
10. the 1m history/model requirements for the selected direction pass;
11. current executable economics pass for the proposed tranche;
12. projected route/portfolio/margin/leverage limits pass;
13. owner challenge and local unlock are still absent at preflight time; preflight itself never authorizes money.

Output: ignored `state/fast-live-preflight.json` with PASS/FAIL, reason codes, exact hashes, route, timestamps, and numerical breakdown.

Validity:

- maximum 600 seconds;
- invalid immediately on code/config/profile/runtime/route/account/data-generation/risk-stage change;
- single-use for one canary or pilot entry intent;
- never stored in Git.

## 12. Laptop live ladder

### 12.1 Canary

Requires a current PASS preflight plus:

- local owner unlock secret;
- Telegram short-lived challenge;
- exact phrase confirming real-money canary;
- one route;
- one tranche;
- minimum valid notional;
- hard projected route/portfolio loss <=1 USDT.

The existing supervisor owns submission and recovery. Success is exchange-verified stable-FLAT with zero active actions, no unknown order, complete actual fills/fees/funding, and no critical defect. No multi-hour post-FLAT wait is required.

### 12.2 Pilot A

After successful canary, require a new PASS preflight and a separate owner confirmation.

Pilot limits:

- one route;
- up to five tranches;
- hard projected route/portfolio loss <=5 USDT;
- exact aggressive profile;
- normal 1.35x/0.15 economics.

Live pilot need not artificially force all five levels. Deterministic replay must prove the full five-level and reverse-exit path; live pilot must complete at least one genuine paired round-trip under the real strategy and finish stable-FLAT.

After successful pilot, create ignored `state/laptop-fast-live-acceptance.json` immediately. No 8-hour post-FLAT requirement.

## 13. Windows wrapper

Create one `scripts/laptop-fast-live.ps1` that composes existing secure scripts and exposes:

```text
verify
onboard
preflight
canary
pilot
status
stop
```

There is no `qualify` command and no scheduled qualification task.

The wrapper must:

- use the existing exact Python 3.12 native environment;
- preserve DPAPI/S4U secret protection;
- keep Windows Time and sleep prevention checks where required;
- never print secrets;
- reset `IPEG_MODE=shadow` and `IPEG_LIVE_ENABLED=false` in `finally`;
- leave emergency controls available;
- write machine-readable evidence.

## 14. Runtime removal work

Codex must find and remove active live dependencies on the old subsystem from:

- configuration and runtime policy;
- live guard and canary runtime;
- risk-stage promotion;
- autonomous orchestrator;
- CLI arguments and commands;
- Windows scripts and scheduled tasks;
- status/owner runbook claims;
- tests and fixtures.

Database compatibility may retain nullable legacy columns/tables to avoid destructive migration. They must be ignored by the live decision path. Old artifacts must neither grant nor deny live.

## 15. Laptop acceptance and VPS gate

`state/laptop-fast-live-acceptance.json` requires:

- `accepted=true`;
- exact merged SHA;
- exact profile/config/runtime/history/model/preflight hashes;
- successful real canary report;
- successful real pilot report;
- exchange-verified stable-FLAT;
- zero active/unknown actions and positions;
- P0=0 and P1=0;
- no production submit outside owner-confirmed canary/pilot.

VPS upload/deploy remains physically blocked without this artifact and exact release identity.

## 16. Definition of done

Software is done when:

- one PR is merged on exact green required checks;
- old duration/sample qualification cannot affect live;
- 1m spread bars and higher aggregation are deterministic;
- five-level persistent grid, stop, reverse exit, and re-arm work in replay/shadow/live decision core;
- fast preflight is implemented and tested;
- Windows wrapper is ready;
- all existing execution/recovery/security proofs remain green;
- no secrets or withdrawal/transfer code exist.

The independent coding goal may stop only before local credentials and explicit real-money consent, after all other work is complete.

## 17. Explicit non-goals

- waiting 12 or 24 hours before live;
- accumulating a required snapshot/funding-checkpoint count;
- a second orchestration framework;
- a web UI, microservices, Redis, Kafka, Celery, or Kubernetes;
- seven complete venue expansions before the first Wave 1 live route;
- forced unprofitable canary orders;
- weakening pair/portfolio/margin/execution/reconciliation controls;
- VPS deployment before laptop acceptance.
