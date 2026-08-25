# Aggressive Symbiosis V1 — executable acceptance

Acceptance is executable. Prose, screenshots, invented values, an unobserved long run, or a green check from another commit are not evidence. All evidence must be bound to the exact code/config/profile/data/runtime identity under review.

Every pre-existing baseline test and protected-branch check must remain green. The criteria below retain the original product/safety outcomes and add the aggressive symbiosis outcomes.

## Baseline (`B-*`)

- **B-01** — Python 3.12 install and `make verify` pass in CI and locally/Windows-equivalent.
- **B-02** — default configuration is `shadow`; every default/test/CI live-guard evaluation denies orders with a reason code.
- **B-03** — invalid risk relationships, unsupported products, non-finite/negative values, or missing safety fields fail startup.
- **B-04** — the existing service starts, reports health, survives supported restart, and shuts down cleanly.
- **B-05** — no repository file, log, fixture, prompt, screenshot, or evidence contains an actual credential; runtime secret/state paths remain ignored.
- **B-06** — the application exposes no withdrawal, transfer, wallet, address-book, or API-key-management operation.
- **B-07** — current required branch checks and security scans remain green on the exact PR head.

## Preserved market/execution product (`PR-*`)

- **PR-01** — venue adapters remain behind the typed boundary; domain/strategy code contains no raw exchange payloads.
- **PR-02** — fixtures prove exact matching of normalized linear USDT perpetual contracts and rejection of inverse, dated, spot, USDC, ambiguous, delisting, and contract-version-mismatched products.
- **PR-03** — at least two available Wave 1 venues can stream fresh public data; an unavailable venue is quarantined without stopping risk reduction elsewhere.
- **PR-04** — L2 books detect freshness, sequence, reconnect, generation, and clock faults and block entry until resynchronized.
- **PR-05** — executable VWAP honors depth, lot/tick steps, multipliers, minimum notional, and actual requested tranche size.
- **PR-06** — directed calculations distinguish A-long/B-short from B-long/A-short and link both to one canonical reference pair.
- **PR-07** — funding schedule/rate, fee source, data quality, and depth are present in every accepted decision; unknown required inputs reject entry.
- **PR-08** — public history is stored in integrity-checked Parquet and queried/replayed deterministically through DuckDB.
- **PR-09** — each tranche owns actual two-leg fills, normalized base quantity, costs, target, stop, risk, model identity, and lifecycle state.
- **PR-10** — four-leg PnL, funding, fees, slippage, partial close, full close, and losing cycles are numerically correct.
- **PR-11** — max ten normal routes, one route per base, five tranches per route, >=20% local stressed free margin, and <=3x initial effective leverage are enforced.
- **PR-12** — partial fill, rejected leg, unknown result, stale private stream, venue outage, emergency hedge, forced close, and process restart have deterministic recovery tests.
- **PR-13** — normal execution always carries a worst acceptable price/slippage cap; unbounded market execution is emergency-only.
- **PR-14** — restart with open simulated/live journal state restores, reconciles, and blocks new entries until consistent.
- **PR-15** — overload preserves close/hedge/private/reconciliation priority and disables new entry first.
- **PR-16** — every evaluated signal emits a stable reason code and numerical input/economics/risk breakdown.
- **PR-17** — Telegram owner allowlisting, pause/resume/kill/status and dangerous-command challenge remain authenticated and audited.
- **PR-18** — production submit count is zero in every software-only CI/replay/shadow proof.

## Live safety baseline (`CR-*`)

- **CR-01** — existing Bybit/OKX private execution and Binance alternate contract suites remain green.
- **CR-02** — account mode, symbol, permissions, fee, margin, position mode, clock, API availability, emergency venue, and raw private state are preflighted.
- **CR-03** — idempotent submit and unknown-result reconciliation cannot create duplicate/reversing orders.
- **CR-04** — actual fill quantity, not requested quantity, drives hedge, journal, risk, and closure.
- **CR-05** — YAML/env changes alone cannot activate live orders.
- **CR-06** — CI/test/replay/shadow, stale or wrong-hash qualification, missing local unlock, absent owner challenge, wrong stage, nonallowlisted route, or unknown state always deny live.
- **CR-07** — canary remains one base, one route, one tranche, minimum valid notional, and hard projected risk <=1 USDT.
- **CR-08** — emergency close and third-venue hedge remain proven under first/second venue faults.
- **CR-09** — stable-FLAT is required before risk/action ownership is released.
- **CR-10** — a restart adopts the same durable action and never requires or permits an unsafe duplicate submit.

## Reference source bars (`AS-DATA-*`)

- **AS-DATA-01** — the canonical pair order is stable and independent of current price; aliases/multipliers are normalized before pairing.
- **AS-DATA-02** — only closed bars with identical UTC minute start and compatible contract metadata are joined.
- **AS-DATA-03** — exact formulas are tested:
  - `open = 10000*ln(open_A/open_B)`;
  - `high = 10000*ln(high_A/low_B)`;
  - `low = 10000*ln(low_A/high_B)`;
  - `close = 10000*ln(close_A/close_B)`.
- **AS-DATA-04** — precision and rounding are deterministic and use `Decimal` or an explicitly equivalent fixed-point implementation; float cannot enter risk/model decisions.
- **AS-DATA-05** — a missing, incomplete, non-finite, non-positive, ambiguous duplicate, unsynchronized, or contract-mismatched minute is rejected; no forward-fill occurs.
- **AS-DATA-06** — 5m/15m/1h/4h/1d reference bars are created only from completed 1m reference bars using first-open/max-high/min-low/last-close.
- **AS-DATA-07** — any missing constituent minute marks the higher interval `INCOMPLETE` and excludes it from the model.
- **AS-DATA-08** — an automated test fails if code attempts to build a reference 1h/4h/1d bar directly from exchange higher-timeframe OHLC.
- **AS-DATA-09** — history acquisition is resumable, idempotent, bounded, rate-limit aware, candidate/on-demand, and restart-safe.
- **AS-DATA-10** — identical source input/config/code produces identical normalized rows, reference rows, ordering, serialized values, and hashes across two runs.
- **AS-DATA-11** — the output explicitly identifies synthetic high/low as a non-executable envelope; no entry can be accepted without fresh L2/VWAP confirmation.
- **AS-DATA-12** — a CLI proof returns coverage, rejected-minute counts/reasons, source/reference hashes, and interval completeness without private credentials or orders.

## Historical/reference model (`AS-MODEL-*`)

- **AS-MODEL-01** — target history is 180 complete days; live entry requires >=90 days; 30–89 days is shadow-only; <30 days disables the direction.
- **AS-MODEL-02** — `S0` is the 1-bps modal close bucket with tie-breaks: nearest exact median, smaller absolute value, then smaller numeric value.
- **AS-MODEL-03** — normal-zone half-width is `max(2 bps, q10(abs(close-S0)))` and is deterministic.
- **AS-MODEL-04** — positive and negative directions use separate valid extremes/ranges and may be independently enabled/disabled.
- **AS-MODEL-05** — `H_plus` comes from maximum valid 1m reference high and `H_minus` from minimum valid 1m reference low; rejected data carries explicit reason evidence.
- **AS-MODEL-06** — an episode begins only after normal-zone reset and first-level reach; it closes at normal-zone return, 24h censoring, or unavailable/incomplete data.
- **AS-MODEL-07** — normal live eligibility requires >=10 historical episodes and >=70% convergence within 24h for that direction.
- **AS-MODEL-08** — per-level sample count, convergence time, adverse excursion, and censoring are persisted and replayable.
- **AS-MODEL-09** — current 24h/7d/30d median/MAD/quantile statistics remain a regime/long-tail guard.
- **AS-MODEL-10** — new entry is blocked when the 7d median drift exceeds both 25% of the historical directional range and 3 current robust sigmas.
- **AS-MODEL-11** — after first tranche, S0/zone/extremes/levels/weights/stop/economics/model identity are immutable until route stable-FLAT.
- **AS-MODEL-12** — a live model cannot move any bounded strategy parameter more than 20% per day.
- **AS-MODEL-13** — model evidence contains exact code SHA, strategy profile hash, source/reference manifest hashes, canonical pair, directed route, contract metadata versions, and time window.
- **AS-MODEL-14** — legacy or partial persisted model data migrates with a deterministic tested schema or fails closed; missing values are never guessed.

## Five-level grid (`AS-GRID-*`)

- **AS-GRID-01** — positive and negative levels equal 20/40/60/80/100% of their own S0-to-extreme range.
- **AS-GRID-02** — tranche weights equal exactly 10/15/20/25/30% and sum to 100% under deterministic rounding.
- **AS-GRID-03** — reference stop is exactly 15% beyond the historical directional range.
- **AS-GRID-04** — effective stop is the farther applicable reference/adaptive-tail boundary; a farther stop only reduces size and cannot expand route risk.
- **AS-GRID-05** — each of five levels persists one of the defined lifecycle states and survives restart unchanged.
- **AS-GRID-06** — the evaluator selects `first_unfilled_crossed_level`; a regression test detects any implementation that always selects E1.
- **AS-GRID-07** — remaining above E1 cannot open E2 unless E2 is reached; a level cannot open twice before re-arm.
- **AS-GRID-08** — a gap across N levels creates at most one tranche in a decision cycle; every next catch-up tranche requires new books, economics, risk, and persisted confirmation.
- **AS-GRID-09** — no sixth tranche, hidden averaging order, or unowned residual position can be created.
- **AS-GRID-10** — actual two-leg fills and normalized base quantity are owned by the exact level/tranche, not only by an aggregate route.
- **AS-GRID-11** — deeper tranches have deterministic reverse-grid targets; the first tranche may target the normal zone.
- **AS-GRID-12** — a closed level rearms only after retreat >=0.25 grid step and a new outward crossing; a stationary spread cannot churn repeated opens.
- **AS-GRID-13** — restart during every level state is tested for idempotent continuation or fail-closed quarantine.
- **AS-GRID-14** — a deterministic replay demonstrates open 1→5, reverse partial exits, re-arm, second oscillation, and stable-FLAT.

## Hybrid entry and economics (`AS-ECON-*`)

- **AS-ECON-01** — an entry requires both the reference level trigger and a fresh executable L2/VWAP opportunity for the exact tranche size.
- **AS-ECON-02** — reference high/low alone can never open a simulated or live tranche.
- **AS-ECON-03** — normal `stressed_cost_multiplier` is exactly 1.35 and normal minimum expected net profit is exactly 0.15 USDT.
- **AS-ECON-04** — 0.01 USDT minimum profit is accepted only under the immutable canary stage; no normal/pilot route may inherit it.
- **AS-ECON-05** — expected favorable funding contributes only 50%; adverse expected funding contributes 100%; adverse stress uses 2x.
- **AS-ECON-06** — gross convergence after four-leg fees/impact, excluding favorable funding credit, must remain positive.
- **AS-ECON-07** — all four fees, entry/exit impact, latency, partial-fill, emergency hedge, reconciliation/forced-exit, funding, and liquidation-distance reserves are included.
- **AS-ECON-08** — actual private taker fees are used when available; unknown fee, funding schedule, depth, freshness, or contract metadata blocks entry.
- **AS-ECON-09** — economics is recalculated after lot/step rounding and after every actual fill.
- **AS-ECON-10** — route selection score follows the profile formula and every tie resolves in the documented deterministic order.
- **AS-ECON-11** — every rejection exposes all numerical terms needed to reproduce the decision.

## Risk and exits (`AS-RISK-*`)

- **AS-RISK-01** — normal route sizing admits at most 4.50 USDT modelled loss at effective stop including all reserves.
- **AS-RISK-02** — hard projected route loss never exceeds 5.00 USDT after any accepted action.
- **AS-RISK-03** — normal aggregate admission never exceeds 45 USDT and hard projected portfolio loss never exceeds 50 USDT.
- **AS-RISK-04** — the 0.50/5.00 reserves cannot be allocated as normal position size.
- **AS-RISK-05** — weighted sizing uses exact 10/15/20/25/30 levels and the effective stop; size is reduced/level skipped when residual risk is insufficient.
- **AS-RISK-06** — quantity rounding, min notional, actual fees/fills, adverse funding, and changed close depth trigger immediate risk recomputation.
- **AS-RISK-07** — hard stop/reference stop/projected-loss exit is executable in replay, real-time shadow, canary/live supervisor, and restart recovery.
- **AS-RISK-08** — exit priority is: emergency/unknown, hard loss/stop, 24h time, adverse funding, reverse target, then new entry.
- **AS-RISK-09** — the stop is never moved farther after first tranche and no new tranche is admitted once an exit condition is active.
- **AS-RISK-10** — 24h hard hold is measured from the first still-open route tranche and closes all remaining tranches.
- **AS-RISK-11** — adverse upcoming funding closes/reduces before the event when remaining expected net economics becomes non-positive.
- **AS-RISK-12** — protected IOC, actual-fill reconciliation, residual correction, emergency hedge/flatten, local margin floor, leverage cap, and stable-FLAT are not weakened.
- **AS-RISK-13** — property tests cover randomized levels, prices, fees, funding, rounding, partial fills, and gaps while preserving route/portfolio invariants.
- **AS-RISK-14** — a price gap or unavailable venue may produce realised loss above 5 USDT in simulation; the system reports this honestly and executes recovery rather than claiming a guarantee.

## Replay/shadow/live parity (`AS-PARITY-*`)

- **AS-PARITY-01** — replay, shadow, and live consume one shared immutable strategy decision result; live does not reimplement a simplified entry rule.
- **AS-PARITY-02** — identical normalized event streams and state produce identical signal/level/economics/risk decisions in two independent runs.
- **AS-PARITY-03** — when intraminute order of level/target/stop cannot be known, replay chooses the documented worst valid outcome.
- **AS-PARITY-04** — process-kill/restart is tested at reference ingestion, model write, every level state, risk reservation, submit states, closing, and stable-FLAT.
- **AS-PARITY-05** — unresolved write/order/private-state outcomes retain ownership and block new risk; no optimistic retry occurs.
- **AS-PARITY-06** — qualification evidence includes all five levels, weights, targets, stops, historical/reference hashes, profile hash, and current runtime identity.
- **AS-PARITY-07** — changing code, profile, model, reference data, contract metadata, route direction, runtime manifest, or qualification identity invalidates live eligibility.
- **AS-PARITY-08** — all existing C4/replay/security/docker proofs remain exact-head green with zero production submits.

## Windows laptop-first acceptance (`AS-LAPTOP-*`)

- **AS-LAPTOP-01** — native Windows CPython 3.12 is a supported first runtime; Docker is not required for laptop acceptance.
- **AS-LAPTOP-02** — one `scripts/laptop-aggressive.ps1` wrapper exposes `verify`, `shadow`, `qualify`, `canary`, `pilot`, `status`, and `stop` by composing existing scripts rather than replacing their security model.
- **AS-LAPTOP-03** — `verify` proves exact interpreter/dependency/source/config/profile identity and runs Windows-equivalent tests without private credentials.
- **AS-LAPTOP-04** — `shadow` uses live public data and the aggressive evaluator, but production submit count remains zero and live flags remain false.
- **AS-LAPTOP-05** — qualification binds code/config/profile/reference manifests/native runtime and the exact route; mismatches invalidate it.
- **AS-LAPTOP-06** — the existing owner-authorized 12h laptop exception may be reused only if its exact current policy checks pass; no code path shortens it further or broadens it to VPS/future qualifications.
- **AS-LAPTOP-07** — API credentials remain local DPAPI/S4U protected, no-withdrawal restricted, and absent from Git/Codex/logs/evidence.
- **AS-LAPTOP-08** — real canary requires local owner consent, local unlock, Telegram challenge, accepted exact qualification, one route, one tranche, minimum valid notional, and hard projected <=1 USDT.
- **AS-LAPTOP-09** — canary success requires exchange-verified filled open/close legs, reconciled fees/funding, stable-FLAT, zero active action, and the existing post-FLAT service evidence.
- **AS-LAPTOP-10** — after successful canary, `pilot_a` can operate one route with all five levels and hard route/portfolio <=5 USDT only after a separate owner stage confirmation.
- **AS-LAPTOP-11** — any failed/unknown canary or pilot returns to shadow/live=false, retains durable action ownership, and follows existing recovery; it cannot fabricate acceptance.
- **AS-LAPTOP-12** — accepted laptop completion creates `state/laptop-aggressive-acceptance.json` with `accepted=true`, exact merged SHA/profile/config/model/reference/runtime/qualification/canary/pilot hashes, stable-FLAT, and >=28,800 post-FLAT service seconds.
- **AS-LAPTOP-13** — the acceptance artifact is ignored by Git and contains no secret; a verifier rejects editing, stale hashes, another machine/runtime identity, incomplete live evidence, or a nonaccepted status.
- **AS-LAPTOP-14** — every VPS upload/deploy/qualification/live entry point for this strategy fails closed unless the verified accepted laptop artifact and exact merged release identity are supplied.
- **AS-LAPTOP-15** — software-only completion creates one precise owner action for credentials/real-money consent; it does not ask the owner to choose implementation details.

## Delivery (`AS-DELIVERY-*`)

- **AS-DELIVERY-01** — exactly one feature branch and one PR contain the software implementation; no parallel planning PRs or documentation-only milestones are created.
- **AS-DELIVERY-02** — `STATUS.md` history is preserved and only its current-state section plus concise decisions/checkpoint evidence are updated.
- **AS-DELIVERY-03** — independent review has P0=0, P1=0, P2=0 and all material threads resolved on the exact head.
- **AS-DELIVERY-04** — the PR is squash-merged only after every current protected check is green and expected head SHA is unchanged.
- **AS-DELIVERY-05** — no VPS is modified in this goal; only a fail-closed handoff command/runbook may be prepared after laptop acceptance support exists.

## Definition of done

### Software ready

All baseline, AS-DATA, AS-MODEL, AS-GRID, AS-ECON, AS-RISK, AS-PARITY, AS-LAPTOP software-only, and AS-DELIVERY criteria pass on the exact merged head. Public-data shadow works natively on Windows. Live remains disabled. One owner action may remain for restricted credentials and explicit real-money consent.

### Laptop live accepted

The minimum canary and one-route five-level laptop pilot have honest accepted evidence, stable-FLAT, and the required post-FLAT service interval. `state/laptop-aggressive-acceptance.json` verifies successfully. Only then may a separate future goal deploy the exact accepted release to a VPS.
