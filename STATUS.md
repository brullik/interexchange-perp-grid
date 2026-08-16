# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** PHASE3_2_LOCAL_TECHNICAL_PASS_AWAITING_EXACT_GATE
- **Current checkpoint:** Draft PR #5 Phase 3.2 local candidate passed 359 tests and independent dirty-tree adversarial/release review with P0/P1/P2=0; commit, fresh exact-head CI/artifacts, and exact-head review are still required before the checkpoint is complete
- **Live orders:** impossible by default
- **Production credentials:** not present and not requested
- **Current Wave 1:** Binance USD-M, Bybit, OKX
- **Canary route:** none; selected only from a route-specific qualified allowlist
- **C5:** forbidden until the remaining master-plan acceptance gates and external owner actions are complete

## Checkpoints

| Checkpoint | Status | Evidence |
|---|---|---|
| C0 lean bootstrap | COMPLETE | [GitHub Actions run 31835084239](https://github.com/brullik/interexchange-perp-grid/actions/runs/31835084239): `make verify` and Docker health/restart smoke passed on commit `c240ea3` |
| C1 public market vertical slice | COMPLETE | [GitHub Actions run 31837867113](https://github.com/brullik/interexchange-perp-grid/actions/runs/31837867113): Linux `make verify` (29 tests) and Docker health/restart passed on `a790344`; live read-only scan found 656 common instruments and two eligible Binance USD-M/OKX directed BTC routes while Bybit failed closed with `BOOK_SEQUENCE_UNKNOWN`; Parquet/DuckDB replay contained 206 L2 levels across all three venues |
| C2 strategy/risk/simulator | COMPLETE | [GitHub Actions run 31839163485](https://github.com/brullik/interexchange-perp-grid/actions/runs/31839163485): Linux `make verify` (43 tests) and Docker health/restart passed on `0849413`; deterministic tests cover open/add/partial close/full close, profitable and losing four-leg PnL, funding, protected prices, partial/rejected/unknown orders, private staleness, venue outage, third-venue hedge, forced close, and property-based 5/50 USDT risk invariants |
| C3 usable shadow product | COMPLETE | [GitHub Actions run 31840533502](https://github.com/brullik/interexchange-perp-grid/actions/runs/31840533502): Linux `make verify` (54 tests) and Docker continuous-service health/restart passed on `aa3715d`; tests prove live-snapshot calibration/risk/paired simulated fills, restart ledger restore and reconciliation block, overload priority, Telegram owner/challenge audit, integrity-checked backup/restore, retention, and code/config/data-hash qualification |
| C4 live-canary-ready execution | RELEASED_RC1 | PR #1 was squash-merged; annotated tag and prerelease [`v0.1.0-rc1`](https://github.com/brullik/interexchange-perp-grid/releases/tag/v0.1.0-rc1) are published. Fresh publisher run [31896663152](https://github.com/brullik/interexchange-perp-grid/actions/runs/31896663152) published both GHCR tags at immutable digest `sha256:2c3ba72caab2fd2c0e99e6efa3ecdaf8c18b20a8b272d872f75e6094ee8aecc8`; manifest artifact `9249990229` and release asset were independently verified with P0/P1/P2=0 |
| Phase 2 Wave 1 data/private core | COMPLETE | PR #4 was independently verified with P0/P1/P2=0 and squash-merged as `0e87a1e`; post-merge [run 31904798345](https://github.com/brullik/interexchange-perp-grid/actions/runs/31904798345) passed all five jobs |
| Phase 3.1 multi-instrument broad BBO | COMPLETE | Draft PR #5 exact code checkpoint `64e5c86e` and evidence-only head `18ae1b1`; real Wave 1 fixture qualifies one common instrument/six routes without fabricated OKX notional; malformed typed records are isolated through registry, route, and canary sizing; 102-safe-common/608-route synthetic boundary; one watchdog-protected batch watcher per venue; cancellation-safe retirement, idempotent transactional startup, single-flight refresh/recycle, tracked broad and selected-route scans, one bounded lifecycle shutdown barrier, explicit shutdown/teardown failure, cancellation-safe partial-factory rollback, and cancellation-aware two-phase Parquet publication with partial-write and event-loop-shutdown cleanup; six-hour resubscription; jittered 1→30 s reconnect; bounded cache with stale provenance; actual quote-receipt-to-prefilter latency; restart-identical proof; stable non-executable prefilter; exact run 31913700713 and independent P0/P1/P2=0 review passed |
| Phase 3.2 bounded Candidate L2 + public overload admission | LOCAL_TECHNICAL_PASS_AWAITING_EXACT_GATE | Deterministic top-30 QUOTE_READY directed candidates plus every active route; one deduplicated venue-symbol L2 subscription with matching Wave 1 unsubscribe; 100 ms coalescing/debounce; active P2 before candidate P5; broad/history P6 then candidate P5 shedding before P4; P0-P3 preserved; exact BookRegistry quality, venue outage, generation, freshness, and receipt-to-decision p95 checks; bounded tasks/cache/locks under 100k churn; restart/recycle/shutdown proof; `execution_authorized=false`; local 359-test gate plus three independent dirty-tree P0/P1/P2=0 reviews passed. This is PROD-05 COMPLETE at code-candidate level and only a narrow PARTIAL PROD-10 implementation; exact commit/CI/evidence remain pending |
| C5 owner-operated canary | FORBIDDEN | Must not start until corrected C4 passes every P0 criterion and independent review |
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
2026-08-15 — Bind qualification to one exact directed route, release/image/config, immutable Parquet manifest, private fees, 24-hour continuity, three funding checkpoints, persisted shadow statistics, and hashed replay/JUnit evidence — row counts or mutable JSON cannot qualify a canary — `qualification.py`, `state.py`, `shadow.py`, `release_evidence.py`, CLI, CI
2026-08-15 — Recompute all four-leg live economics from private fees, schedule-aware funding, depth impact and explicit recovery reserves; derive IOC caps from the marginal consumed level — canary entry must remain profitable after current full stress — `live_economics.py`, `routes.py`, `private_execution.py`
2026-08-15 — Persist both exact requests and every transition in SQLite WAL before network submission, and resume only the same idempotent action — crashes and unknown acknowledgements must never duplicate a leg or permit a new pair — `live_journal.py`, `live_coordinator.py`, `canary_runtime.py`
2026-08-15 — Treat exchange account/orders/positions as reconciliation truth and require zero open positions plus zero bot orders for terminal FLAT — netting nonzero positions is not flat — `adapters/private.py`, `live_reconciliation.py`, `live_control.py`
2026-08-15 — Select canary direction only from exact qualification evidence and derive the remaining Wave 1 venue for emergency recovery — hard-coded Bybit/OKX direction is unsafe while Bybit sequence is unknown — `config.py`, `canary_runtime.py`, owner runbook
2026-08-15 — Checkout the exact PR head SHA in every CI job and name replay artifacts from that SHA — GitHub's synthetic pull-request merge commit cannot serve as final-head evidence — CI workflow
2026-08-15 — Use one checksummed Wave 1 client-ID namespace for generation, classification, reconciliation, and cancellation — ownership predicates must never disagree or absorb external lookalikes — `client_ids.py`, journal, coordinator, control, adapters
2026-08-15 — Make one long-running service supervisor the sole owner of queued submit, monitoring, restart recovery, and one Telegram poller — durable PREPARED/PARTIAL/HEDGED/CLOSING actions must recover without repeating entry gates or creating a second action — `supervisor.py`, `service.py`, `canary_runtime.py`
2026-08-15 — Bind every qualification observation to an immutable exact route/release/source/config/image epoch and require FINALIZED status — observations from a prior identity must never qualify a new release — `state.py`, `qualification.py`, `shadow.py`, CLI
2026-08-15 — Preserve raw private record completeness and quarantine immutable journal identity/status/fill regressions — normalized views may never silently drop exchange activity or manufacture HEDGED/FLAT — private adapter/domain, journal, reconciliation, coordinator
2026-08-15 — Require a two-snapshot quiet stable-FLAT barrier and make emergency flatten qualification-independent and account-wide on dedicated subaccounts — late fills, unknown orders, and non-route positions must remain recoverable without authorizing entry — reconciliation, control, supervisor
2026-08-15 — Prove every possible main-leg residual executable on the third venue and price protected round-trip depth plus its private fee into stress — emergency feasibility must be admission evidence rather than an assumed reserve — `live_economics.py`, `canary_runtime.py`
2026-08-15 — Pin application/build/security dependencies and bind the exact 30-scenario C4 proof to clean head/source/config/image with a zero-production-submit guard — final-head CI must produce auditable deterministic evidence and a reproducible SBOM — locks, `c4_proof.py`, CI, release scripts
2026-08-15 — Install a native Bybit V5 `u/seq` guard before CCXT mutates its order book and carry explicit `u=1` reset evidence downstream — CCXT Pro's Bybit handler assembles deltas but drops both native sequence fields — `adapters/bybit_v5.py`, `adapters/ccxt_pro.py`, `market_data.py`
2026-08-15 — Replace private per-symbol enumeration with two account-wide calls and mark responses at documented page limits UNKNOWN — bounded request rate must not create a false COMPLETE snapshot — `adapters/private.py`, `private_cache.py`
2026-08-15 — Enforce a two-second private-cache age, 250 ms synthetic p95, serialized hard-deadline reconciliation, and monotonic event watermarks — stale, hung, regressing, cross-venue, or incomplete state must block entry — `private_cache.py`
2026-08-15 — Keep Telegram shadow mode alive without a token while retaining credential failure outside shadow mode — public observation and service health must not depend on private control credentials — `telegram_control.py`
2026-08-15 — Persist received private-event watermarks and buffer cross-channel delivery by global watermark with a bounded authoritative-REST recovery path — delayed or reordered order/position/account events must never create false READY state or unbounded memory — `adapters/private.py`, `private_cache.py`, `state.py`
2026-08-15 — Apply the internal per-minute REST budget only to monitoring and entry gates — cancel, close, restart, and unknown-order recovery must retain priority while venue transport limits and hard deadlines still apply — `private_cache.py`, live reconciliation call sites
2026-08-15 — Interpret Bybit's CCXT `side=None, contracts=0` close update as zero-quantity tombstones for both one-way cached sides — a normal terminal close must remove stale exposure without poisoning the stream — `adapters/private.py`
2026-08-15 — Use venue- and channel-specific account-wide WebSocket params and consume only fresh cached account data — CCXT subscription payloads must match Binance USD-M, Bybit, and OKX contracts without masking stale state — private adapter/cache
2026-08-15 — Reserve the persisted private watermark exclusively for account-wide received events and resolve post-submit order watches from the same bounded cache — submit/cancel and legacy symbol watchers must not create invisible sequence gaps — private adapter/cache/coordinator boundary
2026-08-15 — Detach at most one cancellation-resistant REST fetch after a hard reconciliation deadline and bound stream watcher shutdown — timeout and process shutdown guarantees must not depend on cooperative transport cancellation — `private_cache.py`
2026-08-15 — Preserve sticky WebSocket failure for entry while exposing only the just-completed full REST snapshot to cancel, close, restart, and terminal recovery — stream outage must block risk increase without hiding exchange exposure from risk reduction — `private_cache.py`, live reconciliation
2026-08-15 — Include received private-event watermarks in stable-FLAT signatures and verify the combined journal/private watermark before and after the atomic journal commit — a late cache event must reset the barrier or quarantine the action — private adapter/cache, reconciliation, coordinator, control
2026-08-15 — Return the newest fully applied complete private state to recovery and reject a REST snapshot when the received watermark is still ahead of cache delivery — emergency close must neither wait on storage nor act on a stale flat view — `private_cache.py`
2026-08-15 — Bind each recovery result to its complete REST snapshot and reject it after any newer received event, even when that event is applied — local receipt order has no cross-channel/exchange causality, so composed REST/stream state cannot prove account-wide flatness — `private_cache.py`
2026-08-15 — Validate typed universe/data settings against adjacent locked `RUNTIME_POLICY.yaml` at startup — duplicated operational policy must fail startup on drift rather than silently diverge — config
2026-08-15 — Keep one immutable latest universe with startup, six-hour, and reconnect refresh; reject inactive, future, young, unknown-age live, and ambiguous instruments — broad discovery must remain deterministic and fail closed — `market_universe.py`, public adapter/engine
2026-08-15 — Coalesce broad BBO only into known venue-symbol keys and rank observations without execution authority — 100k bursts and one venue outage must not grow memory, subscribe to L2, or create a trade — `bbo_prefilter.py`, `public_engine.py`, shadow snapshot
2026-08-15 — Require canonical linear-USDT product identity and positive contract metadata at the typed registry and economics boundaries — adapter trust or missing minimum notional must never create a candidate or executable route — domain, universe, routes, live economics
2026-08-15 — Consume incremental batch BBO updates through one long-running watcher per venue and reject per-symbol transport fallback — CCXT `newUpdates` emits changed tickers and broad coverage must remain concurrency-bounded — public adapter/engine
2026-08-15 — Retry quarantined public venues automatically with deterministic exponential backoff capped at 30 seconds — a transient feed failure must fail closed without excluding a recovered venue for six hours — `public_engine.py`
2026-08-15 — Represent OKX's documented absence of a fixed notional floor explicitly and recover Bybit's documented minimum from native metadata — mandatory execution constraints must be known without inventing an OKX value — domain, public adapter, universe, route economics
2026-08-15 — Put a hard progress deadline around each batch BBO receive, retain cancellation-resistant tasks, resubscribe on universe changes, and reset jittered reconnect backoff only after a qualified quote — silent feeds and stale subscriptions must fail closed without duplicate watchers — `public_engine.py`, BBO cache
2026-08-16 — Measure broad prefilter latency from the oldest qualified quote actually ranked and reject malformed runtime metadata types at the registry boundary — scan duration and Python type hints must not create false performance or qualification evidence — public engine, universe tests
2026-08-16 — Fail bounded shutdown explicitly when an adapter transport ignores every cancellation and close request — Python cannot forcibly terminate such a coroutine, so a live retained task must never be reported as successful shutdown — `public_engine.py`
2026-08-16 — Recycle a retired venue adapter only after its old transport is confirmed terminal — reconnect must restore availability without ever running overlapping subscriptions, while an uncloseable transport remains quarantined — `public_engine.py`
2026-08-16 — Serialize retired-adapter recycling per venue and surface completed adapter-close exceptions — concurrent scans must create exactly one replacement and shutdown may never conceal failed teardown — `public_engine.py`
2026-08-16 — Validate minimum-notional runtime types before canary sizing and reason-code invalid metadata — malformed adapter records must fail closed before any submit boundary without a Python type crash — `routes.py`, `canary_runtime.py`
2026-08-16 — Yield one millisecond after a sub-millisecond BBO batch loop — an immediate fake or anomalous transport must not starve the event loop or fabricate latency-gate failures — `public_engine.py`
2026-08-16 — Set the closed barrier before waiting on per-venue recycle locks and recheck it before replacement creation — shutdown racing recovery must close the old adapter without allowing a new adapter to escape teardown — `public_engine.py`
2026-08-16 — Recheck reconnect backoff after acquiring the recycle lock and validate raw Decimal sizing fields before derived arithmetic — concurrent failed recovery and malformed metadata must remain bounded and reason-coded fail-closed — public engine, routes, canary
2026-08-16 — Reject refresh and scan results after shutdown begins and restore quarantine if close races replacement initialization — a closed engine must never report a recovered venue even when the replacement is eventually torn down — `public_engine.py`
2026-08-16 — Version retired-adapter failures under the recycle lock — concurrent explicit reconnect followers may bypass an old backoff but must coalesce a failure created by the leader they waited behind — `public_engine.py`
2026-08-16 — Guard every public startup entry point and serialize initialisation with bounded shutdown — a successfully closed engine must never create, probe, discover, or leak a new exchange transport — `public_engine.py`
2026-08-16 — Own startup, forced refresh, reconnect, and shutdown under one idempotent transactional lifecycle — concurrent cold scans, repeated initialisation, partial factory failure, and late transport completion must neither duplicate ownership nor mutate a closed engine — `public_engine.py`
2026-08-16 — Synchronize BBO watcher creation with shutdown and discard transport results after the closed barrier — a late or cancellation-resistant public stream must not update cache, reconnect state, or quarantine after teardown — `public_engine.py`
2026-08-16 — Track complete broad and selected-route scans in the bounded shutdown contract and retain rollback closers before awaiting them — funding/L2 work and cancellation during factory cleanup must never outlive ownership or resume on closed adapters — `public_engine.py`
2026-08-16 — Give the private-event stable-FLAT race test scheduler margin without changing production policy or assertions — exact CI evidence must not fail on a 100 ms wall-clock runner jitter while still proving barrier reset — private cache test
2026-08-16 — Cancel overdue public scans before returning shutdown failure and publish Parquet only after a cancellable staging phase — a recorder worker may finish late but must leave no visible market-history commit after `close()` returns — public engine, history recorder
2026-08-16 — Share pending-file ownership between the event loop and Parquet staging thread and register it before writing — loop shutdown, cancellation, and partial writer failure must remove every unpublished staging artifact — history recorder
2026-08-16 — Bind Phase 3.1 checkpoint evidence to the last exact independently reviewed code head — status must distinguish a proven technical checkpoint from its subsequent evidence-only commit — status
2026-08-16 — Keep one bounded event-driven L2 owner per venue-symbol and gate public work by P2/P5/P6 priority without execution authority — active routes must survive overload while candidate churn, stale data, unsubscribe, reconnect, and adapter-generation races remain fail-closed — candidate L2, public engine, public adapters, shadow evaluator

## Active blockers / owner actions

### Repository visibility

The repository is PUBLIC. `OWNER_ACTION.json` contains the exact separate action required to make it PRIVATE. No credential or operational evidence may be committed while this remains unresolved.

## Last verified command

```text
2026-08-16 Phase 3.2 local Windows equivalent of every Makefile verify target: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (90 files)
- mypy --strict: PASS (88 source/test files)
- pytest: 359 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false

The local Phase 3.2 candidate was independently reviewed on stable file hashes by adversarial,
release, and gap auditors with P0=0, P1=0, and P2=0. PROD-05 is implemented; PROD-10 is
PARTIAL only. A checkpoint commit, fresh exact-head CI/artifacts, and exact-head all-zero review
remain required before completion or any Ready/merge transition.

GNU make is not installed on this Windows host. Exact Linux `make verify`, Docker smoke,
No production credentials were used, Candidate L2 keeps `execution_authorized=false`, and no real
order was submitted.
```
