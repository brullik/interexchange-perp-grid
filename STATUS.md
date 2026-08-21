# Implementation status

This is the only mutable project-status document.

## Current state

- **State:** SOFTWARE_COMPLETION_REMEDIATION
- **Current checkpoint:** protected `main` `384d0292f7654c9be9a6b2bc4a2c36abfb5065fc` includes the exact-reviewed durable 10-route × 5-tranche ownership remediation and remains a green, fail-closed software candidate; the earlier `SOFTWARE_COMPLETE` claim remains withdrawn. The current local checkpoint continuously enables all seven configured venues for capability-gated public observation while preserving immutable Wave 1 live authority; it requires an exact commit, fresh CI/artifacts, and independent exact-head review. Remaining independent software work is exposure-safe upgrade and complete owner-onboarding/canary handoff
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
| Phase 3.2 bounded Candidate L2 + public overload admission | COMPLETE | Exact code checkpoint `f4d1f3e` and evidence head `de3a870`; deterministic top-30 QUOTE_READY directed candidates plus every active route; one deduplicated venue-symbol L2 subscription with matching Wave 1 unsubscribe; 100 ms coalescing/debounce; active P2 before candidate P5; broad/history P6 then candidate P5 shedding before P4; P0-P3 preserved; exact BookRegistry quality, venue outage, generation, freshness, and receipt-to-decision p95 checks; bounded tasks/cache/locks under 100k churn; restart/recycle/shutdown proof; `execution_authorized=false`; exact runs 31918092230 and 31918436474 plus four exact-head artifacts passed; independent final review P0=0/P1=0/P2=0. PROD-05 is COMPLETE; PROD-10 remains only narrow PARTIAL |
| Phase 3.3 persistent public-shadow adaptive calibration | COMPLETE | Exact code head `45e41c3` and evidence/hardening head `a591da8`; persistent SQLite-WAL parameters per directed route and stable size multiplier; truthful 24h/7d/30d robust windows and long-tail stress; generation-bound funding/depth/quality/regime gates; five parallel grid-aligned episodes with censored timeout evidence and 30-episode production support for bucket-specific convergence p90; 20%/24h staged parameter changes; hard-bounded retention with reserved recent-window coverage; restart, migration, stale/outage, overload, true decision-deadline, cancellation, and bounded daemon-persistence shutdown fail-close; indeterminate save/delete/close outcomes retain runtime/risk ownership and latch entry; ShadowTrader consumes only current persisted bucket-qualified parameters; public estimate scope and `execution_authorized=false`; exact runs `32357556918` and `32359031361` passed all five jobs; independent exact review P0=0/P1=0/P2=0. PROD-06 is COMPLETE only for the Candidate-L2/public-shadow boundary |
| Phase 3.4 persistent shadow portfolio | COMPLETE | Exact head `22438c4`; SQLite schema v12 atomically persists each active tranche with its exact risk reservation; startup restores one coherent portfolio snapshot or remains fail-closed; 10 routes × 5 tranches preserve 5 USDT route/50 USDT portfolio limits; concurrent same-base routes admit exactly one; terminal transitions remove risk atomically; corrupt/missing/mismatched risk fails closed; durable indeterminate latch, bounded daemon ownership, exact durable/memory/observed reconciliation, runtime transition lock, and path recovery lease; restart restores identical 50-tranche reservations; exact run `32362827543` passed all five jobs with replay `9404192713`, C4 `9404179777`, C4.3 `9404177260`, and security `9404167259`; independent exact review P0=0/P1=0/P2=0. PROD-07/09/12 are COMPLETE at the shadow boundary |
| Phase 3.5 bounded Telegram portfolio visibility | COMPLETE | Owner-authenticated `/status`, `/positions`, `/pnl`, and `/risk` expose deterministic 10-route/50-tranche summaries below Telegram's 4096-character limit; every shadow/live/control response passes one final bounded renderer; malformed/non-finite private or shadow data fails closed and remains audited; live risk is zero only for complete account-wide FLAT state, otherwise requires exact signed exchange-position equality with durable journal fills and preserves explicit invalid reasons; no credentials or live authority added; exact head `b30cf5a`, run `32366732624`, replay `9405595049`, C4.3 `9405581358`, C4 critical `9405576802`, and security `9405570102` passed; independent exact review P0=0/P1=0/P2=0. PROD-11 is COMPLETE at the bounded shadow/private-read boundary |
| Phase 4.1 durable multi-action journal ownership | COMPLETE | Exact head `3731d72`; SQLite WAL admits at most 10 active actions with one canonical base lease and one exact route lease per action; concurrent same-base creation has exactly one winner; leases survive restart/migration and release only at FLAT; account-wide emergency actions own a global exclusive lease; legacy over-limit state, conflicting reactivation, noncanonical base identity, and inconsistent active snapshots fail closed; exact run `32373391094` passed all five jobs with replay `9408072839`, C4.3 `9408057869`, C4 critical `9408048167`, and security `9408036741`; independent technical review found P0/P1=0 |
| Phase 4.2 multi-action recovery and emergency control | COMPLETE | Exact head `593e691`; supervisor recovery is single-flight per durable action and isolates route failures; service/coordinator/reconciliation/shadow consumers use transactionally consistent multi-action snapshots; live control aggregates exact signed risk and concurrently closes uniquely owned venue-symbol positions; stable-FLAT commits the complete active set atomically, quarantines late private events, and preserves terminal idempotency; durable account-wide flatten single-flight prevents duplicate/reversing close submits; exact run `32379906107` passed all five jobs with replay `9410609578`, C4.3 `9410587650`, C4 critical `9410586435`, and security `9410568204`; downloaded artifacts report zero false success and zero production submits; independent review P0/P1/P2=0 |
| Phase 4.3 bounded 10-action supervisor smoke | COMPLETE | Exact head `e8d12cc`; the journal-level supervisor process-kill smoke persists 10 unique-base labels at the locked 5 USDT each/50 USDT portfolio boundary, supports repeated restart after a subset is already FLAT, rejects non-finite durable stress, and requires a fresh process to recover every remaining action with zero production exchange transports. Exact run `32383862119` passed all five jobs with replay `9412151140`, C4.3 `9412127393`, C4 critical `9412127632`, and security `9412115391`; independent review P0/P1/P2=0 |
| Phase 4.4 multi-symbol private emergency recovery | COMPLETE | Exact code head `c7cfa9f`; production `LiveControlService` reconciles and atomically stable-FLAT-closes 10 routes/20 positions across exact venue-symbol histories; one rejected route does not block the other reductions; durable account-wide ownership is restart-adoptable by verified process identity and reusable across one-shot control objects; accepted journaled client IDs are reconciled without duplicate submit; an unobservable attempted submit retains the exclusive lease and requires explicit external resolution rather than unsafe retry. Private reconciliation owns and coalesces bounded cached/forced account, fee, and history requests across caller timeouts and exposes explicit bounded shutdown failure. Exact run `32390979997` passed all five jobs with replay `9414849317`, C4 critical `9414831961`, C4.3 `9414824434`, and security `9414822131`; independent review P0/P1/P2=0. PROD-08 remains PARTIAL pending transition-complete process-kill/private-transport chaos evidence |
| Phase 4.5 transition-complete private restart chaos | COMPLETE | Exact head `f67afc4`; a fresh process recovers the maximum 10-action set independently from every active durable state (`PREPARED`, `SUBMITTING`, `ACKNOWLEDGED`, `PARTIAL`, `FILLED`, `REJECTED`, `UNKNOWN`, `RECOVERING`, `HEDGED`, `CLOSING`, `QUARANTINED`) through a separately persisted account-wide private simulator, production `LiveControlService`, exact client-ID reconciliation, and stable-FLAT. Exchange-visible outcomes missing from the killed process journal are ingested idempotently; missing/UNKNOWN outcomes never authorize retry or FLAT. Client-ID lookup is single-flight, bounded to one second, retained for explicit shutdown, and isolated so one normal lookup failure does not block other risk reductions. Exact run `32397471622` passed all five jobs, including the 11-state × 10-action Docker kill/restart loop, with replay `9417235987`, C4 critical `9417217665`, C4.3 `9417206863`, and security `9417198123`; independent review P0/P1/P2=0. PROD-08 is COMPLETE; PROD-10 remains PARTIAL pending Phase 4.6 |
| Phase 4.6 full priority scheduler and overload chaos | COMPLETE | Exact head `7458cf0`; one bounded in-process scheduler owns P0 emergency flatten, P1 unmatched hedge, P2 normal close, P3 private reconciliation, P4 new entry, P5 Candidate L2, and P6 broad/history. Four priority-reserved lanes plus two general workers prevent lower critical work or a full queue from blocking P0; exact-key single-flight survives caller cancellation; active keys, queued work, workers, and shutdown are bounded. Critical recovery atomically blocks P4 at the portfolio gate, sheds queued P4-P6, preserves active L2/risk reduction, and is exercised through the production supervisor mapping and 10-action restart smoke. Exact run `32405032176` passed all five jobs with replay `9420004626`, C4.3 `9419966478`, C4 critical `9419959185`, and security `9419950486`; independent exact review reported P0=0/P1=0/P2=0. PROD-10 is COMPLETE |
| Telegram locked command completeness remediation | COMPLETE | Exact head `ec4eb191` passed run `32451989969` 5/5 with five exact artifacts and independent P0/P1/P2=0 review, then Draft PR #7 was promoted and squash-merged to protected main as `7f8524bb`. Production router implements every locked read command (`/health`, `/routes`, `/orders`, `/exchanges`, and `/qualification` included), bounded truthful service/route/order/capability/epoch payloads, strict account-wide order identity, durable FINALIZED qualification visibility, stale capability fail-close, one shared snapshot/audit command budget, and explicit cancellation-resistant shutdown ownership. No live authority or credential was added |
| Durable 10-route × 5-tranche live ownership remediation | EXACT_CODE_GATE_PASS / EVIDENCE_REBIND | The SQLite journal now admits at most five unique tranche IDs and summed 5 USDT stress on one canonical route per base, with at most 10 routes/50 USDT portfolio stress/50 active actions; it rebuilds derived leases after restart, rejects duplicate/sixth/eleventh/conflicting/corrupt legacy state, and distinguishes standalone emergency exclusivity from ordinary multi-action recovery. Multi-action recovery compares the complete durable and exchange venue-symbol exposure maps before submit, journals one idempotent reduce-only close leg per nonzero tranche exposure, and reaches one atomic stable-FLAT barrier. Exact code head `202039c` passed run `32456702576` 5/5. Operations artifact `9437604847` proves 71 acceptance tests, all 11 restart states, 50 recovered HEDGED actions, exactly 100 simulated reducing closes, one stable-FLAT barrier, and zero production transports; replay `9437527891` is 88/88, C4 critical `9437503880` is 30/30 with zero production submits, C4.3 `9437511269` is 8/8 with zero false success/submits, and security `9437495493` reports zero vulnerabilities/secret findings. Local gate: lock64, Ruff113, mypy111, pytest 674 passed/6 platform-skipped, doctor shadow/live=false, YAML/diff-check. This STATUS-only evidence rebind changes no code and is accepted only by fresh exact-head CI plus final P0/P1/P2=0 review; canary remains locked to one tranche |
| C5 owner-operated canary | FORBIDDEN | Must not start until every software-completion remediation, qualification gate, external onboarding action, and separate short-lived live consent passes |
| Phase 5.1 Bitget Classic code candidate | COMPLETE | Bitget is now a typed venue profile with a Classic-only CCXT Pro transport, exact 4096-byte batch ticker framing and matching unsubscribe acknowledgement, raw books15 sequence propagation/regression rejection, exact linear-USDT discovery, split account-wide read-only snapshot/stream params, and final pinned protected IOC `clientOid`/crossed/force mapping. Wave 1 remains exactly Binance USD-M/Bybit/OKX; Bitget is explicitly denied at the live-canary submit boundary. Exact code head `2bee950`; local locked gate: lock64, Ruff96, mypy94, pytest547, doctor shadow/live=false, Bandit medium/high0, diff-check; exact run `32411553358` passed all five jobs and produced replay, C4 critical, C4.3, and security artifacts bound to the exact SHA; independent exact-head review P0=0/P1=0/P2=0. No credentials or network evidence were fabricated. |
| Phase 5.2 KuCoin Futures Classic code candidate | COMPLETE | KuCoin Futures is now a typed Classic-only venue profile with exact batch-BBO topic framing and matching unsubscribe, raw Level-50 sequence propagation, strict linear-USDT/no-fixed-notional proof, account-wide Classic position stream, read-only private snapshots/streams, and pinned protected cross/IOC `clientOid` request mapping. Raw `positionSide` preserves independent hedge sides when zero-position tombstones arrive; ambiguous side-less records fail closed. Wave 1 remains exactly Binance USD-M/Bybit/OKX and KuCoin is denied at the live-canary boundary. Exact code head `d37ded5`; local gate lock64/Ruff97/mypy95/pytest566/doctor shadow-live=false/Bandit0/diff-check passed; exact run `32415664858` passed all five jobs with replay `9423845774`, C4 critical `9423825785`, C4.3 `9423818033`, and security `9423805574`; independent exact review P0/P1/P2=0. |
| Phase 5.3 BingX capability-gated code candidate | COMPLETE | BingX is now a typed venue profile with official `incrDepth` snapshot/update sequence enforcement, persistent desynchronisation until a fresh `action=all`, matching L2 unsubscribe, exact linear-USDT amount/notional metadata, account-wide read-only private parameters, and pinned protected IOC `clientOrderID`/`positionSide=BOTH` mapping. Official BingX WS documents only per-symbol BBO, so the adapter truthfully reports broad BBO unavailable instead of creating an unbounded fallback. Wave 1 remains Binance USD-M/Bybit/OKX and BingX is denied at the live-canary boundary. Exact code head `0678049`; local gate lock64/Ruff99/mypy97/pytest579/doctor shadow-live=false/Bandit0/diff-check passed; exact run `32418726763` passed all five jobs with replay `9424931463`, C4 critical `9424899802`, C4.3 `9424897938`, and security `9424888032`; test-hardening head `54b46f0` passed run `32419627419` 5/5 with replay `9425251748`, C4 critical `9425231202`, C4.3 `9425231328`, and security `9425217060`; independent exact review P0/P1/P2=0. |
| Phase 5.4 MEXC capability-gated code candidate | COMPLETE | MEXC is a typed public/read-only profile with exact incremental-depth continuity and strict raw symbol/base/quote/settle/contract/price/amount/minimum qualification. Official all-contract tickers do not prove bid/ask and book ticker is per-symbol, so broad BBO is deliberately unavailable rather than using unbounded fan-out. Contract create/cancel are physically denied because the official endpoints remain under maintenance; private capability and live canary fail closed. Code head `fcb0e45` passed independent technical review P0/P1/P2=0. Test-hardening head `0f1221f` passed exact run `32423071724` 5/5 with replay `9426439525` (79 scenarios), C4 critical `9426417474` (30/30, zero production submits), C4.3 `9426419340` (8/8, zero false success/submits), and security `9426408589` (zero vulnerabilities/secret findings). Local gate: lock64, Ruff101, mypy99, pytest585, doctor shadow/live=false, diff-check. |
| Phase 5.5 seven-venue capability matrix + FT-02 isolation | COMPLETE | One typed matrix reports qualified, quarantined, or disabled state for all seven venue profiles while startup rejects any Wave 1 reclassification. Public scans expose current six-hour-bound reports and isolate one quarantined venue without stopping healthy Wave 1 routes. PREPARED live-canary recovery revalidates current public/private capability, account state, clock, books, funding, economics, journal/reconciliation/risk, protected marketable IOC caps, and final pause/kill immediately before submit; every transport, control-read, and teardown path is owned and bounded. Code head `7506517` passed exact run `32429457353` 5/5 with replay `9428595773` (80 scenarios), C4 critical `9428577449` (30/30), C4.3 `9428574423` (8/8, zero false success/submits), and security `9428569687` (zero vulnerabilities/secret findings); independent exact technical review P0/P1=0. No credentials, expansion submit authority, or real order was added. |
| Continuous seven-venue public runtime | LOCAL_GATE_PASS / EXACT_GATE_REQUIRED | Production shadow evaluation and `public-scan` instantiate all configured Wave 1/2/3 public venues. Capability and instrument failures isolate only the affected expansion venue; missing factories retry transactionally; cancellation-resistant probes have a hard deadline, cannot publish late qualification, coalesce to one owned task per venue, and make shutdown fail explicitly while nonterminal. P6/broad shedding defers periodic expansion probes while preserving Wave 1 and explicit force/reconnect. The capability matrix keeps Bitget/KuCoin public-only and BingX/MEXC quarantined when required capabilities are absent; live/canary authority remains immutable Wave 1 and `execution_authorized=false`. Local gate: lock64, Ruff113, mypy111, pytest 681 passed/6 platform-skipped, doctor shadow/live=false; exact commit, CI/artifacts, and final review are still required. |
| FT-03 reproducible Germany/Japan latency evidence | SOFTWARE_READY / OWNER_ACTION | Exact Wave 1 feed/API/private-event samples use exchange timestamps, qualified clock skew and monotonic API RTT; raw NDJSON and reports bind executing code/config/host/attestation/instrument; provider evidence requires a checkout-pinned Ed25519 trust root; final selection rebuilds raw evidence and keeps Germany unless Japan improves weighted p95 by at least 20% with no cell p99 worsening above 50%. The default trust-root sentinel and absent credentials/VPS evidence fail closed. Code head `04a4794` passed exact run `32435546614` 5/5 with replay `9430648766` (80 scenarios), C4 critical `9430638548` (30/30, zero submits), C4.3 `9430634489` (8/8, zero false success/submits), and security `9430627269` (zero vulnerabilities/secrets); independent review P0/P1/P2=0. |
| Operations final proof | COMPLETE | Code checkpoint `bc4cc77`; deploy requires an untracked `.env` at mode 0600 and verifies the running exact SHA/digest in shadow; successful identity is atomic and idempotent. Upgrade stops the app for a consistent backup and automatically restores both SQLite state and the previous immutable image after failed health. Qualification progress exposes duration, synchronized-event, funding, replay, unresolved order/exposure, quality, PnL, strategy, and exception blockers. SQLite schema v14 binds strict stage limits and signed evidence to the current release/qualification; only canonical four-leg normal paired cycles count; completion atomically freezes normal entry with complete journal identity; promotion rechecks it; emergency reduction remains available; and legacy weak stage results are archived while runtime resets to SHADOW for sequential re-attestation. Exact run `32446447344` passed all five jobs. Operations `9434329359` passed Ubuntu 24.04 rollback/restart and 42/42 criteria through 68 tests with zero production transports; replay `9434246792` passed 83 scenarios; C4 critical `9434227604` passed 30/30 with zero submits; C4.3 `9434224999` passed 8/8 with zero false success/submits; security `9434217107` reports zero vulnerabilities/secrets. Local gate: lock64, Ruff113, mypy111, pytest666, doctor shadow/live=false, diff-check; independent review P0=0/P1=0/P2=0. |
| Autonomous VPS runtime | SOFTWARE_RELEASED_RC1 | Exact code checkpoint `bc4cc77`, evidence head `82d99e7`, protected-main squash `6b0ebf85`, and post-merge run `32447657564` passed all locked gates. One-command Ubuntu 24.04 bootstrap, `/usr/local/sbin/ipegctl`, hardened systemd/Docker ownership, external mode-0600 onboarding, immutable OCI revision/SHA binding, one service-owned `AutonomousOrchestrator`, signed risk-stage evidence, and fail-closed schema-v14 promotion are complete. Annotated prerelease `v1.0.0-rc1` is published; publisher `32448023653` produced GHCR digest `sha256:c6785d343ddbc5c2af1dd3eb6926edec0e154865a2d7e7bedf18199d5c719895` and manifest artifact `9434727031`. The orchestrator cannot authorize canary/live. |
| C6 venue expansion | SOFTWARE_COMPLETE / OWNER_EVIDENCE | All four expansion adapters plus the seven-venue capability matrix, FT-02 isolation, FT-03 evidence tooling, and operations gates are exact-head verified. Real Germany/Japan measurements, account capability evidence, and a successful owner-operated canary remain external prerequisites; Wave 1 and the live-canary allowlist remain unchanged. |

## Decisions made during implementation

Append only short entries:

```text
YYYY-MM-DD — decision — reason — affected modules
```

2026-08-14 — Persist service heartbeat and restart count in SQLite WAL — Docker health must prove the application loop is alive and restart-safe — `state.py`, `service.py`, CLI, Compose
2026-08-14 — Use `ccxt.pro.binance` future transport for Binance USD-M — the `binanceusdm` Pro class lacked the required WebSocket capabilities in an automated probe — `ccxt_pro.py`
2026-08-14 — Quarantine books with unknown sequence and continue with remaining qualified venues — fail-closed market data must not stop the Wave 1 process — `market_data.py`, `public_engine.py`
2026-08-20 — Persist tranche and exact risk reservation in one SQLite transaction and use a separate durable indeterminate marker — restart must restore identical risk while a locked WAL writer cannot delay fail-closed ownership — `state.py`, `risk.py`, `shadow.py`
2026-08-20 — Render one bounded Telegram summary and derive live risk only from complete account-wide private state plus exact journal-position equality — operator visibility must remain deliverable and must never label unknown or external exposure as zero risk — `telegram_control.py`, `live_control.py`
2026-08-20 — Lease every durable live action by canonical base and exact route, with a global exclusive emergency lease — concurrent or restarted journal writers must never create conflicting active ownership — `live_journal.py`
2026-08-20 — Recover every durable live action independently but flatten the dedicated account through one durable account-wide single-flight lease and one atomic complete-active-set barrier — one hung route must not block another risk reduction, while concurrent controls and late actions must never duplicate or reverse an emergency close — supervisor, journal, reconciliation, live control
2026-08-20 — Run the process-kill recovery smoke at the maximum 10-route/50-USDT durable boundary by default — exact Docker evidence must exercise the product ceiling rather than infer it from a one-action restart — supervisor smoke, CLI, CI Docker job
2026-08-20 — Adopt account-wide emergency ownership only across a proven dead process incarnation and never resubmit an exchange-unobservable attempted client ID — restart recovery must close all known positions without duplicating an unknown live order — live journal, reconciliation, live control
2026-08-20 — Reconcile killed-process client IDs through one bounded owned lookup and require private stable-FLAT after every active durable transition — restart chaos must exercise production private recovery at the 10-action ceiling without turning an unknown submit into a retry — live control, private reconciliation, supervisor smoke, CI Docker proof
2026-08-20 — Schedule every public/private workload class through bounded P0-P6 ownership with one reserved critical lane per P0-P3 priority and atomic P4 portfolio admission — emergency flatten, hedge, close, and reconciliation must remain runnable while entry, Candidate L2, and broad/history work are shed under overload — priority scheduler, supervisor, shadow runtime, service, Docker smoke
2026-08-20 — Add Bitget only through the Classic USDT-FUTURES profile and keep the live canary allowlist unchanged — pinned CCXT supports Classic data/private primitives but its batch ticker unsubscribe stub and unqualified UTA require an explicit matching override and fail-closed separation — public/private adapters, config, execution boundary, CLI
2026-08-20 — Add KuCoin Futures only through the Classic contract profile and preserve hedge tombstones from raw `positionSide` — pinned CCXT normalisation loses the side of a zero contract position, so Classic raw identity is required to avoid erasing the independent opposite hedge — public/private adapters, universe/routes/economics, config, execution boundary
2026-08-21 — Add BingX as a capability-gated linear-USDT profile with native sequenced L2 but keep broad BBO disabled — official BingX WS documents only per-symbol bookTicker, which cannot satisfy the bounded batch-BBO contract without the forbidden fan-out fallback — public/private adapters, config, execution boundary
2026-08-21 — Stabilize the 10-route hung-lookup evidence with a six-second scenario hard timeout — repeated local/CI runner jitter exceeded a 3.5-second wall assertion although transport deadlines and fail-closed functional outcomes remained correct — live control test only
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
2026-08-16 — Persist public-shadow adaptive parameters per directed route and stable size multiplier with five grid-aligned convergence buckets requiring 30 episodes each — restart-safe robust windows must never substitute aggregate or under-supported convergence evidence for the current entry spread — route calibration, public engine, shadow evaluator, SQLite state
2026-08-20 — Treat every simulated tranche SQLite write as a bounded ownership transition and retain runtime/risk ownership behind an entry latch when its terminal outcome is unknowable — cancellation, slow native storage, and shutdown may neither create an unowned durable tranche nor block process exit — shadow runtime/trader, execution coordinator, SQLite state
2026-08-21 — Keep Wave 1 immutable while exposing one current seven-venue capability matrix and repeat every PREPARED opening gate after durable submit intent — expansion evidence must not expand live authority, and stale capability/control/depth/economics must never reach a submit transport — capability matrix, public engine, canary runtime/coordinator
2026-08-21 — Bind region evidence to the executing checkout, current clock-skew policy, and one checkout-pinned Ed25519 operator key while keeping Germany as the default — self-declared VPS metadata, policy files, timestamps, or trust roots must never qualify a deployment region — region latency pipeline, private event adapter, CLI, runtime policy
2026-08-21 — Make deployment identity transactional and promotion state operational — an unhealthy image, permissive secret file, elapsed clock, or YAML-only risk stage must never be mistaken for a deployable or live-authorized product — deploy/upgrade scripts, qualification progress, SQLite state, canary runtime, CI operations proof

## Active blockers / owner actions

### Repository visibility

The repository is PUBLIC. `OWNER_ACTION.json` contains the exact separate action required to make it PRIVATE. No credential or operational evidence may be committed while this remains unresolved.

### FT-03 real regional evidence

- **Exact required action:** provision one Germany VPS and one Japan VPS from the supported provider/region allowlist; create an offline Ed25519 operator key, commit only its raw public-key SHA-256 to `config/RUNTIME_POLICY.yaml`, keep the private key outside Git, provide restricted read-only Wave 1 API credentials to each VPS outside Git, sign each provider metadata document, and run the documented `region-latency-probe`, `region-latency-report`, then `region-latency-select` commands with at least 30 samples per venue/channel.
- **Why Codex cannot do it:** this requires external VPS access, owner-controlled signing material, exchange account permissions, and real account-wide private events; none are present or authorized for disclosure.
- **Validation:** both probes produce complete 270-sample raw files, both reports PASS exact reconstruction, and `interexchange-grid region-latency-select ...` prints a hash-bound selection. Japan is selected only at the locked >=20% weighted-p95 improvement and <=50% maximum cell-p99 worsening thresholds.
- **Fail-closed behavior:** the all-zero trust-root sentinel rejects every supplied key, Germany remains the default, no C5 deployment or live order is authorized, and no region claim is made until the complete signed evidence exists.

## Last verified command

```text
2026-08-21 continuous seven-venue public runtime, Windows equivalent of every
Makefile verify target: PASS
- exact lock validation: PASS (64 packages)
- Ruff format/check: PASS (113 files)
- mypy --strict: PASS (111 source/test files)
- pytest: 681 passed, 6 platform-only skips
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false
- git diff --check: PASS

The scoped checkpoint continuously observes every configured Wave 1/2/3 venue through the existing
typed adapter boundary and isolates unsupported or failed expansion venues without widening submit
authority. Capability probes, retries, cancellation-resistant late returns, overload shedding, and
shutdown ownership are bounded and fail closed. Wave 1 remains the only immutable live allowlist;
no credential, production submit, transfer, withdrawal, or live-money authority was added. The
checkpoint requires an exact commit, fresh exact-head CI/artifacts, and final independent review.
```

```text
2026-08-21 SOFTWARE_COMPLETE release evidence
- protected main merge: 6b0ebf85e1578852c8ba31b3d49144a27f17a3ff
- post-merge CI 32447657564: PASS 5/5
- annotated prerelease: v1.0.0-rc1
- publisher run 32448023653: PASS
- release manifest artifact: 9434727031
- GHCR release + SHA tags: sha256:c6785d343ddbc5c2af1dd3eb6926edec0e154865a2d7e7bedf18199d5c719895

This historical release was later found incomplete by the master-goal audit and no longer supports
a SOFTWARE_COMPLETE claim. Default shadow mode, C5 prohibition, missing credentials, signed
regional/VPS evidence, 24-hour qualification, and irreversible live-money approval continue to
fail closed.
```

```text
2026-08-21 autonomous VPS/risk-stage attestation checkpoint, Windows equivalent of every
Makefile verify target: PASS
- exact lock validation: PASS (64 packages)
- Ruff format/check: PASS (113 files)
- mypy --strict: PASS (111 source/test files)
- pytest: 666 collected, all runnable tests passed (six platform skips)
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false
- git diff --check: PASS

The checkpoint binds signed completion evidence to one canonical normal four-leg paired cycle set,
freezes normal entry atomically with journal identity, revalidates it at promotion, preserves emergency
risk reduction, and resets legacy weak advanced stages to SHADOW for sequential re-attestation.
Independent final review reported P0=0, P1=0, P2=0. Exact code-head run `32446447344` passed
all five jobs and all five downloaded artifacts bind `bc4cc77caa36d9b6d54da0c9c7b87ba0d28f24cd`.
No credentials, production transport, live authority, or real order were used. This evidence-only
status commit requires fresh exact CI/review before protected-main merge.
```

```text
2026-08-20 Phase 4.4 local Windows equivalent of every Makefile verify target: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (92 files)
- mypy --strict: PASS (90 source/test files)
- pytest: 479 passed; focused journal/control/reconciliation/canary integration: 63 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false
- Bandit medium/high: 0; git diff --check: PASS

The frozen dirty Phase 4.4 delta exercises production private reconciliation and emergency control at
10 routes/20 positions, isolates a rejected close, adopts only a dead process incarnation, reuses exact
journaled client IDs already visible at the exchange, and keeps unknown submit ownership fail-closed.
It does not infer that a crash-before-network is safe to retry. Transition-complete process-kill chaos,
full PROD-08/10, live qualification, and Ready/merge remain outside this checkpoint.

GNU make is not installed on this Windows host. No production credentials were used, route
calibration and Candidate L2 keep `execution_authorized=false`, and no real order was submitted.

Fresh exact-head Linux CI run `32390979997` passed verify, Docker smoke, security, C4 critical,
and C4.3 proof on `c7cfa9f`; all four uploaded artifacts bind that exact SHA.
```

```text
2026-08-20 Phase 4.5 local Windows equivalent of every Makefile verify target: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (93 files)
- mypy --strict: PASS (91 source/test files)
- pytest: 514 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false
- Bandit medium/high: 0; git diff --check: PASS

The actual-process Windows smoke killed and restarted each of the 11 active durable states with
10 actions. The restart loaded independently persisted account-wide private state, resolved exact
client IDs through the production control path, closed every known position, and required the stable
FLAT barrier. Missing or cancellation-resistant private outcomes remain bounded and fail closed;
no production exchange transport or real order was used.

Exact-head run `32397471622` passed verify, Docker smoke, security, C4 critical, and C4.3.
All four uploaded artifacts bind `f67afc41f6a2368899f0eb91499c37296bf63594`; independent
final review reported P0=0, P1=0, P2=0. PROD-08 is complete. Full PROD-10 remains pending.
```

```text
2026-08-20 Phase 4.6 local Windows equivalent of every Makefile verify target: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (95 files)
- mypy --strict: PASS (93 source/test files)
- pytest: 532 passed; priority-scheduler stress: 20/20 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false
- Bandit medium/high: 0; git diff --check: PASS

The bounded scheduler preserves a separately reserved execution lane for each P0-P3 class, sheds
P4-P6 before critical recovery is degraded, coalesces exact action keys, and retains explicit bounded
shutdown ownership. The shadow portfolio gate rechecks current scheduler state before each route and
before risk reservation, so a newly arrived critical action stops further P4 mutation. The production
10-action restart smoke asserts scheduler ownership, P0 recovery, P4 shedding, and stable-FLAT.

Independent exact-head review reported P0=0, P1=0, P2=0. No production credentials were used,
live_orders_allowed remains false, and no real order was submitted. Exact run `32405032176` passed all
five jobs on `7458cf0f8fbaf241933b4c58eb04f06ff03fbe7c`; replay artifact `9420004626`
reports 79/79 scenarios, C4 critical `9419959185` reports 30/30 scenarios and zero production
submits, C4.3 `9419966478` reports 8/8 scenarios and zero false success/production submits, and
security artifact `9419950486` is exact-head-bound. PROD-10 is complete.
```

```text
2026-08-20 Phase 5.2 KuCoin Futures Classic exact code checkpoint: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (97 files)
- mypy --strict: PASS (95 source/test files)
- pytest: 566 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false; Wave 1 unchanged
- Bandit medium/high: 0; git diff --check: PASS

The Classic-only adapter proves matching BBO/L2 subscribe-unsubscribe contracts, propagates raw
order-book sequence, rejects uncertain instrument economics, and keeps read-only account-wide
position/order/account state fail closed. Zero-position hedge events use raw `positionSide`, so a
LONG or SHORT tombstone cannot erase the independent opposite side. No credential, production
submit authority, or live-canary allowlist expansion was added. Independent exact-head review
reported P0=0, P1=0, P2=0. Exact run `32415664858` passed verify, Docker smoke, security,
C4 critical, and C4.3; replay `9423845774` reports 79 scenarios, C4 critical `9423825785`
reports 30/30 scenarios and zero production submits, C4.3 `9423818033` reports 8/8 and zero
false success/production submits, and security `9423805574` reports 64 dependencies with zero
known vulnerabilities and zero secret findings. All four artifacts bind code head `d37ded5`.
```

```text
2026-08-21 Phase 5.3 BingX local code checkpoint: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (99 files)
- mypy --strict: PASS (97 source/test files)
- pytest: 579 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false; Wave 1 unchanged
- Bandit medium/high: 0; git diff --check: PASS

The profile proves official `incrDepth` subscription/unsubscription and preserves a sequence-gap
latch until a new `action=all` snapshot. Pinned REST tests prove exact minimum amount/notional,
account-wide linear position parameters, and protected IOC `clientOrderID`/`positionSide=BOTH`.
Broad BBO deliberately remains unavailable because the official WS documents only per-symbol
bookTicker and the product rejects unbounded fan-out. No credential, production-submit authority,
or live-canary allowlist expansion was added. Independent dirty-tree review reported
P0=0, P1=0, P2=0. Exact run `32418726763` passed verify, Docker smoke, security,
C4 critical, and C4.3; replay `9424931463` reports 79/79 scenarios, C4 critical
`9424899802` reports 30/30 scenarios and zero production submits, C4.3 `9424897938`
reports 8/8 with zero false success/production submits, and security `9424888032`
reports zero secret findings. All four artifacts bind code head `0678049`.

The subsequent test-only head `54b46f0` replaces a scheduler-sensitive 3.5-second wall assertion
with a six-second scenario hard timeout while preserving all fail-closed and isolation assertions.
Fresh exact run `32419627419` passed all five jobs; replay `9425251748`, C4 critical
`9425231202`, C4.3 `9425231328`, and security `9425217060` are exact-head-bound.
```

```text
2026-08-21 Phase 5.4 MEXC local code checkpoint: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (101 files)
- mypy --strict: PASS (99 source/test files)
- pytest: 585 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false; Wave 1 unchanged
- Bandit medium/high: 0; git diff --check: PASS

The adapter preserves incremental-depth version continuity and latches a gap until transport
replacement. Broad BBO is unavailable because neither the all-contract ticker payload nor a
bounded batch book-ticker channel proves best bid/ask. Only `apiAllowed=true`, active linear-USDT
swaps with matching official raw symbol/currency/contract metadata qualify;
the documented absence of a fixed notional is represented explicitly. Contract create/cancel are
physically rejected before transport because the official endpoints remain under maintenance.
No credential, production-submit authority, or live-canary allowlist expansion was added.
The first pushed head `8b022a6` was rejected by independent review because it trusted an
unsupported all-ticker BBO payload and incomplete raw metadata. Those claims and tests were removed;
the remediated code head `fcb0e45` is fail-closed and passed independent technical review with
P0=0, P1=0, P2=0. Exact run `32422342149` passed security, C4 critical, C4.3, and Docker,
but verify exposed the pre-existing one-second readiness-file wait in the 10-action supervisor
smoke. The test now waits boundedly for complete readiness content, propagates an early background
failure, passes 10/10 focused repetitions, and preserves every recovery assertion. The full local
gate remains lock64/Ruff101/mypy99/pytest585/doctor shadow-live=false. Test-hardening head
`0f1221f` passed exact run `32423071724` 5/5. Replay `9426439525` reports 79 scenarios;
C4 critical `9426417474` reports 30/30 and zero production submits; C4.3 `9426419340`
reports 8/8 with zero false success/submits; security `9426408589` reports zero dependency
vulnerabilities and secret findings. All artifacts bind the exact SHA. Independent exact review
found P0=0, P1=0; its sole P2 was this then-pending evidence update. Phase 5.4 is complete.
```

```text
2026-08-21 Phase 5.5 seven-venue capability matrix and FT-02 local code checkpoint: PASS
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (103 files)
- mypy --strict: PASS (101 source/test files)
- pytest: 610 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false; Wave 1 unchanged
- Bandit medium/high: 0; git diff --check: PASS

All seven typed venue profiles produce one current, reason-coded capability matrix without changing
the immutable Wave 1 or live-canary allowlist. One quarantined venue is isolated while healthy public
routes continue. A PREPARED action repeats current capability, private account, clock, book depth,
funding, economics, journal/reconciliation/risk, protected IOC marketability, and final pause/kill
checks before any submit. Cancellation-resistant discovery, final control reads, and adapter teardown
are bounded, retained, and explicitly fail shutdown; every denial persists quarantine with zero submit.

No credentials, production-submit authority, or expansion live path was added, and no real order was
sent. Exact code head `75065173b87c06f7d58afa2109b32076d5c88855` passed run `32429457353`
5/5. Replay artifact `9428595773` reports 80 scenarios; C4 critical `9428577449` reports
30/30; C4.3 `9428574423` reports 8/8 and zero false success/submits; security `9428569687`
reports zero vulnerabilities and secret findings. Independent exact technical review found P0/P1=0;
this evidence-only update closes its sole documentation P2. Phase 5.5 is complete.
```

```text
2026-08-21 FT-03 region evidence software checkpoint `04a4794`: PASS / OWNER EVIDENCE REQUIRED
- exact main lock validation: PASS (64 packages)
- ruff format --check + ruff check: PASS (105 files)
- mypy --strict: PASS (103 source/test files)
- pytest: 629 passed
- interexchange-grid doctor: PASS; mode=shadow; live_orders_allowed=false
- git diff --check: PASS; independent dirty-snapshot review: P0=0, P1=0, P2=0

Exact run `32435546614` passed verify, Docker smoke, security, C4 critical, and C4.3. Downloaded
artifacts `9430648766`, `9430638548`, `9430634489`, and `9430627269` bind code head
`04a4794885c460dad9724ecddc56f74edc27f8bf`; replay is 80/80, C4 critical is 30/30,
C4.3 is 8/8 with zero false success/submits, and security reports zero vulnerabilities/secrets.

The software measures the exact Wave 1 feed/API/private-event matrix on attested Germany and Japan
hosts, reconstructs every percentile from raw evidence, and applies the locked Germany-default
migration rule. Monotonic RTT, bounded clock skew, executing-checkout identity, an exact locked-policy
path, and a pinned Ed25519 public-key fingerprint prevent caller-supplied time, source, policy, and
cloud-metadata trust. The committed all-zero key fingerprint deliberately blocks real collection until
the owner installs a real reviewed public trust root. No credentials, real regional evidence,
production-submit authority, or live order are present, and no deployment-region result is claimed.
```

```text
2026-08-21 operations final-proof code checkpoint `16bf1d2`: PASS / SOFTWARE COMPLETE
- exact main lock validation: PASS (64 packages)
- Ruff format/check: PASS (111 files)
- strict mypy: PASS (109 source/test files)
- pytest: 643 passed, 3 Bash-only tests skipped on Windows and required on Ubuntu CI
- doctor: PASS; mode=shadow; live_orders_allowed=false
- Bandit medium/high: 0; git diff --check: PASS

The delta closes OPS-03/04/07/09 in runtime, not only documentation. Exact run `32440216743`
passed all five jobs. Operations artifact `9432249857` proves one pinned Ubuntu 24.04 local registry
digest twice, Compose down/up state recovery, real unhealthy-image rollback with SQLite restoration,
the full 11-state private transition matrix, zero production transports, and all 42 manifest criteria
through 60 executed JUnit tests. Replay `9432176964` is 80/80; C4 critical `9432160656` is 30/30
with zero production submits; C4.3 `9432156081` is 8/8 with zero false success/submits; security
`9432146473` reports zero vulnerabilities and secret findings. No secret, production credential,
live stage promotion, real VPS result, live-money authorization, or real order is present. All further
work requires the explicit external owner actions recorded above; the product remains fail closed.
```

```text
2026-08-21 autonomous runtime exact checkpoint `02789831`: PASS / RELEASE GATE REQUIRED
- exact lock validation: PASS (64 packages)
- Ruff format/check: PASS (113 files)
- strict mypy: PASS (111 source/test files)
- pytest: 649 passed, 6 Bash-only tests skipped on Windows and required on Ubuntu CI
- doctor: PASS; mode=shadow; live_orders_allowed=false
- git diff --check: PASS

This delta installs `ipegctl`, an idempotent Ubuntu 24.04 bootstrap and a hardened systemd unit;
deployment uses `/etc/ipeg/ipeg.env` mode 0600 and rejects an immutable digest whose OCI revision
label differs from the requested full release SHA. The service-owned `AutonomousOrchestrator`
idempotently starts/resumes the exact route/release/source/config/image qualification epoch, reports
blockers, finalizes only the observation collection, and never authorizes canary/live. Parquet progress
runs in a bounded child process that is terminated on cancellation, so shutdown cannot leave an
unowned qualification scan. Local onboarding remains interactive and writes credentials only outside
Git; `canary-arm` fails until a separate `LIVE_CANARY_CONSENT`. Exact run `32443207014` passed all
five jobs. Operations `9433253029`, replay `9433184226`, C4 critical `9433172177`, C4.3
`9433167340`, and security `9433161453` are bound to the same exact head; operations covers 42/42
acceptance criteria. Independent adversarial review, Ready/squash merge, protected main, and the v1
RC image/release remain required.
```

```text
2026-08-21 risk-stage attestation remediation: LOCAL / EXACT GATE REQUIRED

Independent review of exact head `02789831` found that direct runtime stage limits did not enforce
the global 5 USDT pair/50 USDT portfolio caps and `risk-stage-complete` accepted a minimal
self-declared JSON. The local correction requires exact non-boolean integer limits, hard global caps,
and one exact-schema Ed25519-signed stage artifact whose public key is pinned by locked policy. The
artifact is bound to the current promotion timestamp, qualification/runtime-policy/release/source/
config/image identity, account-wide two-snapshot quiet stable-FLAT watermark, zero active/open/
unknown/unresolved exposure state, availability/private completeness, PnL/loss/holding gates, and
the durable journal. The default all-zero key sentinel keeps completion impossible until owner
onboarding installs an authorized offline public-key hash. No live authority or submit path was added.
```
