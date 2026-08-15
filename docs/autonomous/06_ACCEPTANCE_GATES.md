# Acceptance gates

## Gate C4.3

- C4.3 stable-FLAT scenarios all pass.
- `FlatBarrierResult.verified` is mandatory for success.
- exact-head artifact and image digest.
- production submit calls in CI = 0.
- separate adversarial review report has no P0/P1 findings.

## Gate Software RC

- main branch protected and green;
- release tag + image digest;
- deterministic lock/SBOM/security scans;
- clean Ubuntu deploy and rollback;
- live disabled by default;
- no secrets in repository/history/artifacts.

## Gate Multi-instrument Product Ready

- all Wave1 common instruments discovered;
- BBO broad + bounded L2 scheduler;
- adaptive route parameters;
- 10-route/5-tranche shadow simulator;
- persisted portfolio risk;
- Telegram complete;
- restart/overload/chaos tests;
- zero trades is accepted when economics fail.

## Gate Wave1 Canary Ready

- Binance, Bybit, OKX public/private/execution contracts;
- account-wide private cache;
- exact route and emergency venue qualification;
- 24h exact epoch;
- >=10,000 synchronized observations per route side;
- >=3 funding checkpoints per involved venue;
- positive simulated net PnL;
- no unresolved orders/exposures/errors;
- canary stress <=1 USDT;
- dedicated accounts empty before entry.

## Gate Canary PASS

- one minimum-notional paired cycle;
- no other route/tranche;
- no unknown order;
- automatic close/recovery;
- stable-FLAT verified after quiet period;
- all venues report zero positions/open orders;
- realized result and all costs recorded;
- credentials remain withdrawal/transfer disabled.

## Pilot promotion gates

For every stage window:

- availability >=99%;
- no liquidation or ADL;
- no unresolved execution state;
- no manual emergency intervention;
- realized net PnL >=0;
- max realized loss <= stage portfolio limit;
- every position closed within hard max hold;
- data/private completeness >=99.9% while exposure exists;
- risk invariants never violated.

Failure returns system to `SHADOW_CLOSE_ONLY`; automatic risk promotion is disabled.

## Gate seven-venue software complete

- public monitoring implemented for all seven;
- capability matrix generated at runtime;
- Wave1 live-qualified;
- Bitget and KuCoin Classic at least canary-ready;
- BingX capability-gated;
- MEXC live explicitly disabled while official order API remains unavailable;
- no venue can weaken global fail-closed semantics.

## Final v1.0.0 definition

- one-command deploy on clean Ubuntu 24.04;
- autonomous start/restart/recovery;
- multi-instrument adaptive grid;
- persistent risk/execution state;
- Telegram operation;
- qualified live venues;
- complete documentation/runbook;
- immutable release artifact;
- all final manifest criteria PASS.
