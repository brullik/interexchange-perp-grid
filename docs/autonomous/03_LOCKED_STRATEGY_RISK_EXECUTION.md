# Зафиксированная стратегия, риск и исполнение

## 1. Directed executable spread

Для нормализованного base quantity `q`:

```text
entry_long  = VWAP(asks on long venue, q)
entry_short = VWAP(bids on short venue, q)
exit_long   = VWAP(bids on long venue, q)
exit_short  = VWAP(asks on short venue, q)

entry_spread = entry_short - entry_long
exit_spread  = exit_short - exit_long
```

Last price не используется для решения.

## 2. Cost model

```text
stressed_total_cost =
    4 actual-account taker fees
  + entry L2 impact
  + exit L2 impact
  + expected funding
  + stressed funding to maximum hold
  + latency reserve
  + partial-fill reserve
  + third-venue executable hedge cost
  + reconciliation/forced-exit reserve
  + liquidation-distance reserve
```

Entry разрешён только когда:

```text
expected_net_pnl > 0
expected_gross_pnl >= 2.0 × stressed_total_cost
expected_net_pnl >= route.minimum_profit_usdt
```

`2.0` configurable вниз только запрещено; повышение разрешено.

## 3. Adaptive grid — точная начальная формула

Для каждого route/size bucket:

```text
robust_sigma = 1.4826 × MAD(spread_bps)
cost_floor_bps = stressed_total_cost / notional × 10000

entry_level_1 = max(
    q90(spread_bps),
    median(spread_bps) + 2.5 × robust_sigma,
    2.0 × cost_floor_bps + minimum_profit_bps
)

grid_step = max(
    1.25 × robust_sigma,
    q75(adverse_excursion_after_entry_bps),
    cost_floor_bps,
    normalized_tick_bps
)

entry_level_n = entry_level_1 + (n-1) × grid_step, n=1..5
```

Stop/buffer model:

```text
route_stop = max(q99.9(spread_bps), entry_level_1 + 5 × grid_step)
             + 0.5 × grid_step
```

Размер каждой части уменьшается так, чтобы суммарный stressed route loss у
`route_stop` не превысил stage limit. Части распределяются по примерно равному
вкладу в риск, а не по равному notional.

Target close конкретной части:

```text
target_close = min(
    median + 0.5 × robust_sigma,
    tranche_entry - max(grid_step, cost_floor_bps + minimum_profit_bps)
)
```

Часть закрывается только paired и по фактическому одинаковому base quantity.

## 4. Regime/data gates

Новые входы запрещены, если выполняется любое условие:

- менее 10 000 synchronized route observations для текущего epoch;
- observation period менее stage minimum;
- sequence/checksum gap;
- stale data;
- fee/funding/mark/index неизвестны;
- median shift > 3 robust sigma относительно предыдущего окна;
- q99 spread изменился более чем на 30% за сутки;
- exit depth < 3 × текущий route notional;
- listing age < 14 суток;
- unresolved order/action/exposure;
- third venue не может исполнить полный residual.

## 5. Order execution

Normal:

```text
marketable limit IOC with side-aware protected price
```

- protected price от marginal consumed level, не VWAP;
- two legs submitted concurrently после durable PREPARED;
- actual fills only;
- open remainder cancel;
- unknown status только reconcile, без повторной отправки client ID;
- residual recovery: top-up smaller → reduce larger → third venue → flatten;
- success только после stable exchange-verified state.

Unbounded market разрешён только для risk reduction:

```text
EMERGENCY_HEDGE
EMERGENCY_CLOSE
LIQUIDATION_PREVENTION
```

## 6. Stage risk profiles

| Stage | Routes | Tranches/route | Pair stress | Portfolio stress | Effective leverage | Minimum duration before promotion |
|---|---:|---:|---:|---:|---:|---:|
| C5 canary | 1 | 1 | 1 USDT | 1 USDT | 3x cap | one successful stable-FLAT cycle |
| Pilot A | 1 | 2 | 2 USDT | 2 USDT | 2x | 24h |
| Pilot B | 2 | 3 | 3 USDT | 6 USDT | 2x | 72h |
| Wave1 production | 3 | 5 | 5 USDT | 15 USDT | 3x | 7 days |
| Full target | 10 | 5 | 5 USDT | 50 USDT | 3x | all venue/portfolio gates |

Promotion автоматически запрещён при:

- non-flat emergency outcome;
- unresolved order/exposure;
- daily realized loss > current portfolio stage limit;
- any liquidation/ADL;
- private-state completeness failure;
- data-quality violation during active exposure;
- negative aggregate realized net PnL at stage end;
- operational availability < 99% during stage window.

При gate failure система возвращается в shadow/close-only, но не повышает риск.

## 7. Hold time

- route-specific dynamic target from convergence distribution;
- canary timeout: 300 seconds;
- first production hard maximum: 24 hours;
- funding/risk deterioration вызывает раннее закрытие;
- max-hold close имеет P2 priority.
