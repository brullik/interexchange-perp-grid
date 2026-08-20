# Нормативные официальные источники API

Codex обязан перепроверять эти источники перед реализацией каждого adapter и
сохранять дату проверки. Нельзя использовать блог/форум как source of truth.

## Binance

- https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/Introduction
- https://developers.binance.com/en/docs/catalog
- https://developers.binance.com/en/docs/agent-native/llms-txt

## Bybit

- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- https://bybit-exchange.github.io/docs/v5/websocket/private/order
- https://bybit-exchange.github.io/docs/v5/websocket/private/position
- https://bybit-exchange.github.io/docs/v5/position
- https://bybit-exchange.github.io/docs/v5/order/order-list

## OKX

- https://www.okx.com/docs-v5/trick_en/
- https://www.okx.com/docs-v5/en/

## Bitget

- Последняя перепроверка Classic USDT-FUTURES: 2026-08-20. UTA не квалифицирован.
- https://www.bitget.com/api-doc/classic/contract/websocket/public/Tickers-Channel
- https://www.bitget.com/api-doc/classic/contract/websocket/public/Order-Book-Channel
- https://www.bitget.com/api-doc/classic/contract/trade/Get-Orders-Pending
- https://www.bitget.com/api-doc/classic/contract/position/get-all-position
- https://www.bitget.com/api-doc/classic/contract/trade/Place-Order
- https://www.bitget.com/api-doc/contract/websocket/private/Order-Channel
- https://www.bitget.com/api-doc/classic/contract/websocket/private/Positions-Channel

Pinned CCXT Pro 4.5.58 не реализует matching unsubscribe для Classic batch ticker;
локальный override обязан использовать официальный `op=unsubscribe` с теми же topics.
Live остаётся disabled до независимых account/shadow/canary/reconciliation gates.

## KuCoin

- Последняя перепроверка KuCoin Futures Classic: 2026-08-20. UTA не квалифицирован.
- https://www.kucoin.com/docs-new/3470080w0
- https://www.kucoin.com/docs-new/3470097w0
- https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order
- https://www.kucoin.com/docs-new/rest/futures-trading/orders/get-order-list
- https://www.kucoin.com/docs-new/3470082w0
- https://www.kucoin.com/docs-new/3470090w0
- https://www.kucoin.com/docs-new/3470092w0
- https://www.kucoin.com/docs-new/3470093w0
- https://www.kucoin.com/docs-new/3470233w0
- https://www.kucoin.com/docs-new/rest/ua/introduction

Pinned CCXT Pro 4.5.58 реализует batch Futures BBO subscribe, но не matching
`unWatchBidsAsks`; локальный Classic-only override обязан отправлять тот же
`/contractMarket/tickerV2:{symbols}` topic с `type=unsubscribe`, не более 100 symbols
в одном batch. UTA notice является hard gate: пока официальный текст запрещает
production/live, использовать UTA для live нельзя.

## MEXC

- https://mexcdevelop.github.io/apidocs/contract_v1_en/

Order/cancel endpoints, помеченные `Under maintenance`, не квалифицируются для live.

## BingX

- https://bingx-api.github.io/docs/
- https://bingx-api.github.io/api-ai-skills/
- https://github.com/BingX-API/api-ai-skills/blob/main/skills/references/websocket.md
- https://github.com/BingX-API/api-ai-skills/blob/main/skills/swap-market/api-reference.md
- https://github.com/BingX-API/api-ai-skills/blob/main/skills/swap-trade/SKILL.md

USDT-M использует один GZIP/Ping-capable swap WebSocket endpoint; публичные
`{symbol}@bookTicker` и `{symbol}@incrDepth` подписываются и отменяются точными
`sub`/`unsub` frames. Первый incremental-depth event — `action=all`, а каждый
последующий `lastUpdateId` обязан быть равен предыдущему + 1. Contract info
публикует `tradeMinQuantity` и `tradeMinUSDT`; protected orders поддерживают
IOC, `clientOrderID` и `positionSide`. Pinned CCXT не переносит sequence из
ограниченного depth snapshot, а официальный WS документирует только per-symbol
BBO. Поэтому Phase 5.3 добавляет лишь узкий sequenced-L2 override и отклоняет
broad-BBO capability вместо запрещённого unbounded per-symbol fallback. BingX
live остаётся capability-gated и вне Wave 1 canary allowlist.
