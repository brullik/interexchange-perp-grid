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

- https://www.kucoin.com/docs-new/rest/futures-trading/orders/get-order-list
- https://www.kucoin.com/docs-new/3470082w0
- https://www.kucoin.com/docs-new/3470233w0
- https://www.kucoin.com/docs-new/rest/ua/introduction

UTA notice является hard gate: пока официальный текст запрещает production/live,
использовать UTA для live нельзя.

## MEXC

- https://mexcdevelop.github.io/apidocs/contract_v1_en/

Order/cancel endpoints, помеченные `Under maintenance`, не квалифицируются для live.

## BingX

- https://bingx-api.github.io/docs/
- https://bingx-api.github.io/api-ai-skills/

BingX live остаётся capability-gated до точных contract tests официальных endpoints.
