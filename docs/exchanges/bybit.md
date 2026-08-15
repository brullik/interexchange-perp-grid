# Bybit V5 implementation notes

Checked against official documentation on 2026-08-15.

## Public linear order book

Source: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>

- Subscribe to `orderbook.{depth}.{symbol}` and start only from a `snapshot`.
- A later `snapshot` replaces the entire local book.
- A delta quantity of zero deletes a price; a missing price is inserted; an existing price is
  replaced.
- `u` is the update ID. `u=1` is a service-restart snapshot and replaces the entire local book.
- `seq` is the cross sequence; smaller values represent earlier generated data.
- The native assembler uses depths 50/200/1000 and validates strictly monotonic `u/seq`.
  Bybit does not document adjacent `u` values, so monotonic jumps are accepted. A regression,
  malformed record, or failed parent assembly clears the local book and refuses further deltas
  until a new snapshot; guard state is committed only after successful assembly.

The official Pybit implementation also retains the latest `u` and `seq` while assembling deltas:
<https://github.com/bybit-exchange/pybit/blob/master/pybit/_websocket_stream.py>.

## Account-wide active state

Open orders: <https://bybit-exchange.github.io/docs/v5/order/open-order>

Positions: <https://bybit-exchange.github.io/docs/v5/position>

- Linear account-wide queries use `category=linear` and `settleCoin=USDT` without a symbol.
- Open orders are cursor-paginated with a maximum page size of 50.
- Positions are cursor-paginated with a maximum page size of 200; when `symbol` is omitted and
  `settleCoin` is supplied, only non-zero positions are returned.
- The bounded reconciliation path performs two stable samples, each with one concurrent
  account-wide open-order request and one account-wide position request. A response with an
  explicit continuation cursor or at either documented page limit is marked UNKNOWN instead of
  being represented as complete. A changed sample or intervening private event is also UNKNOWN.
