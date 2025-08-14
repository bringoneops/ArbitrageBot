# ArbitrageBot

This workspace is split into several partitions that cooperate to
aggregate and normalize exchange data:

- **ingestor** – command-line application that orchestrates the data flow.
  It spawns exchange-specific **agents**, receives their raw events,
  converts them into the **canonical** model and forwards the
  normalized events to downstream consumers. No library API is exposed.
- **agents** – library of exchange adapters. Each adapter implements a
  common trait and streams raw [`arb_core`](core/) events through a
  channel.
- **canonical** – defines a stable representation of trades and order
  book updates. It converts raw `arb_core` events into the `MdEvent`
  types used across the system.
- **core** – shared utilities such as event definitions, configuration
  loading, rate limiting and TLS helpers.

The ingestor owns a channel that all agents send `StreamMessage` values
into. A task inside the ingestor receives these messages and uses the
`canonical` crate to transform them into `MdEvent`s, ensuring that every
exchange produces data in a common format.

This project connects to the Binance.US, Binance.com (global), Binance Futures, Binance Delivery, and Binance Options WebSocket APIs, forwarding a wide range of spot and derivative market data streams.
## Runtime Configuration

The binary can be configured via environment variables:

- `SOCKS5_PROXY` – optional `host:port` for routing all HTTP and WebSocket traffic through a SOCKS5 proxy.
- `CHUNK_SIZE` – number of streams per WebSocket connection. Defaults to `100` if unset or invalid.
- `STREAMS_CONFIG` – optional path to a JSON file specifying `global` and `per_symbol` stream lists. If omitted, a built-in `streams.json` configuration is used.
- `SPOT_SYMBOLS` – comma-separated spot symbols to subscribe. Set to `ALL` to auto-discover all trading pairs (may subscribe to a very large number of streams).
- `FUTURES_SYMBOLS` – comma-separated futures symbols. Set to `ALL` to fetch every trading pair, which can significantly increase the subscription load.

Example using a local proxy:

```bash
SOCKS5_PROXY=127.0.0.1:9050 cargo run --release
```

To change the number of streams per connection:

```bash
CHUNK_SIZE=50 cargo run --release
```

## Unknown Events

Any WebSocket message with an unrecognized `e` field is logged at the warning
level along with its raw payload. If a metrics recorder is installed, an
`unknown_events` counter is also incremented so operators can set up alerts for
protocol changes.

## Event Channel and Logging

Parsed WebSocket events are sent over a bounded Tokio `mpsc` channel. The
buffer size is configurable via [`config/default.toml`](config/default.toml)
and defaults to `1024`. Downstream tasks can consume the messages directly:

```rust
let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
tokio::spawn(async move {
    while let Some(event) = rx.recv().await {
        // handle event
    }
});
```

Each handled message increments a `ws_events` metrics counter. Per-event
logging is disabled by default; enable the `debug-logs` feature to log every
event at the `debug` level:

```bash
cargo run --features debug-logs
```

## Default Channels

By default, the aggregator subscribes to the following Binance WebSocket channels
as defined in [`streams.json`](streams.json):

### Global Streams

- `!bookTicker@arr`
- `!markPrice@arr`
- `!markPrice@arr@1s`
- `!miniTicker@arr`
- `!ticker@arr`
- `!ticker_1M@arr`
- `!ticker_1d@arr`
- `!ticker_1h@arr`
- `!ticker_1w@arr`
- `!ticker_4h@arr`
- `forceOrder@arr`

### Per-Symbol Streams

- `aggTrade`
- `bookTicker`
- `continuousKline_12h_current_quarter`
- `continuousKline_12h_next_quarter`
- `continuousKline_12h_perpetual`
- `continuousKline_15m_current_quarter`
- `continuousKline_15m_next_quarter`
- `continuousKline_15m_perpetual`
- `continuousKline_1M_current_quarter`
- `continuousKline_1M_next_quarter`
- `continuousKline_1M_perpetual`
- `continuousKline_1d_current_quarter`
- `continuousKline_1d_next_quarter`
- `continuousKline_1d_perpetual`
- `continuousKline_1h_current_quarter`
- `continuousKline_1h_next_quarter`
- `continuousKline_1h_perpetual`
- `continuousKline_1m_current_quarter`
- `continuousKline_1m_next_quarter`
- `continuousKline_1m_perpetual`
- `continuousKline_1w_current_quarter`
- `continuousKline_1w_next_quarter`
- `continuousKline_1w_perpetual`
- `continuousKline_2h_current_quarter`
- `continuousKline_2h_next_quarter`
- `continuousKline_2h_perpetual`
- `continuousKline_30m_current_quarter`
- `continuousKline_30m_next_quarter`
- `continuousKline_30m_perpetual`
- `continuousKline_3d_current_quarter`
- `continuousKline_3d_next_quarter`
- `continuousKline_3d_perpetual`
- `continuousKline_3m_current_quarter`
- `continuousKline_3m_next_quarter`
- `continuousKline_3m_perpetual`
- `continuousKline_4h_current_quarter`
- `continuousKline_4h_next_quarter`
- `continuousKline_4h_perpetual`
- `continuousKline_5m_current_quarter`
- `continuousKline_5m_next_quarter`
- `continuousKline_5m_perpetual`
- `continuousKline_6h_current_quarter`
- `continuousKline_6h_next_quarter`
- `continuousKline_6h_perpetual`
- `continuousKline_8h_current_quarter`
- `continuousKline_8h_next_quarter`
- `continuousKline_8h_perpetual`
- `depth@100ms`
- `depth5@100ms`
- `depth10@100ms`
- `depth20@100ms`
- `forceOrder`
- `indexPrice`
- `indexPrice@1s`
- `indexPriceKline_12h`
- `indexPriceKline_15m`
- `indexPriceKline_1M`
- `indexPriceKline_1d`
- `indexPriceKline_1h`
- `indexPriceKline_1m`
- `indexPriceKline_1w`
- `indexPriceKline_2h`
- `indexPriceKline_30m`
- `indexPriceKline_3d`
- `indexPriceKline_3m`
- `indexPriceKline_4h`
- `indexPriceKline_5m`
- `indexPriceKline_6h`
- `indexPriceKline_8h`
- `kline_12h`
- `kline_15m`
- `kline_1M`
- `kline_1d`
- `kline_1h`
- `kline_1m`
- `kline_1w`
- `kline_2h`
- `kline_30m`
- `kline_3d`
- `kline_3m`
- `kline_4h`
- `kline_5m`
- `kline_6h`
- `kline_8h`
- `markPrice`
- `markPrice@1s`
- `markPriceKline_12h`
- `markPriceKline_15m`
- `markPriceKline_1M`
- `markPriceKline_1d`
- `markPriceKline_1h`
- `markPriceKline_1m`
- `markPriceKline_1w`
- `markPriceKline_2h`
- `markPriceKline_30m`
- `markPriceKline_3d`
- `markPriceKline_3m`
- `markPriceKline_4h`
- `markPriceKline_5m`
- `markPriceKline_6h`
- `markPriceKline_8h`
- `miniTicker`
- `openInterest`
- `takerBuySellVolume`
- `ticker`
- `ticker_1M`
- `ticker_1d`
- `ticker_1h`
- `ticker_1w`
- `ticker_4h`
- `topLongShortAccountRatio`
- `topLongShortPositionRatio`
- `trade`

### Options Streams

The aggregator supports Binance Options channels defined in [`streams_options.json`](streams_options.json):

#### Global Streams

- `!bookTicker@arr`
- `!miniTicker@arr`
- `!ticker@arr`

#### Per-Symbol Streams

- `bookTicker`
- `miniTicker`
- `ticker`
- `trade`
- `kline_1m`
- `kline_5m`
- `kline_15m`
- `kline_30m`
- `kline_1h`
- `kline_2h`
- `kline_4h`
- `kline_1d`
- `kline_1w`
- `greeks`
- `openInterest`
- `impliedVolatility`

