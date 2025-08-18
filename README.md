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
  types used across the system. The schema is versioned with the
  `SCHEMA_VERSION` constant in `canonical/src/lib.rs`; bump this value
  whenever the canonical structs change.
- **core** – shared utilities such as event definitions, configuration
  loading, rate limiting and TLS helpers.

The ingestor owns a channel that all agents send `StreamMessage` values
into. A task inside the ingestor receives these messages and uses the
`canonical` crate to transform them into `MdEvent`s, ensuring that every
exchange produces data in a common format.

This project connects to the Binance.US, Binance.com (global), Binance Futures, Binance Delivery, and Binance Options WebSocket APIs, forwarding a wide range of spot and derivative market data streams.

## Supported Exchanges & Channels

- **BingX:** `depth`, `trades`, `ticker`, `kline`
- **Bitget:** `depth`, `trade`, `ticker`
- **Bitmart Spot:** `ticker`, `kline_1m`, `kline_5m`, `kline_15m`, `kline_30m`, `kline_1h`, `kline_4h`, `kline_1d`, `kline_1w`, `kline_1M`, `depth5`, `depth20`, `trade`
- **Bitmart Contract:** all spot channels above plus `funding_rate`, `futures/depthIncrease5`, `futures/depthIncrease20`
- **CoinEx Spot & Perpetual:** `depth.subscribe`, `deals.subscribe`, `state.subscribe`, `kline.subscribe`
- **Gate.io Spot/Futures:** `order_book_update`, `trades`, `tickers`, `kline.subscribe`
- **KuCoin Spot:** global `/market/ticker:all`, `/market/snapshot:all`; per-symbol `/market/ticker`, `/market/snapshot`, `/market/level2`, `/market/level2Depth5`, `/market/level2Depth50`, `/market/match`
- **KuCoin Futures:** global `/contractMarket/ticker:all`; per-symbol `/contractMarket/ticker`, `/contractMarket/level2`, `/contractMarket/level2Depth5`, `/contractMarket/level2Depth50`, `/contractMarket/execution`, `/contractMarket/indexPrice`, `/contractMarket/markPrice`, `/contractMarket/fundingRate`, `/contractMarket/candles:*`
- **Latoken:** `trades`, `ticker`, `orderbook`
- **LBank:** `ticker`, `trade`, `depth`, `kline_1m`, `kline_3m`, `kline_5m`, `kline_15m`, `kline_30m`, `kline_1h`, `kline_2h`, `kline_4h`, `kline_6h`, `kline_12h`, `kline_1d`, `kline_1w`, `kline_1M`
- **XT:** `depth`, `trade`, `kline`, `ticker`
- **Binance Spot:** global `!bookTicker@arr`, `!miniTicker@arr`, `!ticker@arr`, `!ticker_1M@arr`, `!ticker_1d@arr`, `!ticker_1h@arr`, `!ticker_1w@arr`, `!ticker_4h@arr`; per-symbol `aggTrade`, `bookTicker`, `depth@100ms`, `kline_1m`, `kline_3m`, `kline_5m`, `kline_15m`, `kline_30m`, `kline_1h`, `kline_2h`, `kline_4h`, `kline_6h`, `kline_8h`, `kline_12h`, `kline_1d`, `kline_3d`, `kline_1w`, `kline_1M`, `miniTicker`, `ticker`, `ticker_1h`, `ticker_4h`, `ticker_1d`, `ticker_1w`, `ticker_1M`, `trade`
- **Binance Futures:** global `!bookTicker@arr`, `!markPrice@arr`, `!markPrice@arr@1s`, `!miniTicker@arr`, `!ticker@arr`, `!ticker_1M@arr`, `!ticker_1d@arr`, `!ticker_1h@arr`, `!ticker_1w@arr`, `!ticker_4h@arr`, `forceOrder@arr`; per-symbol all `continuousKline_*`, `depth*`, `forceOrder`, `indexPrice*`, `kline_*`, `markPrice*`, `miniTicker`, `openInterest`, `takerBuySellVolume`, `ticker_*`, `trade`
- **Binance Options:** global `!bookTicker@arr`, `!miniTicker@arr`, `!ticker@arr`; per-symbol `bookTicker`, `miniTicker`, `ticker`, `trade`, `kline_1m`, `kline_5m`, `kline_15m`, `kline_30m`, `kline_1h`, `kline_2h`, `kline_4h`, `kline_1d`, `kline_1w`, `greeks`, `openInterest`, `impliedVolatility`

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for coding style, testing, and PR guidelines.

## Building and Testing

Compile all crates in the workspace:

```bash
cargo build
```

Run the full test suite:

```bash
cargo test
```

### Feature Flags

Optional capabilities are gated behind Cargo features. Enable them with the
`--features` flag:

```bash
cargo run --features debug-logs
cargo build --features "debug-logs prometheus-exporter"
```

Use `--all-features` to activate every flag.

### Coverage

Generate code coverage reports with
[cargo-tarpaulin](https://crates.io/crates/cargo-tarpaulin):

```bash
cargo install cargo-tarpaulin
cargo tarpaulin
```

### Benchmarks

Criterion benchmarks live under `ingestor/benches`. Execute them with:

```bash
cargo bench
```

## Runtime Configuration

The binary can be configured via environment variables:

- `SOCKS5_PROXY` – optional `host:port` for routing all HTTP and WebSocket traffic through a SOCKS5 proxy.
- `MD_SINK_FILE` – path to a JSON Lines file where normalized market data events are written.
- `CHUNK_SIZE` – number of streams per WebSocket connection. Defaults to `100` if unset or invalid.
- `STREAMS_CONFIG` – optional path to a JSON file specifying `global` and `per_symbol` stream lists. If omitted, a built-in `streams/binance_futures.json` configuration is used.
- `SPOT_SYMBOLS` – comma-separated spot symbols to subscribe. Set to `ALL` to auto-discover all trading pairs (may subscribe to a very large number of streams).
- `FUTURES_SYMBOLS` – comma-separated futures symbols. Set to `ALL` to fetch every trading pair, which can significantly increase the subscription load.
- `HTTP_BURST` – maximum number of HTTP requests allowed to accumulate. Defaults to `10`.
- `HTTP_REFILL_PER_SEC` – HTTP token refill rate per second. Defaults to `10`.
- `WS_BURST` – maximum burst of WebSocket messages. Defaults to `5`.
- `WS_REFILL_PER_SEC` – WebSocket token refill rate per second. Defaults to `5`.
- `CERT_PINS` – comma-separated list of SHA-256 certificate fingerprints in hex.

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

Parsed WebSocket events are now partitioned across channels keyed by
`<exchange>:<symbol>`. Each partition has its own bounded Tokio `mpsc`
queue, allowing consumers to normalize events in parallel and improving
throughput under heavy load. The buffer size for each partition is
configurable via [`config/default.toml`](config/default.toml) and
defaults to `1024`.

To consume a specific partition, obtain the corresponding receiver and
spawn a task:

```rust
// channel map is keyed by "Exchange:SYMBOL"
let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
channels.insert("Binance:BTCUSDT".into(), tx);
tokio::spawn(async move {
    while let Some(event) = rx.recv().await {
        // handle event
    }
});
```

Each handled message increments an `md_events_total` metrics counter. Per-event
logging is disabled by default; set `RUST_LOG=debug` and enable the
`debug-logs` feature to log every event at the `debug` level:

```bash
RUST_LOG=debug cargo run --features debug-logs
```

### Tuning

The total number of partitions equals the number of enabled symbol and
exchange combinations. Increasing the `event_buffer_size` can smooth out
bursts at the cost of memory. Adjusting `CHUNK_SIZE` controls how many
streams share a WebSocket connection, trading connection count for per
stream latency.

## Default Channels

By default, the aggregator subscribes to the following Binance WebSocket channels
as defined in [`streams/binance_futures.json`](streams/binance_futures.json):

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
- `openInterest`
- `takerBuySellVolume`
- `ticker`
- `ticker_1M`
- `ticker_1d`
- `ticker_1h`
- `ticker_1w`
- `ticker_4h`
- `trade`

### Options Streams

The aggregator supports Binance Options channels defined in [`streams/binance_options.json`](streams/binance_options.json):

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

