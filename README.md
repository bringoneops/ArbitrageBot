# Binance Stream Aggregator

This project connects to the Binance.US, Binance.com (global), Binance Futures, Binance Delivery, and Binance Options WebSocket APIs, forwarding a wide range of spot and derivative market data streams.

## Runtime Configuration

The binary can be configured via environment variables:

- `SOCKS5_PROXY` – optional `host:port` for routing all HTTP and WebSocket traffic through a SOCKS5 proxy.
- `CHUNK_SIZE` – number of streams per WebSocket connection. Defaults to `100` if unset or invalid.
- `STREAMS_CONFIG` – optional path to a JSON file specifying `global` and `per_symbol` stream lists. If omitted, a built-in `streams.json` configuration is used.

Example using a local proxy:

```bash
SOCKS5_PROXY=127.0.0.1:9050 cargo run --release
```

To change the number of streams per connection:

```bash
CHUNK_SIZE=50 cargo run --release
```


