# Binance Stream Aggregator

This project connects to the Binance.US, Binance.com (global), and Binance Futures WebSocket APIs, forwarding a wide range of spot and derivative market data streams.

## Runtime Configuration

The binary can be configured via environment variables:

- `SOCKS5_PROXY` – optional `host:port` for routing all HTTP and WebSocket traffic through a SOCKS5 proxy.
- `CHUNK_SIZE` – number of streams per WebSocket connection. Defaults to `100` if unset or invalid.

Example using a local proxy:

```bash
SOCKS5_PROXY=127.0.0.1:9050 cargo run --release
```

To change the number of streams per connection:

```bash
CHUNK_SIZE=50 cargo run --release
```


