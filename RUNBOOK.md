# Runbook

This document outlines common development and runtime commands for the ArbitrageBot workspace.

## Code Quality

### Formatting
Format the entire workspace:
```bash
cargo fmt --all
```

### Clippy
Run the linter and fail on warnings:
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Security Audit
Check dependencies for known vulnerabilities:
```bash
cargo audit
```

## Testing

### Unit and Integration Tests
Execute tests with the `nextest` runner:
```bash
cargo nextest run
```

### Chaos Tests
Run the subset of tests tagged for chaos scenarios:
```bash
cargo nextest run chaos
```

## Benchmarks
Run Criterion benchmarks located under `ingestor/benches`:
```bash
cargo bench
```

## Coverage
Generate code coverage reports using Tarpaulin:
```bash
cargo tarpaulin
```

## Running the Ingestor
Start the ingestor and write normalized market data to a file:
```bash
MD_SINK_FILE=output.jsonl cargo run -p ingestor
```
