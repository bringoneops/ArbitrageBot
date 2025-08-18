# Risk Register

The table below outlines key operational risks for ArbitrageBot and the strategies for mitigating them.

| Risk | Mitigation |
| --- | --- |
| Snapshot depth | Configure sufficient order book depth and monitor for truncation to avoid missing price levels. |
| Clock drift | Sync system time with NTP and alert when drift exceeds a threshold to maintain timestamp accuracy. |
| Sink outages | Buffer events to a write-ahead log and retry or fail over to secondary sinks during outages. |
| Rate-limit changes | Detect rate-limit responses, back off automatically, and update configuration when exchange limits change. |
| Storage growth | Compress or rotate logs and implement retention policies to control long-term storage usage. |

