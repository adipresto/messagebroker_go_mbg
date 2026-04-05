# MBG Performance Report (Reactive v0.9.2)

## Metrics Summary
- **Throughput**: 337.28 RPS
- **Processed**: 10,128 Messages (30s)
- **Success Rate**: 100%
- **Latency**:
  - Avg: 29.22ms
  - P95: 58.42ms
  - Max: 130.53ms

## Efficiency Status
- **Memory**: O(1) Snapshot (iterasi tanpa alokasi slice baru).
- **CPU**: Event-driven (wake up hanya saat Push/Retry).
- **Retry**: O(log N) Min-Heap scheduling.
- **Observability**: Metrics available at `/metrics`, pprof at `/debug/pprof/`.
