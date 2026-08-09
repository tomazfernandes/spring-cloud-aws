# SQS Virtual Threads Performance Results

Results from `SqsPerformanceTests` comparing default (platform threads) vs virtual threads.

| Test | Infra | Messages | Concurrent | Load | Throughput (default) | Throughput (VT) | p50 (default) | p50 (VT) |
|------|-------|----------|-----------|------|---------------------|----------------|--------------|----------|
| No load | LocalStack | 200 | 10 | - | 1,786 msg/s | 2,439 msg/s | 0ms | 0ms |
| 1s load | LocalStack | 50 | 10 | 1s | 10 msg/s | 10 msg/s | 1,003ms | 1,002ms |
| 500 concurrent, 1s load | LocalStack | 500 | 500 | 1s | 242 msg/s | 242 msg/s | 1,001ms | 1,001ms |
| 10k concurrent, 1s load | LocalStack | 10,000 | 10,000 | 1s | - | 3,235 msg/s | - | 1,000ms |
| 2k concurrent, 2s load | AWS | 2,000 | 2,000 | 2s | 304 msg/s | 339 msg/s | 2,004ms | 2,000ms |
| 10k concurrent, 1s load | AWS | 10,000 | 10,000 | 1s | - | 1,939 msg/s | - | 1,000ms |
| Blocking interceptor (200ms) + 1s load | LocalStack | 50 | 10 | 1s | - | 8 msg/s | - | 1,208ms |

## Key Takeaways

- At low concurrency, virtual threads and platform threads perform identically
- At 2k concurrent against AWS, virtual threads are ~11% faster with tighter p50 latency
- 10k concurrent virtual threads work cleanly — platform threads can't realistically reach that level
- Blocking interceptors work correctly with virtual threads (latency reflects interceptor + listener delay)
- The bottleneck at high concurrency is the AWS SDK HTTP connection pool, not the thread model
