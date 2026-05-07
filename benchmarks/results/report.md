# Benchmark report
## Environment
| Field | Value |
|---|---|
| Run timestamp (UTC) | 2026-05-07T18:54:25Z |
| OS | Darwin 25.2.0 arm64 |
| CPU | Apple M3 Pro — 11P/11L cores |
| RAM | 36.0 GiB |
| Power source | n/a |
| Python | 3.14.2 |
| Docker | 29.4.0 |
| ClickHouse server | 26.3.9.8 |
| clickhouse-async | 0.4.0 |
| asynch (PyPI) | 0.3.1 |
| asynch (tacto fork) | 0.3.1 |
| clickhouse-connect (thread-pool) | 0.15.1 |
| clickhouse-connect (native async) | 1.0.0rc2 |
| Repo SHA | 389e33f |
| Working tree | dirty |

## Results
### Ping latency (`SELECT 1`)

_Latency (ms, lower is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 200 | 0.905 | 1.014 | 1.048 | 0.803 | 1.064 | 0.913 |
| asynch (PyPI) | 200 | 1.344 | 1.700 | 1.972 | 1.116 | 2.430 | 1.366 |
| asynch (tacto fork) | 200 | 1.225 | 1.363 | 1.493 | 1.101 | 1.901 | 1.237 |
| clickhouse-connect (thread-pool) | 200 | 2.077 | 3.334 | 3.620 | 1.151 | 5.516 | 2.158 |
| clickhouse-connect (native async) | 200 | 1.480 | 2.247 | 3.796 | 0.979 | 4.469 | 1.573 |

![ping_latency](ping_latency.png)

### Read throughput (1M rows scanned)

_Throughput (rows/sec, higher is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 1,471,053 | 1,525,535 | 1,546,995 | 1,436,802 | 1,552,359 | 1,473,459 |
| asynch (PyPI) | 10 | 503,713 | 508,183 | 508,732 | 495,049 | 508,869 | 503,240 |
| asynch (tacto fork) | 10 | 501,230 | 507,371 | 508,745 | 498,070 | 509,088 | 501,871 |
| clickhouse-connect (thread-pool) | 10 | 933,997 | 956,382 | 966,037 | 920,599 | 968,451 | 936,316 |
| clickhouse-connect (native async) | 10 | 3,063,590 | 3,116,243 | 3,140,682 | 3,003,083 | 3,146,792 | 3,053,838 |

![read_throughput](read_throughput.png)

### Insert throughput (100k bulk INSERT)

_Throughput (rows/sec, higher is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 741,776 | 776,644 | 777,463 | 710,895 | 777,667 | 748,350 |
| asynch (PyPI) | 10 | 778,807 | 802,262 | 809,619 | 719,807 | 811,458 | 773,841 |
| asynch (tacto fork) | 10 | 763,834 | 800,360 | 807,879 | 736,857 | 809,758 | 766,417 |
| clickhouse-connect (thread-pool) | 10 | 1,015,549 | 1,043,409 | 1,045,537 | 928,061 | 1,046,069 | 1,004,299 |
| clickhouse-connect (native async) | 10 | 961,974 | 1,077,510 | 1,095,149 | 899,554 | 1,099,559 | 978,442 |

![insert_throughput](insert_throughput.png)

### Concurrent reads (16-way fan-out)

_Total wall time (ms, lower is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 12.303 | 13.849 | 14.362 | 11.203 | 14.490 | 12.439 |
| asynch (PyPI) | 10 | 13.309 | 14.502 | 14.570 | 12.540 | 14.587 | 13.441 |
| asynch (tacto fork) | 10 | 14.804 | 16.721 | 16.955 | 12.058 | 17.014 | 14.725 |
| clickhouse-connect (thread-pool) | 10 | 12.009 | 14.033 | 15.118 | 11.218 | 15.389 | 12.218 |
| clickhouse-connect (native async) | 10 | 52.503 | 56.988 | 58.246 | 47.500 | 58.561 | 51.971 |

![concurrent_reads](concurrent_reads.png)

### RSS over time (5M-row read)

_Resident set size (MiB) over wall-clock time (ms)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 1 | 518.9 | 518.9 | 518.9 | 518.9 | 518.9 | 518.9 |
| asynch (PyPI) | 1 | 505.4 | 505.4 | 505.4 | 505.4 | 505.4 | 505.4 |
| asynch (tacto fork) | 1 | 506.8 | 506.8 | 506.8 | 506.8 | 506.8 | 506.8 |
| clickhouse-connect (thread-pool) | 1 | 683.1 | 683.1 | 683.1 | 683.1 | 683.1 | 683.1 |
| clickhouse-connect (native async) | 1 | 585.9 | 585.9 | 585.9 | 585.9 | 585.9 | 585.9 |

![memory_ceiling](memory_ceiling.png)

