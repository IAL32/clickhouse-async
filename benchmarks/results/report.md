# Benchmark report
## Environment
| Field | Value |
|---|---|
| Run timestamp (UTC) | 2026-05-07T07:11:54Z |
| OS | Darwin 25.2.0 arm64 |
| CPU | Apple M3 Pro — 11P/11L cores |
| RAM | 36.0 GiB |
| Power source | n/a |
| Python | 3.14.2 |
| Docker | 29.4.0 |
| ClickHouse server | 26.3.9.8 |
| clickhouse-async | 0.3.3 |
| asynch (PyPI) | 0.3.1 |
| asynch (tacto fork) | 0.3.1 |
| clickhouse-connect (thread-pool) | 0.15.1 |
| clickhouse-connect (native async) | 1.0.0rc2 |
| Repo SHA | 349df5a |
| Working tree | dirty |

## Results
### Ping latency (`SELECT 1`)

_Latency (ms, lower is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 200 | 1.027 | 1.919 | 3.866 | 0.920 | 5.365 | 1.177 |
| asynch (PyPI) | 200 | 1.470 | 1.637 | 1.748 | 1.247 | 2.147 | 1.472 |
| asynch (tacto fork) | 200 | 1.209 | 1.342 | 1.391 | 1.105 | 1.869 | 1.221 |
| clickhouse-connect (thread-pool) | 200 | 2.109 | 3.621 | 3.888 | 1.328 | 4.024 | 2.339 |
| clickhouse-connect (native async) | 200 | 1.185 | 1.774 | 2.328 | 0.910 | 2.430 | 1.282 |

![ping_latency](ping_latency.png)

### Read throughput (1M rows scanned)

_Throughput (rows/sec, higher is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 626,556 | 628,600 | 628,757 | 624,033 | 628,796 | 626,336 |
| asynch (PyPI) | 10 | 504,120 | 508,780 | 509,523 | 492,902 | 509,708 | 503,646 |
| asynch (tacto fork) | 10 | 506,765 | 511,206 | 511,681 | 501,309 | 511,800 | 506,718 |
| clickhouse-connect (thread-pool) | 10 | 924,310 | 937,440 | 939,335 | 907,468 | 939,809 | 923,153 |
| clickhouse-connect (native async) | 10 | 3,163,793 | 3,214,751 | 3,225,365 | 3,078,983 | 3,228,019 | 3,156,250 |

![read_throughput](read_throughput.png)

### Insert throughput (100k bulk INSERT)

_Throughput (rows/sec, higher is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 758,518 | 797,329 | 806,089 | 730,438 | 808,279 | 761,059 |
| asynch (PyPI) | 10 | 807,326 | 827,141 | 827,158 | 764,523 | 827,162 | 801,515 |
| asynch (tacto fork) | 10 | 814,674 | 841,499 | 852,086 | 736,464 | 854,733 | 803,607 |
| clickhouse-connect (thread-pool) | 10 | 1,078,738 | 1,182,979 | 1,187,870 | 992,850 | 1,189,093 | 1,093,029 |
| clickhouse-connect (native async) | 10 | 1,045,486 | 1,098,717 | 1,099,214 | 944,010 | 1,099,339 | 1,029,287 |

![insert_throughput](insert_throughput.png)

### Concurrent reads (16-way fan-out)

_Total wall time (ms, lower is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 13.403 | 14.153 | 14.254 | 11.382 | 14.279 | 13.073 |
| asynch (PyPI) | 10 | 13.481 | 16.669 | 17.654 | 12.096 | 17.901 | 13.772 |
| asynch (tacto fork) | 10 | 13.653 | 15.331 | 15.344 | 12.066 | 15.347 | 13.737 |
| clickhouse-connect (thread-pool) | 10 | 11.877 | 14.282 | 14.772 | 11.236 | 14.894 | 12.261 |
| clickhouse-connect (native async) | 10 | 47.943 | 50.391 | 50.470 | 45.758 | 50.490 | 48.005 |

![concurrent_reads](concurrent_reads.png)

### RSS over time (5M-row read)

_Resident set size (MiB) over wall-clock time (ms)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 1 | 528.8 | 528.8 | 528.8 | 528.8 | 528.8 | 528.8 |
| asynch (PyPI) | 1 | 506.7 | 506.7 | 506.7 | 506.7 | 506.7 | 506.7 |
| asynch (tacto fork) | 1 | 506.7 | 506.7 | 506.7 | 506.7 | 506.7 | 506.7 |
| clickhouse-connect (thread-pool) | 1 | 683.0 | 683.0 | 683.0 | 683.0 | 683.0 | 683.0 |
| clickhouse-connect (native async) | 1 | 571.4 | 571.4 | 571.4 | 571.4 | 571.4 | 571.4 |

![memory_ceiling](memory_ceiling.png)

