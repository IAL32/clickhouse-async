# Benchmark report
## Environment
| Field | Value |
|---|---|
| Run timestamp (UTC) | 2026-05-07T20:48:40Z |
| OS | Darwin 25.2.0 arm64 |
| CPU | Apple M3 Pro — 11P/11L cores |
| RAM | 36.0 GiB |
| Power source | n/a |
| Python | 3.14.2 |
| Docker | 29.4.0 |
| ClickHouse server | 26.3.9.8 |
| clickhouse-async | 0.4.1 |
| asynch (PyPI) | 0.3.1 |
| asynch (tacto fork) | 0.3.1 |
| clickhouse-connect (thread-pool) | 0.15.1 |
| clickhouse-connect (native async) | 1.0.0rc2 |
| Repo SHA | fe0db06 |
| Working tree | dirty |

## Results
### Ping latency (`SELECT 1`)

_Latency (ms, lower is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 200 | 0.703 | 0.782 | 0.811 | 0.619 | 0.864 | 0.709 |
| asynch (PyPI) | 200 | 1.101 | 1.313 | 1.387 | 0.957 | 1.412 | 1.126 |
| asynch (tacto fork) | 200 | 0.946 | 1.071 | 1.206 | 0.863 | 1.333 | 0.966 |
| clickhouse-connect (thread-pool) | 200 | 1.969 | 3.135 | 3.898 | 1.047 | 4.869 | 2.007 |
| clickhouse-connect (native async) | 200 | 1.035 | 2.122 | 2.339 | 0.661 | 2.800 | 1.187 |

![ping_latency](ping_latency.png)

### Read throughput (1M rows scanned)

_Throughput (rows/sec, higher is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 3,787,324 | 3,849,504 | 3,853,245 | 3,740,211 | 3,854,180 | 3,790,132 |
| asynch (PyPI) | 10 | 677,885 | 689,154 | 689,931 | 657,315 | 690,126 | 675,931 |
| asynch (tacto fork) | 10 | 693,343 | 699,700 | 700,332 | 669,154 | 700,490 | 688,095 |
| clickhouse-connect (thread-pool) | 10 | 1,285,612 | 1,322,677 | 1,333,001 | 1,223,112 | 1,335,582 | 1,281,267 |
| clickhouse-connect (native async) | 10 | 5,083,360 | 5,208,134 | 5,218,870 | 4,941,671 | 5,221,554 | 5,074,618 |

![read_throughput](read_throughput.png)

### Insert throughput (100k bulk INSERT)

_Throughput (rows/sec, higher is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 1,006,725 | 1,194,855 | 1,250,411 | 932,046 | 1,264,300 | 1,028,232 |
| asynch (PyPI) | 10 | 881,372 | 938,633 | 943,809 | 832,191 | 945,103 | 883,223 |
| asynch (tacto fork) | 10 | 861,175 | 907,467 | 911,354 | 782,202 | 912,326 | 854,425 |
| clickhouse-connect (thread-pool) | 10 | 1,079,291 | 1,140,388 | 1,148,762 | 1,001,612 | 1,150,856 | 1,082,205 |
| clickhouse-connect (native async) | 10 | 1,071,904 | 1,290,297 | 1,303,632 | 929,499 | 1,306,965 | 1,095,810 |

![insert_throughput](insert_throughput.png)

### Concurrent reads (16-way fan-out)

_Total wall time (ms, lower is better)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 10 | 9.544 | 12.359 | 12.645 | 8.989 | 12.717 | 10.185 |
| asynch (PyPI) | 10 | 12.751 | 13.541 | 13.647 | 10.809 | 13.674 | 12.587 |
| asynch (tacto fork) | 10 | 11.242 | 12.605 | 12.735 | 9.542 | 12.768 | 11.241 |
| clickhouse-connect (thread-pool) | 10 | 9.182 | 10.454 | 10.532 | 8.456 | 10.552 | 9.426 |
| clickhouse-connect (native async) | 10 | 41.385 | 42.896 | 42.954 | 37.040 | 42.968 | 40.754 |

![concurrent_reads](concurrent_reads.png)

### RSS over time (5M-row read)

_Resident set size (MiB) over wall-clock time (ms)_

| Library | n | p50 | p95 | p99 | min | max | mean |
|---|---|---|---|---|---|---|---|
| clickhouse-async | 1 | 519.2 | 519.2 | 519.2 | 519.2 | 519.2 | 519.2 |
| asynch (PyPI) | 1 | 498.8 | 498.8 | 498.8 | 498.8 | 498.8 | 498.8 |
| asynch (tacto fork) | 1 | 485.4 | 485.4 | 485.4 | 485.4 | 485.4 | 485.4 |
| clickhouse-connect (thread-pool) | 1 | 681.2 | 681.2 | 681.2 | 681.2 | 681.2 | 681.2 |
| clickhouse-connect (native async) | 1 | 585.9 | 585.9 | 585.9 | 585.9 | 585.9 | 585.9 |

![memory_ceiling](memory_ceiling.png)

