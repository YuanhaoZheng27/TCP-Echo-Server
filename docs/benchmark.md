# Performance Benchmark

## Purpose

The benchmark evaluates the pre-refactoring and refactored TCP Echo Server using
the same workload, the same Python client tool, and the same release compiler
flags. It measures steady-state echo behavior on a single Linux VPS.

These results characterize one loopback test environment. They are not a
theoretical Server limit and do not establish production capacity.

## Environment

| Item | Value |
|---|---|
| OS | Ubuntu 24.04.4 LTS (Noble Numbat), `x86_64` |
| Kernel version | `6.8.0-124-generic` |
| CPU model | AMD EPYC 7K83 64-Core Processor |
| Exposed logical CPUs | 2 |
| CPU topology | 1 socket, 1 core per socket, 2 threads per core, 1 NUMA node |
| CPU caches | L1d 32 KiB, L1i 32 KiB, L2 512 KiB, L3 32 MiB (reported totals) |
| Virtualization | KVM, full virtualization |
| VPS memory | 1.9 GiB visible; 1.4 GiB available when the environment was captured |
| Swap | 1.9 GiB visible |
| CPU governor | Not exposed; the cpufreq `scaling_governor` path was unavailable |
| Network path | Loopback (`127.0.0.1`) |
| Client | Python standard library |
| Compiler | `g++ (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0` |
| Python version | Python 3.12.3 |
| CMake version | 3.28.3 |
| Compiler flags | `-O2 -DNDEBUG` |
| Test date | November 24, 2026 |

The environment was captured on the benchmark host. Virtualized CPU quota,
neighbor load, and detailed kernel/network tuning were not recorded. No CPU
governor state is inferred because the requested cpufreq interface was not
present.

## Versions Compared

- **Baseline**: the implementation before the `TcpServer` refactoring.
- **Refactor**: the current `src/` implementation with `TcpServer`,
  `TcpServer.h`, and `TcpServer.cpp`.

The refactoring changed code organization and FD ownership. It did not add a
new network mechanism or a deliberate performance optimization.

## Integration Validation

Both versions passed the same 12 integration tests before benchmarking:

| Stage | Result | Runtime |
|---|---:|---:|
| Baseline, pre-refactoring | 12/12 passed | 3.361 s |
| Regression, refactored | 12/12 passed | 3.350 s |

The strace checks observed:

| Stage | Partial Sends | Send EAGAIN | EPOLLOUT MOD |
|---|---:|---:|---:|
| Baseline, pre-refactoring | 659 | 2 | 2 |
| Regression, refactored | 648 | 3 | 3 |

Both runs verified that Send EAGAIN was followed by an `EPOLLOUT` event update
on the same FD. Trace counts vary with scheduling and socket timing.

## Echo Request Definition

The server has no application-level protocol. The benchmark defines one logical
request as:

> Send exactly N bytes on one TCP connection, then receive and validate exactly
> N echoed bytes before starting another request on that connection.

The client:

- Handles partial writes with a `send()` loop.
- Handles partial reads with `recv_into()` until N bytes are received.
- Compares the complete echo with the original payload.
- Counts a failed request if sending, receiving, timeout, or validation fails.

Connections are pre-created before the measured interval. Connection setup is
therefore excluded from RPS and throughput, while the request round trip
includes client send, server echo, and client validation.

## Benchmark Method

The tool is [`tests/benchmark.py`](../tests/benchmark.py).

- One Server process is shared by all scenarios.
- Server stdout and stderr are redirected to `/dev/null`.
- Strace is not enabled during benchmarking.
- Each scenario has a 2-second warm-up.
- Each formal repetition runs for 10 seconds.
- Each scenario is repeated three times.
- Clients operate in a closed loop: one request per connection is outstanding
  at a time, followed by complete validation.
- Latency uses `time.perf_counter_ns()` and a 1-microsecond histogram.

Redirecting logs to `/dev/null` removes terminal and disk I/O effects, but the
server still constructs and formats log output. That overhead remains part of
the measured result.

## Metrics

| Metric | Definition |
|---|---|
| Throughput | Successful echoed bytes per second, reported in MiB/s for one direction |
| RPS | Completed successful logical requests per measured second |
| Average Latency | Mean successful request round-trip time |
| P50/P95/P99 | Nearest-rank percentiles over successful request latencies |
| Clients | Number of concurrent pre-connected TCP clients |
| Server CPU | `utime + stime` delta from `/proc/<pid>/stat`, divided by wall time |
| Server RSS | Peak sampled resident set size from `/proc/<pid>/statm` |
| Error Rate | Failed requests divided by successful plus failed requests |

CPU and RSS are measured only for the Server process. CPU is expressed against
one core as 100%.

## Scenarios

The benchmark used nine representative scenarios rather than a Cartesian
product of every payload and concurrency value.

| Scenario | Payload | Clients |
|---|---:|---:|
| Single-client latency | 64 B | 1 |
| Single-client large payload | 1 MiB | 1 |
| Multi-client echo | 4 KiB | 50 |
| Payload sweep | 64 B | 10 |
| Payload sweep | 1 KiB | 10 |
| Payload sweep | 4 KiB | 10 |
| Payload sweep | 64 KiB | 10 |
| Concurrency sweep | 4 KiB | 1 |
| Concurrency sweep | 4 KiB | 100 |

## Actual Results

Values below are scenario summaries across three 10-second repetitions. P95 is
in milliseconds.

| Payload | Clients | Baseline MiB/s | Refactor MiB/s | Baseline RPS | Refactor RPS | Baseline P95 | Refactor P95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 B | 1 | 1.09 | 1.09 | 17,811.3 | 17,816.6 | 0.065 | 0.065 |
| 1 MiB | 1 | 396.27 | 396.41 | 396.3 | 396.4 | 2.660 | 2.650 |
| 4 KiB | 50 | 63.41 | 63.64 | 16,233.3 | 16,291.2 | 7.490 | 7.456 |
| 64 B | 10 | 1.59 | 1.59 | 25,980.9 | 26,128.3 | 0.905 | 0.900 |
| 1 KiB | 10 | 18.74 | 18.73 | 19,192.8 | 19,176.3 | 1.229 | 1.231 |
| 4 KiB | 10 | 57.23 | 57.78 | 14,651.5 | 14,790.7 | 1.592 | 1.584 |
| 64 KiB | 10 | 287.63 | 285.57 | 4,602.1 | 4,569.0 | 4.735 | 4.769 |
| 4 KiB | 1 | 58.50 | 58.16 | 14,976.8 | 14,889.0 | 0.077 | 0.078 |
| 4 KiB | 100 | 63.42 | 62.77 | 16,236.7 | 16,069.1 | 15.126 | 15.313 |

All nine scenarios reported a **0% error rate**.

Observed maxima and minima from the refactored run:

- Highest scenario-summary throughput: **396.41 MiB/s**
- Highest scenario-summary RPS: **26,128.3**
- Lowest scenario P95: **0.065 ms**
- Highest sampled Server RSS across the benchmark results: **5.82 MiB**

Per-scenario CPU values were collected by the benchmark tool but were not
included in the result set supplied for this document. They are intentionally
not reconstructed here.

## Baseline and Refactor Comparison

The two versions produced very similar values. Small differences in both
directions are consistent with scheduling, Python client behavior, loopback
traffic variation, and VPS noise.

At 100 clients, the refactored run measured slightly lower throughput and RPS
and slightly higher P95 than the baseline. At several smaller scenarios, the
refactored run measured slightly higher values. These changes are not treated
as evidence of a meaningful optimization or regression.

The structural refactoring preserved the polling model, non-blocking I/O,
pending send buffering, and event-mask logic, which is consistent with the
similar measured behavior.

## Reproduction

Use separate source checkouts for the two versions and build both with the same
flags:

```bash
BASELINE_SRC=/path/to/baseline/TcpServer
REFACTOR_SRC=/path/to/refactor/TcpServer
BASELINE_BUILD=/tmp/tcp-bench-baseline
REFACTOR_BUILD=/tmp/tcp-bench-refactor

cmake -S "$BASELINE_SRC" -B "$BASELINE_BUILD" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS_RELEASE="-O2 -DNDEBUG"
cmake --build "$BASELINE_BUILD" --parallel

cmake -S "$REFACTOR_SRC" -B "$REFACTOR_BUILD" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS_RELEASE="-O2 -DNDEBUG"
cmake --build "$REFACTOR_BUILD" --parallel
```

Run one version at a time because the current server does not set
`SO_REUSEADDR` and both use port `54000`:

```bash
python3 "$REFACTOR_SRC/tests/benchmark.py" \
  --server-bin "$BASELINE_BUILD/TcpServer" \
  --label baseline \
  --warmup 2 \
  --duration 10 \
  --repetitions 3 \
  --json-out /tmp/tcp-bench-baseline.json \
  --csv-out /tmp/tcp-bench-baseline.csv
```

After the baseline process has stopped and port `54000` is available:

```bash
python3 "$REFACTOR_SRC/tests/benchmark.py" \
  --server-bin "$REFACTOR_BUILD/TcpServer" \
  --label refactor \
  --warmup 2 \
  --duration 10 \
  --repetitions 3 \
  --json-out /tmp/tcp-bench-refactor.json \
  --csv-out /tmp/tcp-bench-refactor.csv
```

Use `--quick` for a shorter functional smoke benchmark:

```bash
python3 tests/benchmark.py \
  --server-bin "$REFACTOR_BUILD/TcpServer" \
  --label refactor-smoke \
  --quick
```

## Limitations

- The Python client, GIL, and thread scheduling can become the bottleneck before
  the single-threaded C++ server does.
- Loopback TCP bypasses physical NIC and real network-path effects.
- Virtualized CPU quota, neighbor load, and detailed kernel/network tuning were
  not recorded.
- The cpufreq governor interface was unavailable, so frequency-governor state
  cannot be reported.
- Results vary with frequency scaling, virtualization scheduling, and
  background load.
- Server logging to `/dev/null` avoids external I/O but retains formatting and
  logging overhead.
- The closed-loop client limits outstanding requests to one per connection.
- Many-client throughput is bounded by the configured client concurrency.
- Latency percentiles have 1-microsecond histogram granularity.
- RSS is sampled every 0.25 seconds and may miss short memory peaks.
- Fixed-port restart timing can be affected by TIME_WAIT because
  `SO_REUSEADDR` is absent.
- The test covers only TCP echo behavior; it does not evaluate HTTP, TLS,
  application framing, long-lived idle clients, or production failure modes.

## Related Documentation

- [Project overview](../README.md)
- [Architecture](architecture.md)

