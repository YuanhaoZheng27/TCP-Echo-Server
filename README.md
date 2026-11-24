# Linux Event-driven TCP Echo Server

A compact C++20 TCP echo server built to study Linux network programming with
non-blocking sockets, level-triggered `epoll`, partial I/O handling, and TCP
half-close semantics.

This is an educational and portfolio project. It is not an HTTP server and is
not designed as a production-ready network service.

## Project Overview

The server accepts TCP connections on port `54000`, reads a byte stream from
each client, and echoes the same bytes back. A single thread runs the event
loop. Client state is stored per file descriptor, and echo data that cannot be
sent immediately remains buffered until the socket becomes writable.

The project includes:

- A C++20 event-driven server implementation.
- Integration tests using only the Python standard library and `strace`.
- A lightweight performance benchmark covering latency, throughput, RPS,
  concurrency, CPU, RSS, and request errors.
- Baseline and post-refactoring functional and performance results.

## Project Features

- Linux `epoll` event loop with level-triggered notifications.
- Non-blocking listening and client sockets.
- Multiple simultaneous client connections.
- Partial `recv()` and `send()` handling.
- Per-client pending send buffer.
- Dynamic `EPOLLOUT` registration when a send would block.
- TCP half-close handling that drains remaining echo data before closing.
- Explicit client cleanup and idempotent FD ownership.
- Binary-safe byte-stream echo.
- Automated integration, backpressure-path, and FD-cleanup tests.
- Reproducible benchmark output in JSON and CSV.

## Technical Stack

| Area | Technology |
|---|---|
| Language | C++20 |
| Platform | Linux |
| Networking | POSIX sockets, `epoll`, `fcntl` |
| Build | CMake 3.28+ |
| Integration tests | Python 3 standard library, `strace` |
| Benchmark | Python 3 standard library |

The server uses IPv4 and a fixed `AF_INET`/`SOCK_STREAM` socket. It does not
currently support Windows or macOS because it depends on Linux `epoll`.

## Project Structure

```text
.
├── CMakeLists.txt
├── README.md
├── docs/
│   ├── architecture.md
│   └── benchmark.md
├── src/
│   ├── main.cpp
│   ├── TcpServer.h
│   └── TcpServer.cpp
└── tests/
    ├── benchmark.py
    └── test_echo.py
```

## Architecture Overview

`TcpServer` owns the listening socket, the epoll FD, and a map of per-client
state. `main()` creates the server object and calls `TcpServer::run()`.

```text
main
└── TcpServer::run
    ├── setupServer
    │   ├── createListeningSocket
    │   ├── createEpollInstance
    │   ├── setNonBlocking
    │   └── registerListeningSocket
    ├── runEventLoop
    │   ├── handleEvent
    │   ├── handleAccept
    │   └── handleClientEvent
    │       ├── handleClientRead
    │       ├── handleClientWrite
    │       ├── updateClientEvents
    │       └── cleanupClient
    └── closeServerResources
```

The implementation is intentionally limited to one reactor-style event loop.
See [docs/architecture.md](docs/architecture.md) for the event masks, state
transitions, half-close behavior, and FD lifecycle.

## Build and Run

### Prerequisites

- Linux with `epoll`
- CMake 3.28 or newer
- A C++20-compatible compiler
- Python 3 for tests and benchmarks
- Port `54000` available

### Release Build

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS_RELEASE="-O2 -DNDEBUG"

cmake --build build-release --parallel
./build-release/TcpServer
```

The server runs in the foreground and listens on all IPv4 interfaces at port
`54000`.

### Debug Build

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug --parallel
./build-debug/TcpServer
```

## Integration Testing

The integration suite uses one shared server process and covers:

- Basic and persistent TCP connections.
- Fragmented data and 4096-byte receive boundaries.
- Large and binary payloads.
- Multiple concurrent clients.
- Partial writes and backpressure.
- Normal and empty TCP half-close.
- Abrupt connection reset.
- Repeated connection and FD cleanup.
- Internal Send EAGAIN, Partial Send, and `EPOLLOUT` transitions through
  `strace`.

Run the suite on Linux:

```bash
python3 tests/test_echo.py \
  --server-bin ./build-debug/TcpServer \
  --strace required \
  --artifact-dir /tmp/tcp-echo-tests \
  -v
```

The test tool starts and stops the server itself. Ensure port `54000` is free.

### Integration Test Results

| Stage | Result | Runtime | Send EAGAIN | EPOLLOUT MOD |
|---|---:|---:|---:|---:|
| Baseline, pre-refactoring | 12/12 passed | 3.361 s | 2 | 2 |
| Regression, refactored | 12/12 passed | 3.350 s | 3 | 3 |

The Baseline recorded 659 partial sends, and the regression run recorded 648.
Both versions also passed the same-FD EAGAIN-to-`EPOLLOUT` transition check.
These trace counts are observations from live traffic, not deterministic
performance metrics.

## Performance Benchmark Results

The benchmark ran on a Linux Ubuntu VPS using loopback TCP, a Python standard
library client, and `-O2 -DNDEBUG` for both versions. Each of the nine scenarios
used a 2-second warm-up, a 10-second measurement, and three repetitions.

Selected scenario summaries:

| Payload | Clients | Baseline MiB/s | Refactor MiB/s | Baseline RPS | Refactor RPS | Baseline P95 | Refactor P95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 B | 1 | 1.09 | 1.09 | 17,811.3 | 17,816.6 | 0.065 ms | 0.065 ms |
| 1 MiB | 1 | 396.27 | 396.41 | 396.3 | 396.4 | 2.660 ms | 2.650 ms |
| 4 KiB | 50 | 63.41 | 63.64 | 16,233.3 | 16,291.2 | 7.490 ms | 7.456 ms |
| 4 KiB | 100 | 63.42 | 62.77 | 16,236.7 | 16,069.1 | 15.126 ms | 15.313 ms |

All nine scenarios reported a 0% error rate. The highest refactored summary
throughput was **396.41 MiB/s**, the highest summary RPS was **26,128.3**, the
lowest P95 was **0.065 ms**, and the highest sampled Server RSS was
**5.82 MiB**.

The refactoring was structural. Observed differences between versions are
small and consistent with run-to-run variability; they should not be interpreted
as a deliberate performance improvement or regression.

See [docs/benchmark.md](docs/benchmark.md) for methodology, all nine scenarios,
environment caveats, and reproduction commands.

## Known Limitations

- The server is IPv4-only and uses the fixed port `54000`.
- `SO_REUSEADDR` is not set, so restarting immediately after a run may encounter
  port binding issues while prior connections remain in TIME_WAIT.
- `pendingSend` has no maximum size. A client that sends faster than it reads
  can cause unbounded memory growth.
- Removing sent bytes with `std::string::erase(0, n)` can move the remaining
  payload and becomes expensive for large buffers.
- Reverse DNS in `getnameinfo()` runs synchronously inside the event loop and
  can delay all other clients.
- Graceful shutdown is not implemented. SIGINT or SIGTERM terminates the
  process without a coordinated shutdown sequence.
- Accept and receive loops continue until EAGAIN, so a busy socket can delay
  progress on other ready sockets.
- Every received chunk is formatted through `std::cout`. Redirecting output to
  `/dev/null` avoids terminal or disk I/O but not logging overhead.
- There are no idle timeouts, connection limits, application framing rules, or
  configurable port settings.
- The benchmark is a single-VPS loopback measurement, not a theoretical server
  limit or a production capacity claim.

## AI-Assisted Development

The original TCP server implementation, including non-blocking socket programming, epoll-based event handling, partial writes, backpressure management, and TCP half-close handling, was developed manually as part of a systems programming learning project.

AI coding agents were subsequently used to assist with code refactoring, automated integration test development, performance benchmarking tools, and project documentation.

The project scope, technical decisions, and acceptance criteria were determined or approved by the author. All reported integration tests and performance benchmarks were executed on a Linux VPS by the author.

