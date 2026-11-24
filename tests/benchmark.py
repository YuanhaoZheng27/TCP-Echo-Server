#!/usr/bin/env python3
"""Lightweight performance benchmark for the TCP echo server.

One Echo Request means sending exactly N configured bytes on one TCP
connection, then receiving and validating exactly N echoed bytes before the
next request starts. This fixed-size round trip gives the byte stream a
well-defined request boundary without adding an application protocol.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 54000
DEFAULT_PAYLOAD_SIZES = (64, 1024, 4096, 64 * 1024)
DEFAULT_CONCURRENCY_LEVELS = (1, 10, 50, 100)
MIB = 1024 * 1024


class BenchmarkError(RuntimeError):
    """Raised when the benchmark environment or server is invalid."""


@dataclass(frozen=True)
class Scenario:
    name: str
    payload_size: int
    concurrency: int


@dataclass
class WorkerResult:
    successful_requests: int = 0
    failed_requests: int = 0
    successful_bytes: int = 0
    total_latency_ns: int = 0
    latency_histogram_us: Dict[int, int] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


@dataclass
class RunResult:
    scenario: Scenario
    repetition: int
    timestamp_utc: str
    duration_seconds: float
    successful_requests: int
    failed_requests: int
    error_rate_percent: float
    throughput_mib_s: float
    requests_per_second: float
    average_latency_ms: Optional[float]
    p50_latency_ms: Optional[float]
    p95_latency_ms: Optional[float]
    p99_latency_ms: Optional[float]
    total_latency_ns: int
    latency_histogram_us: Dict[int, int]
    cpu_seconds: float
    server_cpu_percent: float
    rss_start_mib: float
    rss_peak_mib: float
    rss_end_mib: float
    errors: List[str] = field(default_factory=list)


@dataclass
class SummaryResult:
    scenario: Scenario
    repetitions: int
    duration_seconds: float
    successful_requests: int
    failed_requests: int
    error_rate_percent: float
    throughput_mib_s: float
    requests_per_second: float
    average_latency_ms: Optional[float]
    p50_latency_ms: Optional[float]
    p95_latency_ms: Optional[float]
    p99_latency_ms: Optional[float]
    cpu_seconds: float
    server_cpu_percent: float
    rss_peak_mib: float
    errors: List[str] = field(default_factory=list)


def format_bytes(size: int) -> str:
    if size % MIB == 0:
        return "{}MiB".format(size // MIB)
    if size % 1024 == 0:
        return "{}KiB".format(size // 1024)
    return "{}B".format(size)


def parse_positive_int_list(value: str) -> Tuple[int, ...]:
    try:
        values = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("all values must be positive")
    return values


def payload_pattern(size: int) -> bytes:
    block = bytes(range(256))
    return (block * ((size + len(block) - 1) // len(block)))[:size]


def send_all(sock: socket.socket, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        sent = sock.send(view)
        if sent == 0:
            raise ConnectionError("send() returned 0 before the request was complete")
        view = view[sent:]


def receive_exact_into(sock: socket.socket, buffer: bytearray) -> None:
    view = memoryview(buffer)
    received = 0

    while received < len(buffer):
        count = sock.recv_into(view[received:])
        if count == 0:
            raise ConnectionError(
                "connection closed after {}/{} echoed bytes".format(
                    received, len(buffer)
                )
            )
        received += count


def echo_round_trip(
    sock: socket.socket,
    payload: bytes,
    receive_buffer: bytearray,
) -> None:
    send_all(sock, payload)
    receive_exact_into(sock, receive_buffer)

    if memoryview(receive_buffer) != memoryview(payload):
        raise ValueError("echoed payload did not match the request")


def histogram_percentile_ms(
    histogram_us: Dict[int, int], percentile: float
) -> Optional[float]:
    total = sum(histogram_us.values())
    if total == 0:
        return None

    rank = max(1, int(math.ceil(percentile * total)))
    cumulative = 0
    for latency_us in sorted(histogram_us):
        cumulative += histogram_us[latency_us]
        if cumulative >= rank:
            return latency_us / 1000.0
    return None


def read_cpu_seconds(process_pid: int) -> float:
    try:
        stat_text = Path("/proc/{}/stat".format(process_pid)).read_text(
            encoding="ascii"
        )
    except OSError as exc:
        raise BenchmarkError(
            "could not read server CPU statistics from /proc: {}".format(exc)
        ) from exc

    closing_parenthesis = stat_text.rfind(")")
    if closing_parenthesis == -1:
        raise BenchmarkError("invalid /proc/<pid>/stat format")

    fields = stat_text[closing_parenthesis + 1 :].split()
    if len(fields) <= 12:
        raise BenchmarkError("incomplete /proc/<pid>/stat fields")

    cpu_ticks = int(fields[11]) + int(fields[12])
    clock_ticks = os.sysconf("SC_CLK_TCK")
    return cpu_ticks / clock_ticks


def read_rss_bytes(process_pid: int) -> int:
    try:
        statm = Path("/proc/{}/statm".format(process_pid)).read_text(
            encoding="ascii"
        )
    except OSError as exc:
        raise BenchmarkError(
            "could not read server RSS from /proc: {}".format(exc)
        ) from exc

    fields = statm.split()
    if len(fields) < 2:
        raise BenchmarkError("incomplete /proc/<pid>/statm fields")

    resident_pages = int(fields[1])
    page_size = os.sysconf("SC_PAGE_SIZE")
    return resident_pages * page_size


def port_is_available(port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


class ServerProcess:
    def __init__(
        self,
        binary: Path,
        host: str,
        port: int,
        startup_timeout: float,
        stop_timeout: float,
    ) -> None:
        self.binary = binary
        self.host = host
        self.port = port
        self.startup_timeout = startup_timeout
        self.stop_timeout = stop_timeout
        self.process: Optional[subprocess.Popen] = None

    @property
    def pid(self) -> int:
        if self.process is None:
            raise BenchmarkError("server process has not been started")
        return self.process.pid

    def start(self) -> None:
        if not port_is_available(self.port):
            raise BenchmarkError(
                "TCP port {} is already in use".format(self.port)
            )

        self.process = subprocess.Popen(
            [str(self.binary)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

        deadline = time.monotonic() + self.startup_timeout
        last_error: Optional[BaseException] = None

        while time.monotonic() < deadline:
            exit_code = self.process.poll()
            if exit_code is not None:
                raise BenchmarkError(
                    "server exited during startup with code {}".format(exit_code)
                )

            try:
                with socket.create_connection(
                    (self.host, self.port), timeout=0.5
                ):
                    return
            except OSError as exc:
                last_error = exc
                time.sleep(0.1)

        raise BenchmarkError(
            "server did not accept connections within {:.1f}s: {}".format(
                self.startup_timeout, last_error
            )
        )

    def assert_alive(self) -> None:
        if self.process is None:
            raise BenchmarkError("server process has not been started")
        exit_code = self.process.poll()
        if exit_code is not None:
            raise BenchmarkError(
                "server exited during the benchmark with code {}".format(exit_code)
            )

    def stop(self) -> None:
        process = self.process
        if process is None or process.poll() is not None:
            return

        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return

        try:
            process.wait(timeout=self.stop_timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=self.stop_timeout)


class RssSampler:
    def __init__(self, process_pid: int, interval_seconds: float) -> None:
        self.process_pid = process_pid
        self.interval_seconds = interval_seconds
        self.start_mib = 0.0
        self.peak_mib = 0.0
        self.end_mib = 0.0
        self.error: Optional[BaseException] = None
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)

    @staticmethod
    def _mib(value: int) -> float:
        return value / MIB

    def _sample(self) -> float:
        rss_mib = self._mib(read_rss_bytes(self.process_pid))
        self.peak_mib = max(self.peak_mib, rss_mib)
        return rss_mib

    def _sample_loop(self) -> None:
        try:
            while not self._stop_event.wait(self.interval_seconds):
                self._sample()
        except BaseException as exc:
            self.error = exc
            self._stop_event.set()

    def start(self) -> None:
        self.start_mib = self._sample()
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(1.0, self.interval_seconds * 4))
        if self.error is not None:
            raise BenchmarkError(
                "server RSS sampling failed: {!r}".format(self.error)
            ) from self.error
        self.end_mib = self._sample()


class BenchmarkRunner:
    def __init__(
        self,
        server: ServerProcess,
        host: str,
        port: int,
        warmup_seconds: float,
        duration_seconds: float,
        repetitions: int,
        connect_timeout: float,
        io_timeout: float,
        rss_interval: float,
    ) -> None:
        self.server = server
        self.host = host
        self.port = port
        self.warmup_seconds = warmup_seconds
        self.duration_seconds = duration_seconds
        self.repetitions = repetitions
        self.connect_timeout = connect_timeout
        self.io_timeout = io_timeout
        self.rss_interval = rss_interval

    def open_connection(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.connect_timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(self.io_timeout)
            return sock
        except BaseException:
            sock.close()
            raise

    def _client_worker(
        self,
        sock: socket.socket,
        payload: bytes,
        receive_buffer: bytearray,
        phase_end_ns: List[int],
        barrier: threading.Barrier,
        record_latency: bool,
        result: WorkerResult,
    ) -> None:
        try:
            try:
                barrier.wait(timeout=max(5.0, self.connect_timeout * 2))
            except threading.BrokenBarrierError as exc:
                result.failed_requests += 1
                result.errors.append("worker start barrier failed: {!r}".format(exc))
                return

            while time.perf_counter_ns() < phase_end_ns[0]:
                request_start_ns = time.perf_counter_ns()
                try:
                    echo_round_trip(sock, payload, receive_buffer)
                except BaseException as exc:
                    result.failed_requests += 1
                    if len(result.errors) < 5:
                        result.errors.append(
                            "{}: {}".format(type(exc).__name__, exc)
                        )
                    return

                elapsed_ns = time.perf_counter_ns() - request_start_ns
                result.successful_requests += 1
                result.successful_bytes += len(payload)
                result.total_latency_ns += elapsed_ns

                if record_latency:
                    latency_us = elapsed_ns // 1000
                    result.latency_histogram_us[latency_us] = (
                        result.latency_histogram_us.get(latency_us, 0) + 1
                    )
        finally:
            sock.close()

    def _execute_phase(
        self,
        scenario: Scenario,
        duration_seconds: float,
        record_latency: bool,
    ) -> Tuple[List[WorkerResult], float, float, float, float, float]:
        sockets: List[socket.socket] = []
        try:
            for _ in range(scenario.concurrency):
                sockets.append(self.open_connection())
        except BaseException:
            for sock in sockets:
                sock.close()
            raise

        payload = payload_pattern(scenario.payload_size)
        results = [WorkerResult() for _ in sockets]
        barrier = threading.Barrier(scenario.concurrency + 1)
        threads: List[threading.Thread] = []
        phase_end_ns = [0]

        for sock, result in zip(sockets, results):
            thread = threading.Thread(
                target=self._client_worker,
                args=(
                    sock,
                    payload,
                    bytearray(len(payload)),
                    phase_end_ns,
                    barrier,
                    record_latency,
                    result,
                ),
                daemon=True,
            )
            threads.append(thread)
            thread.start()

        cpu_start = 0.0
        sampler: Optional[RssSampler] = None

        try:
            if record_latency:
                cpu_start = read_cpu_seconds(self.server.pid)
                sampler = RssSampler(self.server.pid, self.rss_interval)
                sampler.start()

            start_ns = time.perf_counter_ns()
            phase_end_ns[0] = start_ns + int(
                duration_seconds * 1_000_000_000
            )
            barrier.wait(timeout=max(5.0, self.connect_timeout * 2))
        except threading.BrokenBarrierError as exc:
            barrier.abort()
            for thread in threads:
                thread.join(timeout=max(1.0, self.io_timeout))
            if sampler is not None:
                sampler.stop()
            raise BenchmarkError(
                "client workers did not start: {!r}".format(exc)
            ) from exc
        except BaseException:
            barrier.abort()
            for thread in threads:
                thread.join(timeout=max(1.0, self.io_timeout))
            if sampler is not None:
                sampler.stop()
            raise

        for thread in threads:
            thread.join()

        elapsed_seconds = (time.perf_counter_ns() - start_ns) / 1_000_000_000
        cpu_end = read_cpu_seconds(self.server.pid) if record_latency else 0.0

        if sampler is not None:
            sampler.stop()
            rss_start_mib = sampler.start_mib
            rss_peak_mib = sampler.peak_mib
            rss_end_mib = sampler.end_mib
        else:
            rss_start_mib = rss_peak_mib = rss_end_mib = 0.0

        cpu_seconds = cpu_end - cpu_start
        return (
            results,
            elapsed_seconds,
            cpu_seconds,
            rss_start_mib,
            rss_peak_mib,
            rss_end_mib,
        )

    @staticmethod
    def _merge_histograms(
        results: Sequence[WorkerResult],
    ) -> Dict[int, int]:
        merged: Dict[int, int] = {}
        for result in results:
            for latency_us, count in result.latency_histogram_us.items():
                merged[latency_us] = merged.get(latency_us, 0) + count
        return merged

    def _make_run_result(
        self,
        scenario: Scenario,
        repetition: int,
        phase_result: Tuple[
            List[WorkerResult], float, float, float, float, float
        ],
    ) -> RunResult:
        (
            worker_results,
            elapsed_seconds,
            cpu_seconds,
            rss_start_mib,
            rss_peak_mib,
            rss_end_mib,
        ) = phase_result
        successful = sum(item.successful_requests for item in worker_results)
        failed = sum(item.failed_requests for item in worker_results)
        successful_bytes = sum(item.successful_bytes for item in worker_results)
        total_latency_ns = sum(item.total_latency_ns for item in worker_results)
        attempts = successful + failed
        histogram = self._merge_histograms(worker_results)
        errors: List[str] = []

        for item in worker_results:
            for error in item.errors:
                if error not in errors and len(errors) < 10:
                    errors.append(error)

        return RunResult(
            scenario=scenario,
            repetition=repetition,
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            duration_seconds=elapsed_seconds,
            successful_requests=successful,
            failed_requests=failed,
            error_rate_percent=(failed / attempts * 100.0) if attempts else 0.0,
            throughput_mib_s=(
                successful_bytes / elapsed_seconds / MIB
                if elapsed_seconds > 0
                else 0.0
            ),
            requests_per_second=successful / elapsed_seconds
            if elapsed_seconds > 0
            else 0.0,
            average_latency_ms=(
                total_latency_ns / successful / 1_000_000.0
                if successful
                else None
            ),
            p50_latency_ms=histogram_percentile_ms(histogram, 0.50),
            p95_latency_ms=histogram_percentile_ms(histogram, 0.95),
            p99_latency_ms=histogram_percentile_ms(histogram, 0.99),
            total_latency_ns=total_latency_ns,
            latency_histogram_us=histogram,
            cpu_seconds=cpu_seconds,
            server_cpu_percent=(
                cpu_seconds / elapsed_seconds * 100.0
                if elapsed_seconds > 0
                else 0.0
            ),
            rss_start_mib=rss_start_mib,
            rss_peak_mib=rss_peak_mib,
            rss_end_mib=rss_end_mib,
            errors=errors,
        )

    def warm_up(self, scenario: Scenario) -> None:
        if self.warmup_seconds <= 0:
            return

        phase_result = self._execute_phase(
            scenario, self.warmup_seconds, record_latency=False
        )
        worker_results = phase_result[0]
        failures = sum(item.failed_requests for item in worker_results)
        if failures:
            errors = [
                error for item in worker_results for error in item.errors
            ]
            raise BenchmarkError(
                "warm-up failed for {}: {}".format(
                    scenario.name, "; ".join(errors[:3])
                )
            )

    def measure(self, scenario: Scenario) -> List[RunResult]:
        runs: List[RunResult] = []
        for repetition in range(1, self.repetitions + 1):
            phase_result = self._execute_phase(
                scenario, self.duration_seconds, record_latency=True
            )
            result = self._make_run_result(
                scenario, repetition, phase_result
            )
            runs.append(result)
            self.server.assert_alive()
            print(
                "  run {}/{}: {:.2f} MiB/s, {:.1f} RPS, "
                "p95={} ms, errors={}".format(
                    repetition,
                    self.repetitions,
                    result.throughput_mib_s,
                    result.requests_per_second,
                    (
                        "{:.3f}".format(result.p95_latency_ms)
                        if result.p95_latency_ms is not None
                        else "n/a"
                    ),
                    result.failed_requests,
                ),
                flush=True,
            )
        return runs


def summarize_scenario(
    scenario: Scenario,
    runs: Sequence[RunResult],
) -> SummaryResult:
    duration_seconds = sum(run.duration_seconds for run in runs)
    successful = sum(run.successful_requests for run in runs)
    failed = sum(run.failed_requests for run in runs)
    total_bytes = successful * scenario.payload_size
    total_latency_ns = sum(run.total_latency_ns for run in runs)
    latency_histogram_us: Dict[int, int] = {}
    for run in runs:
        for latency_us, count in run.latency_histogram_us.items():
            latency_histogram_us[latency_us] = (
                latency_histogram_us.get(latency_us, 0) + count
            )
    total_cpu_seconds = sum(run.cpu_seconds for run in runs)
    attempts = successful + failed
    errors: List[str] = []
    for run in runs:
        for error in run.errors:
            if error not in errors and len(errors) < 10:
                errors.append(error)

    return SummaryResult(
        scenario=scenario,
        repetitions=len(runs),
        duration_seconds=duration_seconds,
        successful_requests=successful,
        failed_requests=failed,
        error_rate_percent=(failed / attempts * 100.0) if attempts else 0.0,
        throughput_mib_s=(
            total_bytes / duration_seconds / MIB if duration_seconds else 0.0
        ),
        requests_per_second=(
            successful / duration_seconds if duration_seconds else 0.0
        ),
        average_latency_ms=(
            total_latency_ns / successful / 1_000_000.0 if successful else None
        ),
        p50_latency_ms=histogram_percentile_ms(latency_histogram_us, 0.50),
        p95_latency_ms=histogram_percentile_ms(latency_histogram_us, 0.95),
        p99_latency_ms=histogram_percentile_ms(latency_histogram_us, 0.99),
        cpu_seconds=total_cpu_seconds,
        server_cpu_percent=(
            total_cpu_seconds / duration_seconds * 100.0
            if duration_seconds
            else 0.0
        ),
        rss_peak_mib=max((run.rss_peak_mib for run in runs), default=0.0),
        errors=errors,
    )


def build_scenarios(args: argparse.Namespace) -> List[Scenario]:
    candidates: List[Scenario] = [
        Scenario(
            "single-client-latency-{}".format(
                format_bytes(args.single_payload_bytes)
            ),
            args.single_payload_bytes,
            1,
        ),
        Scenario(
            "single-client-large-{}".format(
                format_bytes(args.large_payload_bytes)
            ),
            args.large_payload_bytes,
            1,
        ),
        Scenario(
            "multi-client-{}-c{}".format(
                format_bytes(args.multi_payload_bytes),
                args.multi_concurrency,
            ),
            args.multi_payload_bytes,
            args.multi_concurrency,
        ),
    ]

    for payload_size in args.payload_sizes:
        candidates.append(
            Scenario(
                "payload-{}-c{}".format(
                    format_bytes(payload_size), args.payload_sweep_concurrency
                ),
                payload_size,
                args.payload_sweep_concurrency,
            )
        )

    for concurrency in args.concurrency_levels:
        candidates.append(
            Scenario(
                "concurrency-{}-c{}".format(
                    format_bytes(args.concurrency_sweep_payload), concurrency
                ),
                args.concurrency_sweep_payload,
                concurrency,
            )
        )

    scenarios: List[Scenario] = []
    seen: set[Tuple[int, int]] = set()
    for scenario in candidates:
        key = (scenario.payload_size, scenario.concurrency)
        if key not in seen:
            seen.add(key)
            scenarios.append(scenario)
    return scenarios


def latency_text(value: Optional[float]) -> str:
    return "{:.3f}".format(value) if value is not None else "n/a"


def print_result_table(summaries: Sequence[SummaryResult]) -> None:
    header = (
        "{:<28} {:>7} {:>5} {:>11} {:>10} {:>9} "
        "{:>9} {:>9} {:>9} {:>8} {:>10} {:>8}"
    ).format(
        "Scenario",
        "Payload",
        "Conn",
        "MiB/s",
        "RPS",
        "Avg ms",
        "P50 ms",
        "P95 ms",
        "P99 ms",
        "CPU %",
        "RSS MiB",
        "Errors",
    )
    print("\n" + header)
    print("-" * len(header))

    for summary in summaries:
        print(
            "{:<28} {:>7} {:>5} {:>11.2f} {:>10.1f} {:>9} "
            "{:>9} {:>9} {:>9} {:>8.1f} {:>10.2f} {:>8}".format(
                summary.scenario.name[:28],
                format_bytes(summary.scenario.payload_size),
                summary.scenario.concurrency,
                summary.throughput_mib_s,
                summary.requests_per_second,
                latency_text(summary.average_latency_ms),
                latency_text(summary.p50_latency_ms),
                latency_text(summary.p95_latency_ms),
                latency_text(summary.p99_latency_ms),
                summary.server_cpu_percent,
                summary.rss_peak_mib,
                "{}/{} ({:.2f}%)".format(
                    summary.failed_requests,
                    summary.successful_requests + summary.failed_requests,
                    summary.error_rate_percent,
                ),
            )
        )


def run_to_dict(run: RunResult) -> Dict[str, object]:
    return {
        "scenario": run.scenario.name,
        "payload_bytes": run.scenario.payload_size,
        "concurrency": run.scenario.concurrency,
        "repetition": run.repetition,
        "timestamp_utc": run.timestamp_utc,
        "duration_seconds": run.duration_seconds,
        "successful_requests": run.successful_requests,
        "failed_requests": run.failed_requests,
        "error_rate_percent": run.error_rate_percent,
        "throughput_mib_s": run.throughput_mib_s,
        "requests_per_second": run.requests_per_second,
        "average_latency_ms": run.average_latency_ms,
        "p50_latency_ms": run.p50_latency_ms,
        "p95_latency_ms": run.p95_latency_ms,
        "p99_latency_ms": run.p99_latency_ms,
        "cpu_seconds": run.cpu_seconds,
        "server_cpu_percent": run.server_cpu_percent,
        "rss_start_mib": run.rss_start_mib,
        "rss_peak_mib": run.rss_peak_mib,
        "rss_end_mib": run.rss_end_mib,
        "errors": run.errors,
    }


def summary_to_dict(summary: SummaryResult) -> Dict[str, object]:
    return {
        "scenario": summary.scenario.name,
        "payload_bytes": summary.scenario.payload_size,
        "concurrency": summary.scenario.concurrency,
        "repetitions": summary.repetitions,
        "duration_seconds": summary.duration_seconds,
        "successful_requests": summary.successful_requests,
        "failed_requests": summary.failed_requests,
        "error_rate_percent": summary.error_rate_percent,
        "throughput_mib_s": summary.throughput_mib_s,
        "requests_per_second": summary.requests_per_second,
        "average_latency_ms": summary.average_latency_ms,
        "p50_latency_ms": summary.p50_latency_ms,
        "p95_latency_ms": summary.p95_latency_ms,
        "p99_latency_ms": summary.p99_latency_ms,
        "cpu_seconds": summary.cpu_seconds,
        "server_cpu_percent": summary.server_cpu_percent,
        "rss_peak_mib": summary.rss_peak_mib,
        "errors": summary.errors,
    }


def write_json_output(
    path: Path,
    label: str,
    server_binary: Path,
    scenarios: Sequence[Scenario],
    runs: Sequence[RunResult],
    summaries: Sequence[SummaryResult],
    config: Dict[str, object],
) -> None:
    output = {
        "schema_version": 1,
        "label": label,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "server_binary": str(server_binary),
            "server_log": os.devnull,
        },
        "request_definition": (
            "One send of exactly N bytes followed by one complete N-byte "
            "echo verification before the next request on that connection."
        ),
        "throughput_definition": (
            "Successful echoed bytes divided by measured wall time; "
            "one direction only."
        ),
        "config": config,
        "scenarios": [
            {
                "name": scenario.name,
                "payload_bytes": scenario.payload_size,
                "concurrency": scenario.concurrency,
            }
            for scenario in scenarios
        ],
        "runs": [run_to_dict(run) for run in runs],
        "summaries": [summary_to_dict(summary) for summary in summaries],
        "limitations": [
            "Python client and GIL can limit request generation.",
            "Loopback networking and VPS CPU scheduling affect results.",
            "Server stdout/stderr are redirected to /dev/null, but logging "
            "and formatting overhead remains.",
            "Latency percentiles use 1 microsecond histogram buckets.",
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_csv_output(
    path: Path,
    label: str,
    runs: Sequence[RunResult],
    summaries: Sequence[SummaryResult],
) -> None:
    fields = [
        "label",
        "record_type",
        "scenario",
        "payload_bytes",
        "concurrency",
        "repetition",
        "repetitions",
        "timestamp_utc",
        "duration_seconds",
        "successful_requests",
        "failed_requests",
        "error_rate_percent",
        "throughput_mib_s",
        "requests_per_second",
        "average_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "cpu_seconds",
        "server_cpu_percent",
        "rss_start_mib",
        "rss_peak_mib",
        "rss_end_mib",
        "errors",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        for run in runs:
            row = {"label": label, "record_type": "run"}
            row.update(run_to_dict(run))
            row["errors"] = " | ".join(run.errors)
            writer.writerow(row)

        for summary in summaries:
            row = {"label": label, "record_type": "summary"}
            row.update(summary_to_dict(summary))
            row["repetition"] = "all"
            row["timestamp_utc"] = ""
            row["rss_start_mib"] = ""
            row["rss_end_mib"] = ""
            row["errors"] = " | ".join(summary.errors)
            writer.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a closed-loop TCP echo performance benchmark on Linux. "
            "One request is one fixed-size send plus complete echo validation."
        )
    )
    parser.add_argument(
        "--server-bin",
        default=os.environ.get("TCP_SERVER_BIN", "build/TcpServer"),
        help="path to the TcpServer executable (default: %(default)s)",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="connection host (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="server TCP port; must match the compiled server (default: %(default)s)",
    )
    parser.add_argument(
        "--label",
        default="unlabeled",
        help="result label, such as baseline or refactor (default: %(default)s)",
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=2.0,
        help="warm-up seconds per scenario (default: %(default)s)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=10.0,
        help="measured seconds per repetition (default: %(default)s)",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="measured repetitions per scenario (default: %(default)s)",
    )
    parser.add_argument(
        "--payload-sizes",
        type=parse_positive_int_list,
        default=DEFAULT_PAYLOAD_SIZES,
        help="comma-separated payload sweep in bytes (default: 64,1024,4096,65536)",
    )
    parser.add_argument(
        "--concurrency-levels",
        type=parse_positive_int_list,
        default=DEFAULT_CONCURRENCY_LEVELS,
        help="comma-separated concurrency sweep (default: 1,10,50,100)",
    )
    parser.add_argument(
        "--single-payload-bytes",
        type=int,
        default=64,
        help="single-client latency payload size (default: %(default)s)",
    )
    parser.add_argument(
        "--large-payload-bytes",
        type=int,
        default=MIB,
        help="single-client large payload size (default: %(default)s)",
    )
    parser.add_argument(
        "--multi-payload-bytes",
        type=int,
        default=4096,
        help="multi-client payload size (default: %(default)s)",
    )
    parser.add_argument(
        "--multi-concurrency",
        type=int,
        default=50,
        help="multi-client connection count (default: %(default)s)",
    )
    parser.add_argument(
        "--payload-sweep-concurrency",
        type=int,
        default=10,
        help="concurrency used for the payload-size sweep (default: %(default)s)",
    )
    parser.add_argument(
        "--concurrency-sweep-payload",
        type=int,
        default=4096,
        help="payload size used for the concurrency sweep (default: %(default)s)",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=3.0,
        help="TCP connect timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--io-timeout",
        type=float,
        default=10.0,
        help="socket I/O timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=10.0,
        help="server startup timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--stop-timeout",
        type=float,
        default=5.0,
        help="server shutdown timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--rss-interval",
        type=float,
        default=0.25,
        help="server RSS sampling interval in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="optional JSON result path",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=None,
        help="optional CSV result path",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="short smoke benchmark: 0.5s warm-up, 2s duration, 1 repetition",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    numeric_values = {
        "--duration": args.duration,
        "--connect-timeout": args.connect_timeout,
        "--io-timeout": args.io_timeout,
        "--startup-timeout": args.startup_timeout,
        "--stop-timeout": args.stop_timeout,
        "--rss-interval": args.rss_interval,
        "--single-payload-bytes": args.single_payload_bytes,
        "--large-payload-bytes": args.large_payload_bytes,
        "--multi-payload-bytes": args.multi_payload_bytes,
        "--multi-concurrency": args.multi_concurrency,
        "--payload-sweep-concurrency": args.payload_sweep_concurrency,
        "--concurrency-sweep-payload": args.concurrency_sweep_payload,
    }
    for name, value in numeric_values.items():
        if value <= 0:
            raise BenchmarkError("{} must be positive".format(name))

    if args.duration <= 0 or args.repetitions <= 0:
        raise BenchmarkError("--duration and --repetitions must be positive")
    if args.warmup < 0:
        raise BenchmarkError("--warmup must be non-negative")
    if args.rss_interval <= 0:
        raise BenchmarkError("--rss-interval must be positive")
    if not (1 <= args.port <= 65535):
        raise BenchmarkError("--port must be between 1 and 65535")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.quick:
        args.warmup = 0.5
        args.duration = 2.0
        args.repetitions = 1

    try:
        validate_args(args)
    except BenchmarkError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    if not sys.platform.startswith("linux"):
        print(
            "The server and /proc resource measurements require Linux; "
            "current platform is {}.".format(sys.platform),
            file=sys.stderr,
        )
        return 2

    server_binary = Path(args.server_bin).expanduser().resolve()
    if not server_binary.is_file():
        print(
            "error: server binary not found: {}".format(server_binary),
            file=sys.stderr,
        )
        return 2
    if not os.access(server_binary, os.X_OK):
        print(
            "error: server binary is not executable: {}".format(server_binary),
            file=sys.stderr,
        )
        return 2
    if not Path("/proc").is_dir():
        print("error: /proc is required for CPU and RSS measurement", file=sys.stderr)
        return 2

    scenarios = build_scenarios(args)
    estimated_seconds = len(scenarios) * (
        args.warmup + args.duration * args.repetitions
    )
    server = ServerProcess(
        binary=server_binary,
        host=args.host,
        port=args.port,
        startup_timeout=args.startup_timeout,
        stop_timeout=args.stop_timeout,
    )
    runner = BenchmarkRunner(
        server=server,
        host=args.host,
        port=args.port,
        warmup_seconds=args.warmup,
        duration_seconds=args.duration,
        repetitions=args.repetitions,
        connect_timeout=args.connect_timeout,
        io_timeout=args.io_timeout,
        rss_interval=args.rss_interval,
    )
    runs: List[RunResult] = []
    summaries: List[SummaryResult] = []

    print("Label: {}".format(args.label))
    print("Server: {}".format(server_binary))
    print("Server log: {} (log formatting overhead remains)".format(os.devnull))
    print(
        "Warm-up: {:.1f}s, duration: {:.1f}s, repetitions: {}".format(
            args.warmup, args.duration, args.repetitions
        )
    )
    print(
        "Scenarios: {}, estimated minimum time: {:.1f}s".format(
            len(scenarios), estimated_seconds
        )
    )

    try:
        server.start()
        print("Server PID: {}".format(server.pid), flush=True)

        for index, scenario in enumerate(scenarios, start=1):
            print(
                "\n[{}/{}] {} (payload={}, connections={})".format(
                    index,
                    len(scenarios),
                    scenario.name,
                    format_bytes(scenario.payload_size),
                    scenario.concurrency,
                ),
                flush=True,
            )
            server.assert_alive()
            runner.warm_up(scenario)
            scenario_runs = runner.measure(scenario)
            runs.extend(scenario_runs)

            summaries.append(summarize_scenario(scenario, scenario_runs))

        print_result_table(summaries)

        config = {
            "host": args.host,
            "port": args.port,
            "warmup_seconds": args.warmup,
            "duration_seconds": args.duration,
            "repetitions": args.repetitions,
            "connect_timeout_seconds": args.connect_timeout,
            "io_timeout_seconds": args.io_timeout,
            "rss_sampling_interval_seconds": args.rss_interval,
            "payload_sizes_bytes": list(args.payload_sizes),
            "concurrency_levels": list(args.concurrency_levels),
            "single_payload_bytes": args.single_payload_bytes,
            "large_payload_bytes": args.large_payload_bytes,
            "multi_payload_bytes": args.multi_payload_bytes,
            "multi_concurrency": args.multi_concurrency,
            "payload_sweep_concurrency": args.payload_sweep_concurrency,
            "concurrency_sweep_payload": args.concurrency_sweep_payload,
        }

        if args.json_out is not None:
            write_json_output(
                args.json_out,
                args.label,
                server_binary,
                scenarios,
                runs,
                summaries,
                config,
            )
            print("\nJSON written to: {}".format(args.json_out))

        if args.csv_out is not None:
            write_csv_output(args.csv_out, args.label, runs, summaries)
            print("CSV written to: {}".format(args.csv_out))

        return 0
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.", file=sys.stderr)
        return 130
    except BaseException as exc:
        print("\nBenchmark failed: {}".format(exc), file=sys.stderr)
        return 1
    finally:
        server.stop()


if __name__ == "__main__":
    sys.exit(main())
