#!/usr/bin/env python3
"""Integration tests for the TCP echo server.

The tests use only the Python standard library and are intended to run on
Linux because the server depends on epoll. A single server process is shared
by all tests to avoid repeated binds while SO_REUSEADDR is not configured.

Typical Linux usage:

    python3 tests/test_echo.py \
        --server-bin ./build-baseline/TcpServer \
        --strace required \
        -v
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 54000

LARGE_PAYLOAD_BYTES = 8 * 1024 * 1024
BACKPRESSURE_PAYLOAD_BYTES = 16 * 1024 * 1024
CONCURRENT_CLIENT_COUNT = 32
CONCURRENT_PAYLOAD_BYTES = 64 * 1024
REPEATED_CONNECTION_COUNT = 200


def patterned_payload(size: int) -> bytes:
    """Return deterministic binary data of the requested size."""
    block = bytes(range(256))
    return (block * ((size + len(block) - 1) // len(block)))[:size]


def send_all(sock: socket.socket, data: bytes) -> None:
    """Send all bytes without assuming a single send() handles the payload."""
    view = memoryview(data)
    while view:
        sent = sock.send(view)
        if sent == 0:
            raise ConnectionError("socket.send() returned 0 before all data was sent")
        view = view[sent:]


def parse_send_calls(trace_text: str) -> Tuple[List[Tuple[int, int]], List[int]]:
    """Return positive partial sends and line indices of send EAGAIN results."""
    partial_sends: List[Tuple[int, int]] = []
    eagain_indices: List[int] = []
    send_pattern = re.compile(
        r"\b(?:sendto|send)\(.*?, (?P<length>\d+), MSG_NOSIGNAL\)"
        r"\s+=\s+(?P<result>-?\d+)(?P<tail>.*)$"
    )

    for index, line in enumerate(trace_text.splitlines()):
        match = send_pattern.search(line)
        if not match:
            continue

        requested = int(match.group("length"))
        result = int(match.group("result"))
        tail = match.group("tail")

        if 0 < result < requested:
            partial_sends.append((requested, result))

        if result == -1 and ("EAGAIN" in tail or "EWOULDBLOCK" in tail):
            eagain_indices.append(index)

    return partial_sends, eagain_indices


def port_is_available(port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


class ManagedServer:
    """Start, monitor, and stop one server process."""

    def __init__(
        self,
        binary: Path,
        host: str,
        port: int,
        startup_timeout: float,
        stop_timeout: float,
        artifact_dir: Path,
        strace_path: Optional[str],
    ) -> None:
        self.binary = binary
        self.host = host
        self.port = port
        self.startup_timeout = startup_timeout
        self.stop_timeout = stop_timeout
        self.artifact_dir = artifact_dir
        self.strace_path = strace_path
        self.log_path = artifact_dir / "server.log"
        self.trace_path = artifact_dir / "server.strace"
        self.process: Optional[subprocess.Popen] = None
        self.server_pid: Optional[int] = None
        self._log_file = None

    def start(self) -> None:
        if not port_is_available(self.port):
            raise RuntimeError(
                "TCP port {} is already in use. Stop the existing process "
                "before running the integration tests.".format(self.port)
            )

        command: List[str] = []
        if self.strace_path:
            command.extend(
                [
                    self.strace_path,
                    "-f",
                    "-tt",
                    "-s",
                    "0",
                    "-e",
                    "trace=network,epoll_ctl,epoll_wait,close",
                    "-o",
                    str(self.trace_path),
                ]
            )
        command.append(str(self.binary))

        self._log_file = self.log_path.open("wb", buffering=0)
        try:
            self.process = subprocess.Popen(
                command,
                stdout=self._log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except BaseException:
            self._log_file.close()
            self._log_file = None
            raise

        if self.strace_path:
            self.server_pid = self._find_strace_child()
        else:
            self.server_pid = self.process.pid

        self._wait_until_ready()

    def _find_strace_child(self) -> Optional[int]:
        if self.process is None:
            return None

        children_path = Path(
            "/proc/{}/task/{}/children".format(self.process.pid, self.process.pid)
        )
        deadline = time.monotonic() + self.startup_timeout

        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                return None

            try:
                children = children_path.read_text(encoding="ascii").split()
            except OSError:
                children = []

            if children:
                try:
                    return int(children[0])
                except ValueError:
                    return None

            time.sleep(0.05)

        return None

    def _wait_until_ready(self) -> None:
        if self.process is None:
            raise RuntimeError("server process was not started")

        deadline = time.monotonic() + self.startup_timeout
        last_error: Optional[BaseException] = None

        while time.monotonic() < deadline:
            exit_code = self.process.poll()
            if exit_code is not None:
                raise RuntimeError(
                    "server exited during startup with code {}\n{}".format(
                        exit_code, self.log_tail()
                    )
                )

            try:
                with socket.create_connection(
                    (self.host, self.port), timeout=0.5
                ) as probe:
                    probe.settimeout(0.5)
                return
            except OSError as exc:
                last_error = exc
                time.sleep(0.1)

        raise RuntimeError(
            "server did not accept connections within {:.1f}s: {}\n{}".format(
                self.startup_timeout, last_error, self.log_tail()
            )
        )

    def assert_alive(self) -> None:
        if self.process is None:
            raise RuntimeError("server process was not started")

        exit_code = self.process.poll()
        if exit_code is not None:
            raise RuntimeError(
                "server exited unexpectedly with code {}\n{}".format(
                    exit_code, self.log_tail()
                )
            )

    def stop(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

            try:
                process.wait(timeout=self.stop_timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=self.stop_timeout)

        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    def log_tail(self, max_bytes: int = 12000) -> str:
        try:
            data = self.log_path.read_bytes()[-max_bytes:]
        except OSError:
            return "<server log unavailable>"
        return data.decode("utf-8", errors="replace")

    def trace_text(self) -> str:
        try:
            return self.trace_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""


@unittest.skipUnless(
    sys.platform.startswith("linux"),
    "the server requires Linux epoll and must be tested on Linux",
)
class EchoIntegrationTests(unittest.TestCase):
    host = DEFAULT_HOST
    port = DEFAULT_PORT
    server_binary = Path("cmake-build-debug/TcpServer")
    connect_timeout = 3.0
    io_timeout = 10.0
    startup_timeout = 10.0
    workload_timeout = 60.0
    stop_timeout = 5.0
    strace_mode = "auto"
    strace_binary: Optional[str] = None

    server: ManagedServer
    artifacts: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.artifacts = tempfile.TemporaryDirectory(prefix="tcp-echo-tests-")
        artifact_dir = Path(cls.artifacts.name)
        cls.server = ManagedServer(
            binary=cls.server_binary,
            host=cls.host,
            port=cls.port,
            startup_timeout=cls.startup_timeout,
            stop_timeout=cls.stop_timeout,
            artifact_dir=artifact_dir,
            strace_path=cls.strace_binary if cls.strace_mode != "off" else None,
        )
        try:
            cls.server.start()
        except BaseException:
            cls.server.stop()
            cls.artifacts.cleanup()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.server.stop()
        finally:
            cls.artifacts.cleanup()
            super().tearDownClass()

    def setUp(self) -> None:
        self.server.assert_alive()

    @contextmanager
    def connection(
        self, receive_buffer: Optional[int] = None
    ) -> Iterator[socket.socket]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.connect_timeout)
            if receive_buffer is not None:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
            sock.connect((self.host, self.port))
            sock.settimeout(self.io_timeout)
            yield sock
        finally:
            sock.close()

    def recv_exact(
        self, sock: socket.socket, count: int, timeout: Optional[float] = None
    ) -> bytes:
        """Receive exactly count bytes from the TCP byte stream."""
        deadline = time.monotonic() + (timeout or self.workload_timeout)
        chunks: List[bytes] = []
        received = 0

        while received < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "timed out after receiving {}/{} bytes".format(received, count)
                )

            sock.settimeout(min(remaining, self.io_timeout))
            try:
                chunk = sock.recv(min(64 * 1024, count - received))
            except socket.timeout as exc:
                raise TimeoutError(
                    "no data received after {}/{} bytes".format(received, count)
                ) from exc

            if not chunk:
                raise ConnectionError(
                    "connection closed after receiving {}/{} bytes".format(
                        received, count
                    )
                )

            chunks.append(chunk)
            received += len(chunk)

        return b"".join(chunks)

    def recv_to_eof(
        self, sock: socket.socket, timeout: Optional[float] = None
    ) -> bytes:
        """Receive until the peer cleanly closes the connection."""
        deadline = time.monotonic() + (timeout or self.workload_timeout)
        chunks: List[bytes] = []

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for EOF")

            sock.settimeout(min(remaining, self.io_timeout))
            try:
                chunk = sock.recv(64 * 1024)
            except socket.timeout as exc:
                raise TimeoutError(
                    "timed out waiting for EOF after {} bytes".format(
                        sum(len(item) for item in chunks)
                    )
                ) from exc

            if not chunk:
                return b"".join(chunks)

            chunks.append(chunk)

    def echo_while_sending(
        self, sock: socket.socket, payload: bytes, hold_before_read: float = 0.0
    ) -> bytes:
        """Send in one thread while receiving in the main test thread."""
        sender_errors: List[BaseException] = []

        def sender() -> None:
            try:
                send_all(sock, payload)
            except BaseException as exc:
                sender_errors.append(exc)

        sender_thread = threading.Thread(target=sender, daemon=True)
        sender_thread.start()

        if hold_before_read > 0:
            time.sleep(hold_before_read)

        try:
            received = self.recv_exact(
                sock, len(payload), timeout=self.workload_timeout
            )
        except BaseException:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            raise
        finally:
            sender_thread.join(timeout=self.workload_timeout)

        if sender_thread.is_alive():
            self.fail("sender thread did not finish within the workload timeout")
        if sender_errors:
            raise sender_errors[0]
        return received

    def send_then_half_close(
        self, sock: socket.socket, payload: bytes
    ) -> Tuple[bytes, List[BaseException]]:
        sender_errors: List[BaseException] = []

        def sender() -> None:
            try:
                send_all(sock, payload)
                sock.shutdown(socket.SHUT_WR)
            except BaseException as exc:
                sender_errors.append(exc)

        sender_thread = threading.Thread(target=sender, daemon=True)
        sender_thread.start()

        try:
            received = self.recv_to_eof(sock, timeout=self.workload_timeout)
        finally:
            sender_thread.join(timeout=self.workload_timeout)

        if sender_thread.is_alive():
            self.fail("sender thread did not finish within the workload timeout")
        return received, sender_errors

    def fd_count(self, process_pid: int) -> int:
        return len(os.listdir("/proc/{}/fd".format(process_pid)))

    def wait_for_stable_fd_count(
        self, process_pid: int, expected: Optional[int] = None
    ) -> int:
        deadline = time.monotonic() + 5.0
        last_count = self.fd_count(process_pid)
        stable_since = time.monotonic()

        while time.monotonic() < deadline:
            time.sleep(0.05)
            current_count = self.fd_count(process_pid)

            if expected is not None and current_count == expected:
                return current_count

            if expected is None and current_count == last_count:
                if time.monotonic() - stable_since >= 0.5:
                    return current_count
            else:
                stable_since = time.monotonic()

            last_count = current_count

        return last_count

    def test_01_basic_tcp_echo(self) -> None:
        payload = b"hello from the integration test\n"
        with self.connection() as sock:
            send_all(sock, payload)
            received = self.recv_exact(
                sock, len(payload), timeout=self.io_timeout
            )
        self.assertEqual(received, payload)

    def test_02_persistent_connection(self) -> None:
        messages = [
            "message-{:03d}|".format(index).encode("ascii") for index in range(64)
        ]
        payload = b"".join(messages)

        with self.connection() as sock:
            send_all(sock, payload)
            first_round = self.recv_exact(
                sock, len(payload), timeout=self.io_timeout
            )
            self.assertEqual(first_round, payload)

            follow_up = b"same connection still works"
            send_all(sock, follow_up)
            second_round = self.recv_exact(
                sock, len(follow_up), timeout=self.io_timeout
            )
            self.assertEqual(second_round, follow_up)

    def test_03_fragmented_data_and_buffer_boundaries(self) -> None:
        sizes = [1, 4095, 4096, 4097, 8193]

        with self.connection() as sock:
            for size in sizes:
                payload = bytes([size % 251]) * size
                offset = 0
                chunk_index = 0

                while offset < len(payload):
                    chunk_size = min((1, 2, 7, 64, 511, 4096)[chunk_index % 6],
                                     len(payload) - offset)
                    send_all(sock, payload[offset:offset + chunk_size])
                    offset += chunk_size
                    chunk_index += 1
                    time.sleep(0.002)

                received = self.recv_exact(
                    sock, len(payload), timeout=self.io_timeout
                )
                self.assertEqual(received, payload, "failed for size {}".format(size))

    def test_04_large_payload(self) -> None:
        payload = patterned_payload(LARGE_PAYLOAD_BYTES)
        with self.connection() as sock:
            received = self.echo_while_sending(sock, payload)

        self.assertEqual(len(received), len(payload))
        self.assertEqual(received, payload)

    def test_05_binary_data(self) -> None:
        payload = b"\x00\xff" + patterned_payload(64 * 1024) + b"\x00\xff"
        with self.connection() as sock:
            send_all(sock, payload)
            received = self.recv_exact(
                sock, len(payload), timeout=self.workload_timeout
            )

        self.assertEqual(received, payload)

    def test_06_multiple_concurrent_clients(self) -> None:
        barrier = threading.Barrier(CONCURRENT_CLIENT_COUNT)
        expected: Dict[int, bytes] = {
            index: ("client-{:02d}:".format(index).encode("ascii")
                    + patterned_payload(CONCURRENT_PAYLOAD_BYTES))
            for index in range(CONCURRENT_CLIENT_COUNT)
        }

        def run_client(index: int) -> Tuple[int, bytes]:
            with self.connection() as sock:
                barrier.wait(timeout=self.connect_timeout * 2)
                payload = expected[index]
                send_all(sock, payload)
                return index, self.recv_exact(
                    sock, len(payload), timeout=self.workload_timeout
                )

        with ThreadPoolExecutor(max_workers=CONCURRENT_CLIENT_COUNT) as executor:
            futures = [
                executor.submit(run_client, index)
                for index in range(CONCURRENT_CLIENT_COUNT)
            ]
            for future in as_completed(futures):
                index, received = future.result()
                self.assertEqual(received, expected[index])

        self.server.assert_alive()

    def test_07_tcp_half_close(self) -> None:
        payload = patterned_payload(256 * 1024)
        with self.connection() as sock:
            received, sender_errors = self.send_then_half_close(sock, payload)

        self.assertFalse(sender_errors, "sender failed: {!r}".format(sender_errors))
        self.assertEqual(len(received), len(payload))
        self.assertEqual(received, payload)

    def test_08_empty_half_close(self) -> None:
        with self.connection() as sock:
            sock.shutdown(socket.SHUT_WR)
            received = self.recv_to_eof(sock, timeout=self.io_timeout)

        self.assertEqual(received, b"")
        self.server.assert_alive()

    def test_09_abrupt_client_disconnection(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.connect_timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(self.io_timeout)
            send_all(sock, patterned_payload(128 * 1024))

            # SO_LINGER with a zero timeout makes close() send a TCP RST.
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
        finally:
            sock.close()

        time.sleep(0.2)
        self.server.assert_alive()

        payload = b"server survived connection reset"
        with self.connection() as replacement:
            send_all(replacement, payload)
            received = self.recv_exact(
                replacement, len(payload), timeout=self.io_timeout
            )
        self.assertEqual(received, payload)

    def test_10_repeated_connections_and_fd_cleanup(self) -> None:
        if not sys.platform.startswith("linux") or not Path("/proc").is_dir():
            self.skipTest("/proc file descriptor inspection is unavailable")
        if self.server.server_pid is None:
            self.skipTest("could not determine the server process PID")

        baseline = self.wait_for_stable_fd_count(self.server.server_pid)

        for index in range(REPEATED_CONNECTION_COUNT):
            payload = "connection-{:03d}".format(index).encode("ascii")
            with self.connection() as sock:
                send_all(sock, payload)
                received = self.recv_exact(
                    sock, len(payload), timeout=self.io_timeout
                )
            self.assertEqual(received, payload)

        final_count = self.wait_for_stable_fd_count(
            self.server.server_pid, expected=baseline
        )
        self.assertEqual(
            final_count,
            baseline,
            "server leaked file descriptors after {} connections".format(
                REPEATED_CONNECTION_COUNT
            ),
        )
        self.server.assert_alive()

    def test_11_partial_write_and_backpressure(self) -> None:
        # A small receiver buffer plus a large in-flight payload forces the
        # server's outgoing path to encounter backpressure. Sending happens in
        # another thread while the main thread reads, so neither side can
        # deadlock by waiting for the other to make progress first.
        payload = (
            b"0123456789abcdef"
            * ((BACKPRESSURE_PAYLOAD_BYTES + 15) // 16)
        )[:BACKPRESSURE_PAYLOAD_BYTES]

        with self.connection(receive_buffer=4096) as sock:
            received = self.echo_while_sending(
                sock, payload, hold_before_read=0.5
            )

        self.assertEqual(len(received), len(payload))
        self.assertEqual(received, payload)

    def collect_trace_evidence(
        self,
    ) -> Tuple[List[Tuple[int, int]], List[int], int, List[int], str]:
        trace_text = self.server.trace_text()
        partial_sends, send_eagain_indices = parse_send_calls(trace_text)
        recv_eagain_count = sum(
            1
            for line in trace_text.splitlines()
            if "recvfrom(" in line
            and "= -1" in line
            and ("EAGAIN" in line or "EWOULDBLOCK" in line)
        )
        epollout_updates = [
            index
            for index, line in enumerate(trace_text.splitlines())
            if "epoll_ctl(" in line
            and "EPOLL_CTL_MOD" in line
            and "EPOLLOUT" in line
        ]
        return (
            partial_sends,
            send_eagain_indices,
            recv_eagain_count,
            epollout_updates,
            trace_text,
        )

    def test_12_straced_internal_paths(self) -> None:
        if not self.server.strace_path:
            self.skipTest(
                "strace was unavailable or disabled; internal EAGAIN path "
                "was not verified"
            )

        (
            partial_sends,
            send_eagain_indices,
            recv_eagain_count,
            epollout_updates,
            _,
        ) = self.collect_trace_evidence()

        deadline = time.monotonic() + min(self.io_timeout, 5.0)
        while time.monotonic() < deadline:
            if send_eagain_indices and epollout_updates:
                break
            time.sleep(0.1)
            (
                partial_sends,
                send_eagain_indices,
                recv_eagain_count,
                epollout_updates,
                _,
            ) = self.collect_trace_evidence()

        # Do not depend on unittest method ordering. If earlier tests did not
        # produce trace evidence, repeat the workload here.
        if not send_eagain_indices or not epollout_updates:
            payload = (
                b"0123456789abcdef"
                * ((BACKPRESSURE_PAYLOAD_BYTES + 15) // 16)
            )[:BACKPRESSURE_PAYLOAD_BYTES]
            with self.connection(receive_buffer=4096) as sock:
                received = self.echo_while_sending(
                    sock, payload, hold_before_read=0.5
                )
            self.assertEqual(received, payload)

            deadline = time.monotonic() + min(self.io_timeout, 5.0)
            while time.monotonic() < deadline:
                (
                    partial_sends,
                    send_eagain_indices,
                    recv_eagain_count,
                    epollout_updates,
                    _,
                ) = self.collect_trace_evidence()
                if send_eagain_indices and epollout_updates:
                    break
                time.sleep(0.1)

        eagain_then_epollout = any(
            eagain_index < epollout_index <= eagain_index + 50
            for eagain_index in send_eagain_indices
            for epollout_index in epollout_updates
        )

        print(
            "\n[strace evidence] partial sends={}, send EAGAIN={}, "
            "recv EAGAIN={}, EPOLLOUT MOD={}".format(
                len(partial_sends),
                len(send_eagain_indices),
                recv_eagain_count,
                len(epollout_updates),
            )
        )

        self.assertTrue(
            send_eagain_indices,
            "no send EAGAIN was observed; the backpressure path was not verified",
        )
        self.assertTrue(
            epollout_updates,
            "no EPOLLOUT update was observed",
        )
        self.assertTrue(
            eagain_then_epollout,
            "send EAGAIN was not followed by an EPOLLOUT EPOLL_CTL_MOD",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Linux TCP echo server integration tests. "
            "One server process is shared by all tests."
        )
    )
    parser.add_argument(
        "--server-bin",
        default=os.environ.get("TCP_SERVER_BIN", "cmake-build-debug/TcpServer"),
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
        default=int(os.environ.get("TCP_SERVER_PORT", DEFAULT_PORT)),
        help="server TCP port; must match the compiled server (default: %(default)s)",
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
        help="individual network I/O timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=10.0,
        help="server startup timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--workload-timeout",
        type=float,
        default=60.0,
        help="timeout for large send/receive workloads (default: %(default)s)",
    )
    parser.add_argument(
        "--strace",
        choices=("auto", "required", "off"),
        default="auto",
        help=(
            "run the server under strace for internal-path evidence; "
            "auto uses strace when installed (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list test names and exit without starting a server",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="show each test result",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    loader = unittest.TestLoader()

    if args.list:
        for test_name in loader.getTestCaseNames(EchoIntegrationTests):
            print(test_name)
        return 0

    if not sys.platform.startswith("linux"):
        print(
            "The TCP server uses Linux epoll and cannot be run on {}.\n"
            "Build and run this script on Linux; see the command guidance "
            "provided with this test suite.".format(sys.platform),
            file=sys.stderr,
        )
        return 2

    server_binary = Path(args.server_bin).expanduser().resolve()
    if not server_binary.is_file():
        print("Server binary not found: {}".format(server_binary), file=sys.stderr)
        return 2
    if not os.access(server_binary, os.X_OK):
        print(
            "Server binary is not executable: {}".format(server_binary),
            file=sys.stderr,
        )
        return 2

    strace_binary = shutil.which("strace")
    if args.strace == "required" and not strace_binary:
        print(
            "--strace required was requested, but strace is not installed "
            "or not in PATH",
            file=sys.stderr,
        )
        return 2

    EchoIntegrationTests.server_binary = server_binary
    EchoIntegrationTests.host = args.host
    EchoIntegrationTests.port = args.port
    EchoIntegrationTests.connect_timeout = args.connect_timeout
    EchoIntegrationTests.io_timeout = args.io_timeout
    EchoIntegrationTests.startup_timeout = args.startup_timeout
    EchoIntegrationTests.workload_timeout = args.workload_timeout
    EchoIntegrationTests.strace_mode = args.strace
    EchoIntegrationTests.strace_binary = (
        strace_binary if args.strace != "off" else None
    )

    suite = loader.loadTestsFromTestCase(EchoIntegrationTests)
    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)
    result = runner.run(suite)

    if result.wasSuccessful() and EchoIntegrationTests.strace_binary is None:
        print(
            "Network tests passed. Internal send EAGAIN/EPOLLOUT evidence "
            "was not collected because strace was unavailable or disabled."
        )

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
