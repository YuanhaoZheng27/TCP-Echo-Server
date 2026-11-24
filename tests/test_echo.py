#!/usr/bin/env python3
"""Integration tests for the TCP echo server.

The tests use only the Python standard library and are intended to run on
Linux because the server depends on epoll. A single server process is shared
by all tests to avoid repeated binds while SO_REUSEADDR is not configured.

Typical Linux usage:

    python3 tests/test_echo.py \
        --server-bin ./build-baseline/TcpServer \
        --strace required \
        --artifact-dir /tmp/tcp-echo-baseline \
        -v
"""

from __future__ import annotations

import argparse
import json
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
BACKPRESSURE_SENDER_WAIT_SECONDS = 2.0
BACKPRESSURE_SETTLE_SECONDS = 0.5
CONCURRENT_CLIENT_COUNT = 32
CONCURRENT_PAYLOAD_BYTES = 64 * 1024
REPEATED_CONNECTION_COUNT = 200

REAL_STRACE_SAMPLE = """\
1072183 11:12:07.480902 sendto(5, "0123456789abcdef"..., 274432, MSG_NOSIGNAL, NULL, 0) = 8192
1072183 11:12:07.480956 sendto(5, "0123456789abcdef"..., 266240, MSG_NOSIGNAL, NULL, 0) = 4096
1072183 11:12:07.481832 sendto(5, "0123456789abcdef"..., 176128, MSG_NOSIGNAL, NULL, 0) = -1 EAGAIN (Resource temporarily unavailable)
1072183 11:12:07.481877 epoll_ctl(4, EPOLL_CTL_MOD, 5, {events=EPOLLIN|EPOLLOUT|EPOLLRDHUP, data={u32=5, u64=5}}) = 0
1072183 11:12:07.481915 epoll_wait(4, [{events=EPOLLOUT, data={u32=5, u64=5}}], 10, -1) = 1
1072183 11:12:07.484743 sendto(5, "0123456789abcdef"..., 176128, MSG_NOSIGNAL, NULL, 0) = 176128
1072183 11:12:07.484965 epoll_ctl(4, EPOLL_CTL_MOD, 5, {events=EPOLLIN|EPOLLRDHUP, data={u32=5, u64=5}}) = 0
"""


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


def parse_send_arguments(
    syscall_name: str, arguments: str
) -> Optional[Tuple[int, int]]:
    """Extract FD and buffer length from strace send/sendto arguments."""
    parts = [part.strip() for part in arguments.rsplit(",", 5)]

    # glibc send() on Linux is traced as:
    # sendto(fd, buffer, length, flags, destination_address, address_length)
    # The destination is normally NULL with address_length 0.
    if (
        syscall_name == "sendto"
        and len(parts) == 6
        and parts[2].isdigit()
        and parts[5].isdigit()
    ):
        fd_field = parts[0]
        length_field = parts[2]
    else:
        parts = [part.strip() for part in arguments.rsplit(",", 3)]
        if len(parts) != 4 or not parts[2].isdigit():
            return None
        fd_field = parts[0]
        length_field = parts[2]

    fd_match = re.match(r"^(?P<fd>\d+)", fd_field)
    if not fd_match:
        return None
    return int(fd_match.group("fd")), int(length_field)


def parse_send_calls(trace_text: str) -> Dict[str, object]:
    """Analyze all send syscall records while tolerating strace layouts."""
    partial_sends: List[Tuple[int, int]] = []
    send_calls: List[Dict[str, object]] = []
    partial_send_calls: List[Dict[str, object]] = []
    send_eagain_calls: List[Dict[str, object]] = []
    send_other_error_calls: List[Dict[str, object]] = []
    eagain_indices: List[int] = []
    other_error_indices: List[int] = []
    send_line_indices: List[int] = []
    full_send_count = 0
    parsed_send_count = 0
    call_pattern = re.compile(r"\b(?:sendto|send)\(")
    head_pattern = re.compile(
        r"\b(?P<syscall>sendto|send)\((?P<arguments>.*)\)$"
    )
    result_pattern = re.compile(r"\s*(?P<result>-?\d+)(?P<tail>.*)$")

    for index, line in enumerate(trace_text.splitlines()):
        if not call_pattern.search(line):
            continue

        send_line_indices.append(index)
        head, separator, result_text = line.rpartition(" = ")
        if not separator:
            continue

        head_match = head_pattern.search(head)
        if not head_match:
            continue

        parsed_arguments = parse_send_arguments(
            head_match.group("syscall"), head_match.group("arguments")
        )
        result_match = result_pattern.match(result_text)
        if parsed_arguments is None or not result_match:
            continue

        parsed_send_count += 1
        fd, requested = parsed_arguments
        result = int(result_match.group("result"))
        tail = result_match.group("tail")
        send_call = {
            "line_index": index,
            "fd": fd,
            "requested": requested,
            "result": result,
        }
        send_calls.append(send_call)

        if result > 0 and result == requested:
            send_call["outcome"] = "full"
            full_send_count += 1
        elif 0 < result < requested:
            send_call["outcome"] = "partial"
            partial_sends.append((requested, result))
            partial_send_calls.append(send_call)
        elif result == -1 and ("EAGAIN" in tail or "EWOULDBLOCK" in tail):
            send_call["outcome"] = "eagain"
            eagain_indices.append(index)
            send_eagain_calls.append(send_call)
        elif result < 0:
            send_call["outcome"] = "error"
            other_error_indices.append(index)
            send_other_error_calls.append(send_call)
        else:
            send_call["outcome"] = "other"

    return {
        "send_line_count": len(send_line_indices),
        "parsed_send_count": parsed_send_count,
        "full_send_count": full_send_count,
        "send_calls": send_calls,
        "partial_sends": partial_sends,
        "partial_send_calls": partial_send_calls,
        "send_eagain_indices": eagain_indices,
        "send_eagain_calls": send_eagain_calls,
        "send_other_error_indices": other_error_indices,
        "send_other_error_calls": send_other_error_calls,
    }


def parse_epollout_mod_updates(trace_text: str) -> List[Dict[str, int]]:
    """Return EPOLLOUT MOD updates with the monitored socket FD."""
    pattern = re.compile(
        r"\bepoll_ctl\(\s*\d+,\s*EPOLL_CTL_MOD,\s*(?P<fd>\d+),"
        r".*EPOLLOUT"
    )
    updates: List[Dict[str, int]] = []
    for index, line in enumerate(trace_text.splitlines()):
        match = pattern.search(line)
        if match:
            updates.append(
                {"line_index": index, "target_fd": int(match.group("fd"))}
            )
    return updates


def parse_epollout_notifications(trace_text: str) -> List[Dict[str, int]]:
    """Return EPOLLOUT-ready notifications and their associated socket FD."""
    pattern = re.compile(
        r"\bepoll_wait\(.*?events=[A-Z0-9_|]*EPOLLOUT[A-Z0-9_|]*"
        r".*?data=\{u32=(?P<fd>\d+)"
    )
    notifications: List[Dict[str, int]] = []
    for index, line in enumerate(trace_text.splitlines()):
        match = pattern.search(line)
        if match:
            notifications.append(
                {"line_index": index, "target_fd": int(match.group("fd"))}
            )
    return notifications


def find_eagain_epollout_transition(
    send_evidence: Dict[str, object],
    epollout_updates: List[Dict[str, int]],
) -> Optional[Dict[str, int]]:
    """Find a same-FD EPOLLOUT MOD shortly after a send EAGAIN."""
    send_eagain_calls = send_evidence["send_eagain_calls"]
    for send_call in send_eagain_calls:
        send_index = int(send_call["line_index"])
        send_fd = int(send_call["fd"])
        for update in epollout_updates:
            update_index = int(update["line_index"])
            update_fd = int(update["target_fd"])
            if (
                send_fd == update_fd
                and send_index < update_index <= send_index + 50
            ):
                return {
                    "send_fd": send_fd,
                    "send_line_index": send_index,
                    "update_line_index": update_index,
                }
    return None


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
        self.command: List[str] = []
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
                    "16",
                    "-e",
                    (
                        "trace=network,sendto,recvfrom,epoll_ctl,"
                        "epoll_wait,close"
                    ),
                    "-o",
                    str(self.trace_path),
                ]
            )
        command.append(str(self.binary))
        self.command = list(command)

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

    def trace_size(self) -> int:
        try:
            return self.trace_path.stat().st_size
        except OSError:
            return 0

    def trace_text(self, start_offset: int = 0) -> str:
        try:
            data = self.trace_path.read_bytes()
        except OSError:
            return ""
        return data[start_offset:].decode("utf-8", errors="replace")


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
    artifact_dir_override: Optional[Path] = None
    backpressure_trace_offset: Optional[int] = None

    server: ManagedServer
    artifacts: Optional[tempfile.TemporaryDirectory] = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.backpressure_trace_offset = None
        if cls.artifact_dir_override is not None:
            artifact_dir = cls.artifact_dir_override
            artifact_dir.mkdir(parents=True, exist_ok=True)
            cls.artifacts = None
        else:
            cls.artifacts = tempfile.TemporaryDirectory(prefix="tcp-echo-tests-")
            artifact_dir = Path(cls.artifacts.name)
        print("\nTest diagnostics directory: {}".format(artifact_dir), flush=True)

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
            if cls.artifacts is not None:
                cls.artifacts.cleanup()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.server.stop()
        finally:
            if cls.artifacts is not None:
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
        self,
        sock: socket.socket,
        payload: bytes,
        hold_before_read: float = 0.0,
        wait_for_sender_before_read: bool = False,
    ) -> bytes:
        """Send in one thread while receiving in the main test thread."""
        sender_errors: List[BaseException] = []
        sender_finished = threading.Event()

        def sender() -> None:
            try:
                send_all(sock, payload)
            except BaseException as exc:
                sender_errors.append(exc)
            finally:
                sender_finished.set()

        sender_thread = threading.Thread(target=sender, daemon=True)
        sender_thread.start()

        if wait_for_sender_before_read:
            sender_finished.wait(
                timeout=min(
                    self.workload_timeout, BACKPRESSURE_SENDER_WAIT_SECONDS
                )
            )

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
        type(self).backpressure_trace_offset = self.server.trace_size()
        payload = (
            b"0123456789abcdef"
            * ((BACKPRESSURE_PAYLOAD_BYTES + 15) // 16)
        )[:BACKPRESSURE_PAYLOAD_BYTES]

        with self.connection(receive_buffer=4096) as sock:
            received = self.echo_while_sending(
                sock,
                payload,
                hold_before_read=BACKPRESSURE_SETTLE_SECONDS,
                wait_for_sender_before_read=True,
            )

        self.assertEqual(len(received), len(payload))
        self.assertEqual(received, payload)

    def collect_trace_evidence(
        self, start_offset: int = 0
    ) -> Tuple[Dict[str, object], str]:
        trace_text = self.server.trace_text(start_offset)
        lines = trace_text.splitlines()
        evidence = parse_send_calls(trace_text)
        evidence["trace_line_count"] = len(lines)
        evidence["recv_eagain_indices"] = [
            index
            for index, line in enumerate(lines)
            if "recvfrom(" in line
            and "= -1" in line
            and ("EAGAIN" in line or "EWOULDBLOCK" in line)
        ]
        epollout_updates = parse_epollout_mod_updates(trace_text)
        epollout_notifications = parse_epollout_notifications(trace_text)
        evidence["epollout_updates"] = epollout_updates
        evidence["epollout_notifications"] = epollout_notifications
        evidence["epollout_update_indices"] = [
            int(update["line_index"]) for update in epollout_updates
        ]
        return evidence, trace_text

    def test_12_straced_internal_paths(self) -> None:
        sample_evidence = parse_send_calls(REAL_STRACE_SAMPLE)
        sample_epollout_updates = parse_epollout_mod_updates(REAL_STRACE_SAMPLE)
        sample_epollout_notifications = parse_epollout_notifications(
            REAL_STRACE_SAMPLE
        )
        sample_transition = find_eagain_epollout_transition(
            sample_evidence, sample_epollout_updates
        )

        self.assertEqual(sample_evidence["send_line_count"], 4)
        self.assertEqual(sample_evidence["parsed_send_count"], 4)
        self.assertEqual(
            sample_evidence["partial_sends"],
            [(274432, 8192), (266240, 4096)],
        )
        self.assertEqual(
            [call["fd"] for call in sample_evidence["partial_send_calls"]],
            [5, 5],
        )
        self.assertEqual(sample_evidence["full_send_count"], 1)
        self.assertEqual(
            sample_evidence["send_calls"][-1],
            {
                "line_index": 5,
                "fd": 5,
                "requested": 176128,
                "result": 176128,
                "outcome": "full",
            },
        )
        self.assertEqual(
            sample_evidence["send_eagain_calls"],
            [
                {
                    "line_index": 2,
                    "fd": 5,
                    "requested": 176128,
                    "result": -1,
                    "outcome": "eagain",
                }
            ],
        )
        self.assertEqual(
            sample_epollout_updates,
            [{"line_index": 3, "target_fd": 5}],
        )
        self.assertEqual(
            sample_epollout_notifications,
            [{"line_index": 4, "target_fd": 5}],
        )
        self.assertEqual(
            sample_transition,
            {
                "send_fd": 5,
                "send_line_index": 2,
                "update_line_index": 3,
            },
        )
        print(
            "\n[real trace parser sample] send lines=4, parsed=4, partial=2, "
            "full=1, send EAGAIN=1 on FD 5, EPOLLOUT MOD on FD 5",
            flush=True,
        )

        if not self.server.strace_path:
            self.skipTest(
                "strace was unavailable or disabled; internal EAGAIN path "
                "was not verified"
            )

        trace_offset = type(self).backpressure_trace_offset
        if trace_offset is None:
            trace_offset = self.server.trace_size()
            type(self).backpressure_trace_offset = trace_offset

        evidence, trace_text = self.collect_trace_evidence(trace_offset)

        deadline = time.monotonic() + min(self.io_timeout, 5.0)
        while time.monotonic() < deadline:
            if find_eagain_epollout_transition(
                evidence, evidence["epollout_updates"]
            ):
                break
            time.sleep(0.1)
            evidence, trace_text = self.collect_trace_evidence(trace_offset)

        # Do not depend on unittest method ordering. If earlier tests did not
        # produce trace evidence, repeat the workload here.
        if not find_eagain_epollout_transition(
            evidence, evidence["epollout_updates"]
        ):
            trace_offset = self.server.trace_size()
            type(self).backpressure_trace_offset = trace_offset
            payload = (
                b"0123456789abcdef"
                * ((BACKPRESSURE_PAYLOAD_BYTES + 15) // 16)
            )[:BACKPRESSURE_PAYLOAD_BYTES]
            with self.connection(receive_buffer=4096) as sock:
                received = self.echo_while_sending(
                    sock,
                    payload,
                    hold_before_read=BACKPRESSURE_SETTLE_SECONDS,
                    wait_for_sender_before_read=True,
                )
            self.assertEqual(received, payload)

            deadline = time.monotonic() + min(self.io_timeout, 5.0)
            while time.monotonic() < deadline:
                evidence, trace_text = self.collect_trace_evidence(trace_offset)
                if find_eagain_epollout_transition(
                    evidence, evidence["epollout_updates"]
                ):
                    break
                time.sleep(0.1)

        send_eagain_calls = evidence["send_eagain_calls"]
        epollout_updates = evidence["epollout_updates"]
        eagain_then_epollout = find_eagain_epollout_transition(
            evidence, epollout_updates
        )

        trace_lines = trace_text.splitlines()
        send_samples = [
            line for line in trace_lines if re.search(r"\b(?:sendto|send)\(", line)
        ][-20:]
        send_eagain_samples = [
            line
            for line in trace_lines
            if re.search(r"\b(?:sendto|send)\(", line)
            and ("EAGAIN" in line or "EWOULDBLOCK" in line)
        ][-20:]
        epollout_samples = [
            line
            for line in trace_lines
            if "epoll_ctl(" in line
            and "EPOLL_CTL_MOD" in line
            and "EPOLLOUT" in line
        ][-20:]
        summary_path = self.server.artifact_dir / "trace-evidence.json"
        summary = {
            "trace_path": str(self.server.trace_path),
            "server_log_path": str(self.server.log_path),
            "server_command": self.server.command,
            "trace_offset": trace_offset,
            "trace_line_count": evidence["trace_line_count"],
            "send_line_count": evidence["send_line_count"],
            "parsed_send_count": evidence["parsed_send_count"],
            "full_send_count": evidence["full_send_count"],
            "partial_send_count": len(evidence["partial_sends"]),
            "partial_sends": evidence["partial_sends"][:20],
            "partial_send_calls": evidence["partial_send_calls"][:20],
            "send_eagain_count": len(send_eagain_calls),
            "send_eagain_calls": send_eagain_calls[:20],
            "send_other_error_count": len(
                evidence["send_other_error_indices"]
            ),
            "recv_eagain_count": len(evidence["recv_eagain_indices"]),
            "epollout_mod_count": len(epollout_updates),
            "epollout_notification_count": len(
                evidence["epollout_notifications"]
            ),
            "epollout_updates": epollout_updates[:20],
            "epollout_notifications": evidence["epollout_notifications"][:20],
            "eagain_then_epollout": eagain_then_epollout,
            "send_samples": send_samples,
            "send_eagain_samples": send_eagain_samples,
            "epollout_samples": epollout_samples,
            "trace_tail": trace_lines[-40:],
        }
        summary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )

        print(
            "\n[strace evidence] send lines={}, parsed sends={}, full sends={}, "
            "partial sends={}, send EAGAIN={}, recv EAGAIN={}, EPOLLOUT MOD={}"
            "\nRaw trace: {}\nSummary: {}\nServer log: {}".format(
                evidence["send_line_count"],
                evidence["parsed_send_count"],
                evidence["full_send_count"],
                len(evidence["partial_sends"]),
                len(send_eagain_calls),
                len(evidence["recv_eagain_indices"]),
                len(epollout_updates),
                self.server.trace_path,
                summary_path,
                self.server.log_path,
            )
        )

        self.assertTrue(
            evidence["partial_sends"],
            "no positive partial send was observed; the server accepted either "
            "the complete request or no bytes before EAGAIN; raw trace: {}".format(
                self.server.trace_path
            ),
        )
        self.assertTrue(
            send_eagain_calls,
            "no send EAGAIN was observed; send lines={}, parsed sends={}; "
            "raw trace: {}".format(
                evidence["send_line_count"],
                evidence["parsed_send_count"],
                self.server.trace_path,
            ),
        )
        self.assertTrue(
            epollout_updates,
            "no EPOLLOUT update was observed; raw trace: {}".format(
                self.server.trace_path
            ),
        )
        self.assertTrue(
            eagain_then_epollout,
            "no same-FD EPOLLOUT EPOLL_CTL_MOD followed send EAGAIN; "
            "raw trace: {}".format(self.server.trace_path),
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
        "--artifact-dir",
        type=Path,
        default=None,
        help=(
            "preserve server.log, raw server.strace, and trace-evidence.json "
            "in this directory"
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
    EchoIntegrationTests.artifact_dir_override = (
        args.artifact_dir.expanduser().resolve()
        if args.artifact_dir is not None
        else None
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
