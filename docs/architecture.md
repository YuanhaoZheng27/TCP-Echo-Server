# Architecture

## Scope

This document describes the implementation in
[`src/TcpServer.cpp`](../src/TcpServer.cpp) and
[`src/TcpServer.h`](../src/TcpServer.h). The server is a single-threaded,
level-triggered `epoll` echo server. It does not perform HTTP parsing and does
not define an application-level message protocol.

## Component Overview

`TcpServer` is the reactor. It owns the listening socket, epoll instance, and
client state map. The program entry point only creates the object and delegates
to `run()`.

```text
TcpServer::run()
├── setupServer()
│   ├── createListeningSocket()
│   ├── createEpollInstance()
│   ├── setNonBlocking()
│   └── registerListeningSocket()
├── runEventLoop()
│   └── handleEvent()
│       ├── handleAccept()
│       └── handleClientEvent()
│           ├── handleClientRead()
│           ├── handleClientWrite()
│           ├── updateClientEvents()
│           └── cleanupClient()
└── closeServerResources()
```

The listening FD and epoll FD are initialized to `-1`. Client ownership is
tracked by `std::unordered_map<int, ClientState>` with the file descriptor as
the key and `epoll_event.data.fd` as the event token.

## Socket Initialization

`createListeningSocket()` performs the following sequence:

1. Build IPv4 hints with `AF_INET`, `SOCK_STREAM`, and `AI_PASSIVE`.
2. Resolve numeric service `54000` with `getaddrinfo()`.
3. Create a TCP socket.
4. Bind it to the passive wildcard IPv4 address.
5. Call `listen()` with `SOMAXCONN`.

Only the first `addrinfo` result is used. `SO_REUSEADDR` is not enabled in the
current version.

`setupServer()` then creates the epoll instance, marks the listening socket
non-blocking, and registers it with `EPOLLIN`.

## Non-blocking I/O

The listening socket and every accepted client socket are changed to
non-blocking mode using:

```text
fcntl(fd, F_GETFL, 0)
fcntl(fd, F_SETFL, flags | O_NONBLOCK)
```

`TcpServer::NonBlockingResult` distinguishes failure to read the existing flags
from failure to update them. The listening socket maps those failures to the
original setup exit codes, while accepted client sockets are closed when
non-blocking setup fails.

Non-blocking mode changes the meaning of these conditions:

- `accept()` returning `EAGAIN` means no more pending connections.
- `recv()` returning `EAGAIN` means no more data is currently available.
- `send()` returning `EAGAIN` means the socket cannot currently accept more
  data into its send buffer.

## Level-triggered epoll

The server calls `epoll_create1(0)` and does not add `EPOLLET`. It therefore
uses epoll's default level-triggered behavior.

The listening socket is registered with:

```text
EPOLLIN
```

Each accepted client starts with:

```text
EPOLLIN | EPOLLRDHUP
```

Level-triggered mode reports a socket again while data remains unread or while
the socket remains writable after an `EPOLLOUT` interest is registered. The
read and write handlers consequently drain readiness until `EAGAIN`.

`MAX_EVENTS` is fixed at `10`, so each `epoll_wait()` call returns at most ten
ready events. Remaining ready descriptors are reported in later loop iterations.

## Reactor-style Event Loop

`runEventLoop()` repeatedly calls `epoll_wait()` and delegates each event to
`handleEvent()`.

```text
epoll_wait
    |
    +-- listening FD --> handleAccept
    |
    +-- client FD -----> handleClientEvent
                             |
                             +-- readable/HUP/RDHUP --> read
                             +-- EPOLLERR -----------> close decision
                             +-- writable work ------> send
                             +-- state changed ------> epoll_ctl(MOD)
                             +-- no longer usable ---> DEL, close, erase
```

`EINTR` from `epoll_wait()`, `accept()`, `recv()`, and `send()` is retried.
Other fatal client-side errors set the handler result to `false`.

For client processing methods, the Boolean convention is:

- `true`: the operation was normal and the connection can remain registered.
- `false`: the connection must be cleaned up.

## Client State Management

Each connection stores only the state required by the current implementation:

```cpp
struct ClientState
{
    std::string pendingSend;
    bool readEOF = false;
};
```

`pendingSend` contains echoed bytes that have not yet been accepted by the
client socket. `readEOF` records that `recv()` returned zero.

The file descriptor remains the key in `clients_`. An event whose FD is no
longer present in the map is ignored.

## Partial Read and Partial Write

### Read path

`handleClientRead()` allocates a 4096-byte stack buffer and repeatedly calls
`recv()`:

- A positive result is appended to `pendingSend`.
- Zero sets `readEOF` and ends reading for that connection.
- `EINTR` retries the call.
- `EAGAIN`/`EWOULDBLOCK` returns normally with the connection preserved.
- Any other error marks the connection for cleanup.

TCP does not preserve write boundaries. A client write may arrive as one read,
several reads, or a read coalesced with later data. The server treats the
connection as a byte stream and does not attempt message parsing.

### Write path

`handleClientWrite()` repeatedly sends the current contents of `pendingSend`:

- A positive result removes the accepted prefix.
- Zero is treated as a fatal send condition.
- `EINTR` retries.
- `EAGAIN`/`EWOULDBLOCK` leaves the remaining data buffered.
- Other errors mark the connection for cleanup.

`MSG_NOSIGNAL` is used to avoid process termination from `SIGPIPE` when a peer
has disconnected.

## Pending Send Buffer, Backpressure, and Dynamic EPOLLOUT

Echo data is appended to `pendingSend` after it is read. The server then
attempts to send the buffer until it is empty or the kernel reports `EAGAIN`.

When `pendingSend` remains non-empty, the next event mask includes `EPOLLOUT`.
When the socket becomes writable again, the loop attempts to send the remaining
data. Once the buffer is empty, the next mask omits `EPOLLOUT`.

The interest mask is calculated as:

```text
events = 0
if !readEOF:
    events |= EPOLLIN | EPOLLRDHUP
if !pendingSend.empty():
    events |= EPOLLOUT
```

The buffer is a `std::string` and has no configured maximum. A slow-reading
client can therefore cause memory growth if it continues sending input.

## TCP Half-close

When the peer calls `shutdown(SHUT_WR)`, the server's `recv()` eventually
returns zero. The server:

1. Sets `readEOF = true`.
2. Stops attempting to read from that client.
3. Continues sending any data already in `pendingSend`.
4. Closes the connection only after the pending buffer becomes empty.

The connection states are approximately:

| State | Interest mask | Transition |
|---|---|---|
| Active | `EPOLLIN \| EPOLLRDHUP` | Peer closes its write side |
| Send blocked | `EPOLLIN \| EPOLLRDHUP \| EPOLLOUT` | Pending buffer drains or EAGAIN repeats |
| EOF with pending output | `EPOLLOUT` | Output drains |
| Closed | none | `DEL`, `close`, remove from map |

This behavior preserves the half-close meaning: the peer has no more data to
send, but can still receive the remaining echo response.

## FD Ownership and Resource Lifecycle

The server owns three categories of FD:

| Owner | Resource |
|---|---|
| `TcpServer::listeningSocket_` | Listening socket |
| `TcpServer::epollFD_` | epoll instance |
| `TcpServer::clients_` keys | Connected client sockets |

Accepted client FDs have a clear ownership transition:

1. `accept()` returns a local client FD.
2. If setup or `EPOLL_CTL_ADD` fails, `registerClient()` closes it.
3. After successful registration, `clients_` owns the FD until
   `cleanupClient()` closes and erases it.

`cleanupClient()` performs:

```text
epoll_ctl(EPOLL_CTL_DEL)
close(fd)
clients_.erase(fd)
```

`closeServerResources()` closes remaining client FDs, then the listening socket
and epoll FD, and resets the owned FDs to `-1`. The destructor calls the same
function, making normal cleanup idempotent.

Initialization errors also flow through `run()` and
`closeServerResources()`, so partially initialized resources use one cleanup
path.

There is currently no signal-driven graceful shutdown. If the process receives
SIGINT or SIGTERM, the operating system reclaims its resources, but the C++
cleanup sequence does not run.

## Related Documentation

- [Project overview](../README.md)
- [Benchmark methodology and results](benchmark.md)

