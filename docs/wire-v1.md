# UDP transport wire version 1

Version 1 implements the behavior in [protocol.md](protocol.md) in the dedicated
`internal/transport` Go package. It uses only the standard library. The transport
carries opaque ordered byte streams and target names; opening a Windows resource
and authorizing access are responsibilities of the accepting application.

## Datagram header

All integers are unsigned and big endian. Each UDP datagram contains exactly one
packet. The default maximum UDP payload is 1,200 bytes, including this header.

| Byte offset | Size | Field |
| --- | --- | --- |
| 0 | 2 | Magic bytes `WP` |
| 2 | 1 | Version, currently 1 |
| 3 | 1 | Packet kind |
| 4 | 2 | Connection ID, zero only for OPEN and REJECT |
| 6 | 2 | Payload byte length, excluding the header |
| 8 | 4 | DATA or FIN sequence number; zero for other kinds |
| 12 | 4 | Inclusive receive limit for OPEN, OPENED, ACK, KEEPALIVE; zero otherwise |
| 16 | Variable | Kind-specific payload |

Unknown versions/kinds, inconsistent lengths, invalid reserved fields, and
oversized datagrams are discarded. The UDP checksum is supplied by the network
stack; the protocol adds no cryptographic integrity or encryption.

| Kind | Value | Payload |
| --- | --- | --- |
| OPEN | 1 | 48-byte opening profile followed by a nonempty target name |
| OPENED | 2 | 48-byte opening profile |
| REJECT | 3 | 8-byte request token |
| DATA | 4 | One or more stream bytes |
| ACK | 5 | Zero or more 4-byte DATA/FIN sequence numbers |
| KEEPALIVE | 6 | Empty |
| FIN | 7 | Empty; consumes a sequence number and window slot |
| RESET | 8 | Empty |

ACK entries are selective and unordered. An empty ACK updates receive capacity.
Every accepted DATA/FIN reception, including a duplicate, schedules an ACK.
Repeated entries in a pending ACK batch can be combined. FIN is sequenced after
preceding bytes; EOF is exposed only once those bytes have been consumed.

## Opening profile

These offsets are relative to the payload:

| Byte offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Request token |
| 8 | 4 | Sender's receive-window capacity in packets |
| 12 | 4 | Maximum datagram bytes, including header |
| 16 | 8 | Connection timeout, nanoseconds |
| 24 | 8 | DATA/FIN delivery timeout, nanoseconds |
| 32 | 8 | Keepalive interval, nanoseconds |
| 40 | 8 | Receive inactivity timeout, nanoseconds |
| 48 | Variable | Target name, OPEN only |

The header's initial receive limit equals the advertised window capacity because
both directions start at sequence 1. Window capacities can differ between peers.
Datagram size and the four lifetime settings must match exactly; incompatible
profiles are rejected. Each window and the combined local send/peer receive
window must stay below the uint32 half-range. Local validation also bounds the
combined local send/receive windows.

Target names are opaque nonempty bytes represented by a Go string, fitting within
one OPEN packet. The application decides which names exist and how to encode
them. The request token is generated with `crypto/rand`, correlates retries only,
and is not present in established DATA/ACK traffic. It is neither authentication
nor a session-generation field.

## Opening and admission

The client binds a dedicated UDP socket and retries OPEN using exponential backoff
until OPENED, REJECT, context cancellation, or the fixed connection timeout.
OPENED must come from the requested peer and echo the request token and a compatible
profile. A successful dial owns its socket until closure or failure.

The listener keys opening requests by peer UDP endpoint plus token. A new valid
request reserves payload budget, allocates an eligible FIFO ID, creates a stream,
sends OPENED, and queues the stream for acceptance. A duplicate request with the
same target and window reuses that response and never queues another stream.
Opening acceptance confirms only the transport, not target availability.

Admission rejects requests when the accept backlog, eligible IDs, or payload
budget are exhausted. The default buffer budget is 64 MiB. Admission reserves
`(SendWindowPackets + ReceiveWindowPackets) * (MaxDatagramBytes - 16)` payload
bytes per connection; actual payload allocation is lazy. Metadata, socket buffers,
and caller-owned buffers are additional memory. No unlimited application send
queue exists.

On closure, the server releases payload reservations and quarantines the ID for
20 seconds by default. The opening request becomes a rejection tombstone until
that slot is reused. This prevents a retry from reopening the same request during
quarantine. Lost opening responses leave server state bounded by receive inactivity.
An application should drain pending accepts; failed queued streams are skipped.

FIFO allocation and quarantine do not eliminate ambiguity for sufficiently late
packets or process/endpoint reuse. This is the explicitly accepted limitation of
omitting session generations.

## Stream state and timers

The receive window is anchored at the next unconsumed sequence. Partial reads
retain the packet's slot. ACK acceptance and consumer release of capacity are
separate. Out-of-order packets occupy bounded slots and cannot move the window
past missing data. Consumed duplicates are recognized within the unambiguous
sequence-history range. Conflicting buffered retransmissions are discarded.

Receive limits only advance in modular sequence order. A new advertisement cannot
claim consumption beyond the packets actually sent. Exactly half-range comparisons
are rejected. Older limits do not shrink the current allowance.

The first pending ACK starts a fixed flush deadline. More arrivals cannot postpone
it. The batch flushes at the configured count or deadline; consumption can flush
it sooner to advertise freed capacity. Timers are processed by a shared 1 ms scan
by default, so delivery is subject to scheduler and socket scheduling latency.

Each outstanding packet records its original send time, last send time, and retry
deadline. A scan retries only due packets. A scan containing retries is one round:
the delay for the next retries doubles once, up to the configured ceiling. Genuine
ACK progress resets the retry interval and recomputes outstanding retry deadlines
from their last transmission. Unknown and duplicate ACKs do not reset backoff.
The original send time always governs the absolute delivery timeout.

KEEPALIVE carries the current receive limit. It is sent after an interval without
outgoing traffic or without a capacity advertisement. This second condition
recovers a lost window-only ACK even if the reverse direction keeps sending DATA.
Only valid incoming traffic updates receive liveness. Keepalive does not extend
an outstanding DATA/FIN delivery deadline.

## EOF and closure

`CloseWrite` queues FIN after prior writes, waiting for window capacity if needed.
FIN is retried and acknowledged like DATA. Its acceptance prohibits later stream
data in that direction. The peer exposes `io.EOF` after consuming preceding bytes;
the reverse direction remains usable. Consumed FIN duplicates are re-ACKed.

`Close` is abortive: it discards local pending state and sends one best-effort
RESET. Receiving RESET fails the connection with `ErrPeerClosed`. A lost RESET
is bounded by the peer's receive inactivity and delivery timeouts. Do not call
Close immediately after CloseWrite expecting it to flush pending output; keep the
connection alive for the application exchange to finish. Neither FIN nor a
successful Write means the target application processed the bytes.

Closing a listener closes all its streams and its UDP socket. No automatic
reconnection or application-byte replay occurs after any failure.

## Go API and concurrency

| API | Contract |
| --- | --- |
| `DefaultConfig`, `Config.Validate` | Explicit starting defaults and validation; zero Config is invalid |
| `Listen`, `ListenPacket` | Bind UDP or take ownership of an existing PacketConn |
| `Listener.Accept`, `AcceptContext` | Return independent streams; context limits acceptance |
| `DialContext`, `DialPacket` | Establish one stream for a target; context limits opening only |
| `Conn.Target` | Retrieve the opaque opening target name |
| `Conn.Read`, `Write` | Ordered stream I/O; Write copies into bounded windows |
| `Conn.CloseWrite` | Queue reliable EOF; write deadline applies |
| `Conn.Close` | Abort and release local resources |
| `Conn.Done` | Channel closed on connection failure/Close; FIN alone leaves it open |
| `SetDeadline`, `SetReadDeadline`, `SetWriteDeadline` | Change current and future I/O deadlines; zero disables |

`Conn` implements `net.Conn`; `Listener` implements `net.Listener`. Socket-taking
constructors take ownership even when validation fails. Methods on returned
connections may run concurrently. Writes are serialized so FIN cannot overtake
an earlier Write. Deadline changes wake blocked operations; expiration returns
an error matching `os.ErrDeadlineExceeded`. Partial writes report the number of
bytes already queued, and such bytes must not be blindly replayed after failure.

The public `client` package exposes `Config`, `Conn`, `DefaultConfig`, and
`DialContext`, plus the transport's sentinel errors for use with `errors.Is`.
Consumers do not need to import the internal transport package. `Conn.Done`
signals abort/failure independently of I/O, allowing resource adapters to cancel
blocked target operations; receiving FIN alone does not signal failure.

An endpoint serializes state under a mutex. It has one socket reader and one
scheduler worker. Socket writes have a configured deadline; closing the endpoint
interrupts socket I/O before waiting for workers. There is no timer or worker per
outstanding packet. Each client dial currently uses its own socket and workers;
the server multiplexes clients on one endpoint. High-connection-count performance
has not yet been benchmarked.

## Validation boundary

A separate [Quint lifecycle model](../formal/transport/README.md) checks eventual
peer closure in a finite established-connection abstraction, assuming scheduler
progress and finite stale traffic. It is not a proof of the Go implementation,
sequence wraparound, FIN behavior, or ID reuse.

Deterministic state-machine tests inject packet loss, duplication, reordering,
slow consumption, and wrapping counters with simulated time. Socket tests exercise
opening loss, bidirectional transfer, FIN retries, deadlines, peer binding,
admission, quarantine, concurrent connections, and shutdown on Linux and Windows
loopback. Race detection and AMD64/ARM64 Windows cross-compilation supplement
these checks. Actual WSL-to-Windows UDP and 1Password named-pipe identity queries
are verified; see [Windows SSH-agent findings](windows-ssh-agent.md) for the tested
scope and remaining limits. The wire format is unchanged by the forwarding layer.
