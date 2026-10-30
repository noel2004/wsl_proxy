# WSL proxy transport protocol

## Purpose and status

This protocol carries ordered, bidirectional byte streams between a Linux client
library and a Windows server over UDP. Each logical connection accesses a Windows
resource selected from the targets configured when the server starts. The first
target is the Windows OpenSSH agent named pipe.

The transport is implemented in Go in this project. KCP and QUIC were considered;
the selected approach is a custom transport with bounded windows, selective ACKs,
batched acknowledgment, and timed retransmission.

This is the behavioral design implemented by `internal/transport`.
[Wire version 1](wire-v1.md) specifies the packet encoding, opening exchange,
closure, and public Go API. Numeric defaults remain configurable and should be
tuned through measurement.

## Deployment assumptions

- Both endpoints normally run on the same host, across WSL and Windows.
- WSL mirrored networking is the baseline deployment environment.
- Each accepted logical connection has independent state and its own target
  resource connection. The target byte stream is opaque to the transport.
- The server exposes only its startup-configured targets. Opening arbitrary
  client-supplied Windows paths is outside the intended interface.
- The maximum number of concurrent logical connections is 65,535 server-wide.
  This is an admission ceiling, not a measured Windows handle capacity.

## Packet concepts

Every datagram has a compact header identifying its packet kind and, for an
established connection, its connection ID. Version 1 uses a 16-byte big-endian
header; exact layouts are specified in [wire-v1.md](wire-v1.md).

| Kind | Purpose |
| --- | --- |
| OPEN and response | Establish a logical connection, select a configured target, and communicate initial receive limits |
| DATA | Carry bytes and a sequence number in the sender's direction |
| ACK | Acknowledge accepted DATA sequence numbers and advertise a receive limit for the opposite direction |
| KEEPALIVE | Maintain receive liveness and recover knowledge of current receive limits |
| Close and error control | Report EOF, explicit closure, or failure; sequenced FIN for EOF and best-effort RESET for abort |

ACK packets may have no ACK entries when they only advertise newly available
receive capacity. ACK packets are not themselves acknowledged. Keepalive is
periodic rather than reliably retransmitted. OPEN uses a retrying request and
idempotent response; FIN uses the same sequence and ACK mechanism as DATA. RESET
is best-effort, with peer timers bounding cleanup if it is lost.

## Connection identity and allocation

Connection IDs use a 16-bit space. Zero is reserved; values 1 through 65,535 are
available. The server allocates IDs globally from a FIFO of eligible free IDs.
Connection state also binds the ID to the peer's UDP address and port. Traffic
from another endpoint cannot affect that connection. This binding is
demultiplexing, not authentication.

Released IDs enter quarantine before returning to the FIFO. Quarantine must
cover the remaining peer retransmission horizon plus an allowance for delayed
packets. The default quarantine is 20 seconds. OPEN verifies matching connection,
delivery, keepalive, and receive inactivity timeouts, and configuration validation
requires quarantine to exceed those horizons. When no eligible IDs are available, new connections are
rejected rather than bypassing quarantine.

Only an explicit OPEN exchange creates a connection. DATA, ACK, and KEEPALIVE
cannot resurrect expired state. Duplicate OPEN requests reuse the existing response;
closed requests retain a rejection tombstone until their slot is reused.

No per-session generation or unique session nonce is used for established
connection identity. FIFO reuse, endpoint binding, and quarantine reduce stale
packet collisions but do not eliminate them. Very delayed packets, endpoint
reuse, and server restarts can still cause ambiguity. This limitation is accepted
for the local deployment model; the protocol must not claim unconditional
protection against stale packets.

## Sequence numbers

Each direction maintains its own wrapping `uint32` DATA sequence counter. Zero
is valid after wraparound. Both directions initially start at sequence 1.

Order is modular, not ordinary unsigned numeric order:

```go
func seqAfter(a, b uint32) bool {
    return int32(a-b) > 0
}
```

Every active window and valid comparison distance must be strictly less than
2^31 packets. A distance of exactly 2^31 is ambiguous and must not be used for
ordering. Equality is handled separately. These rules apply to DATA sequence
numbers and receive limits, including comparisons of delayed advertisements.
Raw `a > b` and numerical `max` are not sequence-order operations.

Packets must be validated against active connection state and permitted ranges
before affecting counters or ACK state. Modular comparison alone cannot establish
that an arbitrary old packet belongs to the current sequence cycle.

## Receiving and acknowledgment

Each reception of an accepted DATA packet queues its sequence number for ACK,
including duplicate receptions. If an earlier ACK was lost, retransmission of
the DATA therefore triggers another ACK. Multiple receptions of the same sequence
within one pending ACK batch may share an entry.

ACK scheduling does not need delivery history. The receive path separately
maintains the state needed to distinguish new data, accepted duplicates, and
invalid packets. Accepted duplicates are acknowledged again but never delivered
twice. Invalid packets and unaccepted packets outside the receive window are not
acknowledged as accepted. Already accepted duplicates behind the current window
are distinct from new packets outside the window.

An ACK means that the receiver has accepted responsibility for the bytes. It
does not mean that the named-pipe consumer or application processed them. Once
acknowledged, accepted bytes cannot silently be discarded while keeping the
connection successful.

Data is delivered in sequence order. Out-of-order data may occupy bounded receive
storage until preceding data arrives. ACK batching does not delay delivery of
data that is already in order.

### ACK batching

The first entry in an empty pending batch starts a flush deadline. Later arrivals
do not postpone that deadline. Flush when either `AckMaxDelay` expires or
`AckBatchSize` distinct entries accumulate, subject to the datagram size limit.
After flushing, the next entry starts a new batch.

For a 10 ms delay:

```text
0 ms   Receive DATA 10; queue ACK 10; deadline is 10 ms
3 ms   Receive DATA 11; queue ACK 11; deadline stays 10 ms
8 ms   Receive DATA 12; queue ACK 12; deadline stays 10 ms
10 ms  Send ACK entries [10, 11, 12]
```

A duplicate received after this flush queues a fresh ACK. Continuous incoming
traffic cannot indefinitely postpone acknowledgment.

## Sending and retransmission

Each direction has a bounded send window. New DATA may be sent when both the
local send-window allowance and the peer's receive limit permit it. Sending
does not wait for each individual ACK or for a retry scan.

Retain each unacknowledged packet for retransmission. A valid ACK removes the
corresponding outstanding packet. Duplicate ACK entries are harmless; entries
for unsent or unrelated packets do not change send state. Connection failure
releases remaining storage without treating unacknowledged bytes as delivered.

Record the most recent transmission time for each outstanding packet. A shared
scheduler can scan pending packets and resend only those whose retry interval
has elapsed. There is no requirement for a separate timer per packet.

```text
0 ms   Send DATA 10
25 ms  Send DATA 11
30 ms  Scan with a 30 ms retry interval
       DATA 10 is due; DATA 11 is not due
```

Update a packet's last-transmission timestamp when it is retransmitted. This
gives each packet a full acknowledgment opportunity following its own latest
transmission, even when other packets in the window are older.

### Backoff and failure

Increase the retry interval exponentially after unsuccessful retry rounds with
no new acknowledgment progress, up to a configured ceiling. Full-window status
alone does not trigger backoff: a full window can be normal, and a failed peer
can have only one outstanding packet. Duplicate ACKs do not count as progress.

A scheduler scan containing due retransmissions is one retry round. The next
retry delay doubles once per such round, capped by `MaxRetryInterval`. A newly
acknowledged packet resets the interval to its initial value and recalculates
outstanding retry deadlines from each packet's last transmission time. Backoff must not extend the fixed
delivery deadline. A packet's delivery age is measured from its original send,
not its last retransmission.

Delivery timeout fails the connection. Reconnecting and replaying application
bytes automatically is not permitted: the target may already have processed an
operation whose response was lost. Transport duplicate suppression is scoped
to the live connection, not a guarantee of exactly-once application execution
across reconnects.

## Receiver flow control

ACK packets include `ReceiveLimit`, the inclusive highest DATA sequence number
the peer currently permits the sender to transmit. ACK entries describe accepted
data; the receive limit promises capacity for future data. These are independent.

The limit advances in modular sequence order when receive capacity is freed by
application consumption or target writes. Mere packet reception does not free
capacity. Moving bytes to another internal queue does not create capacity unless
that queue is bounded and accounted for in the receive budget.

For a four-packet receive window starting with sequence 1:

```text
Advertise ReceiveLimit = 4             Sender may transmit packets 1 through 4
Accept packets 1 through 4             ACK them; ReceiveLimit remains 4
Application consumes packets 1 and 2   Advertise ReceiveLimit = 6
                                      Sender may transmit packets 5 and 6
```

In-order consumption advances the window's left edge. Partial consumption must
not release a whole packet slot prematurely. Missing early data and a blocked
consumer must not allow unbounded buffering through selective ACKs.

The OPEN exchange communicates initial receive limits in both directions.
Subsequent advertisements never retract promised capacity within a connection.
Delayed advertisements cannot shrink the sender's known limit: retain the latest
limit in modular sequence order. A new ACK does not grant capacity unless its
limit actually advances.

Application consumption can trigger a window-only ACK even without new DATA
receptions. Losing that update must not deadlock a sender that has no outstanding
DATA to retransmit. Keepalive must therefore carry the current receive limit or
trigger a response that carries it. Version 1 carries the limit directly in
KEEPALIVE and repeats capacity advertisements periodically even during continuous
outgoing DATA traffic, so that traffic cannot suppress recovery.

## Keepalive and expiration

Both endpoints send keepalive when they have sent no other traffic for the
configured keepalive interval. Version 1 additionally sends keepalive when the
current receive limit has not been advertised for that interval, even if outgoing
DATA traffic is active. Both expire a connection after the receive
inactivity timeout without valid incoming traffic for that connection. Sending
traffic does not refresh the sender's receive inactivity timer.

Initial values are a 2-second keepalive interval and an 8-second receive
inactivity timeout. Periodic keepalive does not require its own reliable retry
mechanism. It must still provide receive-window recovery as described above.

Receive inactivity and delivery timeout are distinct. Valid keepalives may keep
a connection alive while a specific unacknowledged DATA packet reaches its
delivery deadline. Either failure condition may terminate the connection first.
Windows sleep or a WSL pause may expire connections; subsequent use reports a
failure and requires reopening.

## Configuration

Use a typed Go configuration with defaults and validation. Avoid scattering
timing values and window sizes through the transport. The following values are
starting points for measurement unless marked as a protocol ceiling.

| Field | Initial value | Meaning |
| --- | --- | --- |
| `MaxConnections` | 65,535 | Configurable admission ceiling, never above the protocol maximum |
| `SendWindowPackets` | 64 | Maximum outstanding DATA packets per direction |
| `ReceiveWindowPackets` | 64 | Bounded receive capacity per direction |
| `MaxDatagramBytes` | 1,200 | Entire UDP payload, including protocol headers |
| `AckMaxDelay` | 10 ms | Maximum delay from first pending ACK |
| `AckBatchSize` | 32 | Distinct ACK entries that trigger an early flush |
| `InitialRetryInterval` | 30 ms | Initial retransmission delay |
| `RetryBackoffFactor` | 2 | Exponential backoff multiplier |
| `MaxRetryInterval` | 1 s | Retry delay ceiling |
| `ConnectTimeout` | 5 s | Proposed establishment deadline |
| `DeliveryTimeout` | 10 s | Proposed maximum age of an unacknowledged DATA packet |
| `KeepaliveInterval` | 2 s | Send keepalive after this period without outgoing traffic |
| `ReceiveIdleTimeout` | 8 s | Expire after this period without valid incoming traffic |
| `IDReuseDelay` | 20 s | Quarantine exceeding retry and inactivity horizons |
| `ScanInterval` | 1 ms | Shared scheduler cadence |
| `SocketWriteTimeout` | 1 s | Bound on a UDP socket write |
| `AcceptBacklog` | 128 | Maximum pending accepts |
| `MaxBufferedBytes` | 64 MiB | Reserved worst-case payload window budget per endpoint |

The earlier suggestion to disable idle expiry is superseded by the agreed
keepalive and 8-second receive inactivity design.

Validate positive capacities and intervals, ACK batch fit within a datagram,
sequence comparison bounds, retry interval ordering, and a keepalive interval
below receive inactivity timeout. Peer-visible limits and assumptions must be
communicated or made compatible during opening; local configuration alone
cannot impose a peer's retry horizon. Allocate connection buffers lazily.

## Implementation boundary

Wire layout, opening, EOF, backoff, keepalive advertisement, and resource admission
are implemented and specified in [wire-v1.md](wire-v1.md). Target names are passed
to the accepting application; the transport does not open Windows resources.

Authentication, authorization, encryption, and the server's bind-address policy
remain application work before exposing SSH-agent access. Endpoint binding does
not authenticate a caller. FIFO and quarantine retain the accepted stale-packet
limitation around extreme delays, reuse, and restarts.

The initial implementation serializes state per endpoint and reserves payload
budgets before admission. Scheduler scalability, Windows runtime interoperability,
and practical maximum connection counts still need measurement.

## Validation expectations

Use a controllable clock and simulated datagram delivery to exercise loss,
duplication, reordering, and bounded queues. Verify these behaviors directly:

- Lost ACK causes retransmission and re-ACK without duplicate delivery.
- Continuous reception cannot postpone an ACK beyond its batching deadline.
- A newer packet is not retried merely because an older packet is due.
- Backoff responds to missing ACK progress even with a partially filled window.
- Out-of-order traffic and blocked consumers stay within memory bounds.
- A lost window-only ACK is recovered through keepalive without deadlock.
- Sequence and receive-limit ordering work across uint32 wraparound.
- Duplicate ACKs and stale window advertisements cannot advance invalid state.
- Peer binding, FIFO allocation, quarantine, exhaustion, and expiration follow
  the connection rules without claiming to eliminate accepted stale ambiguity.
- Keepalive preserves a quiet connection; receive inactivity and fixed delivery
  deadlines still fail their respective cases.

Follow with Windows/WSL interoperability testing and the actual SSH-agent pipe.
