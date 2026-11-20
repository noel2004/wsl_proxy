# Transport lifecycle model

This is a finite abstraction of one established connection in
[wire v1](../../docs/wire-v1.md), with one DATA sender and one DATA receiver.
Both endpoints can send KEEPALIVE and RESET. The source is
[core.qnt](core.qnt); syntax and temporal operators follow the
[Quint language manual](https://quint.sh/docs/lang).

## What “no lingering peer” means

Temporary disagreement is expected: `Conn.Close` retires local state and sends
one best-effort RESET. If that datagram is lost, the peer needs time to expire.
The checked property is therefore:

```quint
temporal no_linger = always(oneClosed implies eventually(bothClosed))
temporal fair_no_linger = retry_round.weakFair(s) implies no_linger
```

`open` means a live connection, not a writable stream direction. FIN and
`CloseWrite` deliberately leave the connection alive and are outside this first
model. The property neither requires simultaneous closure nor forces a healthy
connection to close. `no_reopen` separately checks that retirement is permanent.

Two environmental assumptions matter:

1. **Scheduler progress.** Weak fairness of `retry_round` requires the timer worker
   to continue running. Unrestricted scheduling could otherwise stop it forever
   after one side closes. Packet delivery, application consumption, application
   close, and RESET delivery have no fairness assumptions; every packet may be lost.
2. **Finite stale traffic.** Each in-flight datagram has a bounded remaining
   lifetime in retry rounds. Delivering a duplicate never renews that lifetime.
   This models eventual disappearance of old traffic, not a wire TTL field or an
   enforced UDP lifetime. Endless replay of valid old packets could indefinitely
   refresh receive liveness in the actual implementation. ID reuse and the
   documented absence of session generations are outside the model.

Once one endpoint closes, it sends no further packets. Previously emitted
packets can still reset the survivor's idle counter until their lifetimes expire.
After the last such reception, `IdleRounds` scheduler rounds close the survivor,
even when it has no DATA pending. A conservative bound is
`PacketLifetime + IdleRounds` rounds after the first closure. This reasoning
explains the TLC result; a bound in seconds is not checked.

## State and actions

The single record `s` holds both endpoints and the unreliable network. Actions
update only local state and emitted/received datagrams; they do not inspect the
remote endpoint's openness to decide when to expire.

| State | Meaning |
| --- | --- |
| `open`, `idle`, `keepaliveDue` | Per-endpoint lifecycle, silent-round counts, periodic advertisement eligibility |
| `nextSend`, `pending`, `retryDue`, `peerLimit` | DATA sequence allocation, original-send age per unacknowledged packet, resend eligibility, inclusive receive allowance |
| `recvNext`, `buffered`, `ackPending` | Next unconsumed packet, bounded receive storage, selective ACK batch |
| `accepted`, `acknowledged`, `delivered` | Observation history used by safety checks; retained after abort |
| `network` | Bounded unordered set of in-flight datagrams with remaining lifetimes |
| `roundParity` | Toggles every scheduler round so fairness can observe progress even with saturated counters |

`send_data` allocates a sequence only within both windows. `receive_data` accepts
valid new packets and duplicates, queues ACKs for both, and stores new bytes once.
`send_ack` flushes a selective batch or a window-only update; `receive_ack`
removes acknowledged outstanding packets and never decreases the peer limit.
`consume` releases a whole packet slot in order and emits a capacity advertisement.
A blocked consumer can remain blocked indefinitely.

`retry_round` represents elapsed retry opportunities, without a wall clock.
One global round advances both peers; independent timer scheduling granularity
is not modeled.
It increments all outstanding delivery ages and all open endpoints' silence
counters, expires old network packets, and retires expired connections before
enabling resends. `resend` can retransmit each due packet once per round and
does not reset its original-send age. ACK progress for a different packet also
does not reset that age. These counters abstract Go's fixed delivery and receive
inactivity deadlines; they do not introduce a retry-count setting in the Go API.

`send_keepalive` becomes eligible every round independently of DATA traffic.
Only valid incoming DATA, ACK, or KEEPALIVE resets a silence counter. `close`
retires either endpoint and emits RESET; `receive_reset` retires its destination.
Timeouts retire without RESET, matching the current `endpoint.failLocked` path.
Retirement clears live pending/buffered state but leaves in-flight datagrams alone.

The network may lose any packet, deliver any available packet out of order, or
deliver a duplicate while retaining the packet. A full network drops newly
emitted packets; it never prevents local closure. Identical copies coalesce in
the set, and duplicate reception retains the original lifetime.

## Implementation correspondence and limits

| Model | Go implementation |
| --- | --- |
| DATA/window admission | `session.canSend`, `session.queue` |
| DATA/ACK/KEEPALIVE reception | `session.input` |
| ACK flush and consumption | `session.flushACK`, `session.read` |
| Retry age, idle expiry, keepalive opportunities | `session.tick` |
| Abort and retirement | `Conn.Close`, `endpoint.dispatchLocked`, `endpoint.failLocked` |

The model starts after OPEN and permits only genuine, well-formed packets in
one connection. Sequence IDs are finite, non-wrapping integers. Payload bytes,
partial reads, reverse DATA, FIN, OPEN, peer binding, ID allocation/reuse,
authentication, socket failures, and Windows resources are not modeled.
Millisecond ACK deadlines, per-packet retry timestamps, and exponential backoff
are abstracted by nondeterministic flush/resend actions and retry rounds.
This does not verify those timing algorithms, uint32 wraparound, or the full
bidirectional Go implementation. Existing deterministic Go tests still cover
those implementation details. There is no automatic reconnect or replay action.

## First verification

Run the commands in [formal/README.md](../README.md). Results on 2026-11-20:

| Check | Result |
| --- | --- |
| Type/effect checking | All three configured instances passed |
| Scenario tests | 12 passed |
| Seeded safety simulation | 1,000 traces of up to 100 steps passed; seed `20261120` |
| TLC: `safety`, `fair_no_linger`, `no_reopen` | Passed; 10,463 distinct states, 60,979 generated states, complete search depth 19 |
| No idle expiry, retaining scheduler fairness | Expected temporal counterexample |
| No scheduler fairness, retaining idle expiry | Expected temporal counterexample |

The exhaustive instance [transport.qnt](transport.qnt) uses two DATA packets,
one-packet send/receive windows, one in-flight datagram, one round of packet
lifetime, and two-round delivery/idle expiry. TLC exhausts this finite graph;
it does not prove the claim for every possible parameter value.

[scenarios.qnt](scenarios.qnt) uses three DATA packets, two-packet windows,
three in-flight datagrams, a two-round packet lifetime, and three-round deadlines.
Its larger network exercises reordering and delayed duplicates, but this instance
is tested/simulated rather than exhaustively verified. Scenarios cover RESET
delivery/loss in both directions, ACK loss, duplicate consumption suppression,
blocked consumers, lost window updates, fixed delivery age despite keepalives,
delayed keepalives after closure, outgoing traffic not refreshing local liveness,
irreversible closure, and a healthy idle connection.

The [no_idle.qnt](no_idle.qnt) counterexample has an empty send queue: the receiver
closes, its RESET expires undelivered, and the sender's timer rounds continue
forever without retiring it. The second counterexample stops scheduling expiry
after one endpoint closes. These checks demonstrate why the two versions of the
property differ and why retransmission exhaustion alone cannot prevent lingering.

This result supports the existing receive-inactivity design. It does not identify
a Go defect, and no production transport behavior was changed.
