// One established connection, with DATA flowing sender -> receiver.
// Both endpoints send control traffic. See README.md for the abstraction boundary.
module core {
  const MaxData: int
  const Window: int
  const NetworkCapacity: int
  const PacketLifetime: int
  const DeliveryRounds: int
  const IdleRounds: int
  const IdleExpiry: bool

  assume positiveBounds = and {
    MaxData > 0, Window > 0, NetworkCapacity > 0,
    PacketLifetime > 0, DeliveryRounds > 0, IdleRounds > 0,
  }

  pure val Sender = "sender"
  pure val Receiver = "receiver"
  pure val Peers = Set(Sender, Receiver)

  type Packet = {
    kind: str, source: str, seq: int, entries: Set[int], limit: int, life: int,
  }
  type State = {
    open: Set[str], idle: str -> int, keepaliveDue: Set[str],
    nextSend: int, pending: int -> int, retryDue: Set[int], peerLimit: int,
    recvNext: int, buffered: Set[int], ackPending: Set[int],
    accepted: Set[int], acknowledged: Set[int], delivered: List[int],
    network: Set[Packet], roundParity: bool,
  }
  var s: State

  pure def other(peer: str): str = if (peer == Sender) Receiver else Sender
  pure def packet(kind: str, source: str, seq: int, entries: Set[int], limit: int): Packet =
    { kind: kind, source: source, seq: seq, entries: entries, limit: limit, life: PacketLifetime }

  // A full network drops newly emitted datagrams, never blocks local cleanup.
  // Sets coalesce identical copies; receive(..., true) allows duplicate delivery
  // while preserving the original packet's remaining lifetime.
  pure def emit(st: State, p: Packet): State =
    if (st.network.size() < NetworkCapacity) { ...st, network: st.network.union(Set(p)) }
    else st

  pure def take(st: State, p: Packet, duplicate: bool): State =
    if (duplicate) st else { ...st, network: st.network.exclude(Set(p)) }

  // Retirement is irreversible. Buffered bytes can be discarded only on failure;
  // accepted/acknowledged/delivered are observation history, not live buffers.
  pure def retire(st: State, peer: str): State = {
    val closed = {
      ...st, open: st.open.exclude(Set(peer)),
      keepaliveDue: st.keepaliveDue.exclude(Set(peer)),
    }
    if (peer == Sender) {
      ...closed, pending: Map(), retryDue: Set(),
    } else {
      ...closed, buffered: Set(), ackPending: Set(),
    }
  }

  val receiveLimit = s.recvNext + Window - 1
  val bothClosed = s.open == Set()
  val oneClosed = s.open.size() == 1

  action init = s' = {
    open: Peers, idle: Map(Sender -> 0, Receiver -> 0), keepaliveDue: Peers,
    nextSend: 1, pending: Map(), retryDue: Set(), peerLimit: Window,
    recvNext: 1, buffered: Set(), ackPending: Set(),
    accepted: Set(), acknowledged: Set(), delivered: List(),
    network: Set(), roundParity: false,
  }

  action send_data = all {
    s.open.contains(Sender), s.nextSend <= MaxData,
    s.pending.keys().size() < Window, s.nextSend <= s.peerLimit,
    s' = emit({
      ...s, nextSend: s.nextSend + 1, pending: s.pending.put(s.nextSend, 0),
    }, packet("DATA", Sender, s.nextSend, Set(), 0)),
  }

  // Each packet gets at most one resend opportunity per round. Its delivery
  // age is measured from the original send and is NEVER reset by retransmission
  // or an ACK for another packet. Backoff duration is abstracted away.
  action resend(seq: int): bool = all {
    s.open.contains(Sender), s.retryDue.contains(seq),
    s' = emit({ ...s, retryDue: s.retryDue.exclude(Set(seq)) },
      packet("DATA", Sender, seq, Set(), 0)),
  }

  action receive_data(p: Packet, duplicate: bool): bool = all {
    s.network.contains(p), p.kind == "DATA", s.open.contains(Receiver),
    // Every emitted DATA is genuine. Old consumed duplicates are accepted again.
    p.seq < s.recvNext or p.seq <= receiveLimit,
    s' = {
      ...take(s, p, duplicate), idle: s.idle.set(Receiver, 0),
      buffered: if (p.seq < s.recvNext) s.buffered else s.buffered.union(Set(p.seq)),
      accepted: s.accepted.union(Set(p.seq)),
      ackPending: s.ackPending.union(Set(p.seq)),
    },
  }

  // Selective ACKs form a batch. A window-only ACK is also legal. Flush timing
  // is nondeterministic here; the fixed millisecond deadline is outside this model.
  action send_ack = all {
    s.open.contains(Receiver),
    s' = emit({ ...s, ackPending: Set() },
      packet("ACK", Receiver, 0, s.ackPending, receiveLimit)),
  }

  action receive_ack(p: Packet, duplicate: bool): bool = all {
    s.network.contains(p), p.kind == "ACK", s.open.contains(Sender),
    p.limit <= s.nextSend - 1 + Window,
    s' = {
      ...take(s, p, duplicate), idle: s.idle.set(Sender, 0),
      pending: s.pending.keys().exclude(p.entries).mapBy(seq => s.pending.get(seq)),
      retryDue: s.retryDue.exclude(p.entries),
      acknowledged: s.acknowledged.union(p.entries),
      peerLimit: if (p.limit > s.peerLimit) p.limit else s.peerLimit,
    },
  }

  // Consumption is optional (a blocked consumer is allowed). Only consumption
  // advances ReceiveLimit. Whole-packet consumption abstracts partial byte reads.
  action consume = all {
    s.open.contains(Receiver), s.buffered.contains(s.recvNext),
    s' = emit({
      ...s, recvNext: s.recvNext + 1, buffered: s.buffered.exclude(Set(s.recvNext)),
      delivered: s.delivered.append(s.recvNext), ackPending: Set(),
    }, packet("ACK", Receiver, 0, s.ackPending, receiveLimit + 1)),
  }

  action send_keepalive(peer: str): bool = all {
    s.open.contains(peer), s.keepaliveDue.contains(peer),
    s' = emit({ ...s, keepaliveDue: s.keepaliveDue.exclude(Set(peer)) },
      packet("KEEPALIVE", peer, 0, Set(), if (peer == Receiver) receiveLimit else Window)),
  }

  action receive_keepalive(p: Packet, duplicate: bool): bool = all {
    s.network.contains(p), p.kind == "KEEPALIVE", s.open.contains(other(p.source)),
    s' = {
      ...take(s, p, duplicate), idle: s.idle.set(other(p.source), 0),
      peerLimit: if (p.source == Receiver and p.limit > s.peerLimit) p.limit else s.peerLimit,
    },
  }

  // Close models Conn.Close (abortive RESET), not CloseWrite/FIN. In-flight
  // datagrams survive retirement and may still refresh the surviving peer.
  action close(peer: str): bool = all {
    s.open.contains(peer),
    s' = emit(retire(s, peer), packet("RESET", peer, 0, Set(), 0)),
  }

  action receive_reset(p: Packet): bool = all {
    s.network.contains(p), p.kind == "RESET", s.open.contains(other(p.source)),
    s' = retire(take(s, p, false), other(p.source)),
  }

  action drop(p: Packet): bool = all {
    s.network.contains(p), s' = take(s, p, false),
  }

  // One retry opportunity replaces elapsed wall time. It ages EVERY outstanding
  // packet even if no resend happens, and counts silence even with no DATA pending.
  // Expiry precedes retries, as in session.tick. Failures send no RESET, matching
  // endpoint.failLocked. Only valid receives reset idle; sending never does.
  action retry_round = {
    val aged = {
      ...s,
      idle: Peers.mapBy(peer =>
        if (s.open.contains(peer) and s.idle.get(peer) < IdleRounds)
          s.idle.get(peer) + 1 else s.idle.get(peer)),
      pending: s.pending.keys().mapBy(seq => s.pending.get(seq) + 1),
      retryDue: s.pending.keys(), keepaliveDue: s.open,
      network: s.network.filter(p => p.life > 1).map(p => { ...p, life: p.life - 1 }),
      roundParity: not(s.roundParity),
    }
    val senderExpired =
      (IdleExpiry and aged.idle.get(Sender) >= IdleRounds) or
      aged.pending.keys().exists(seq => aged.pending.get(seq) >= DeliveryRounds)
    val receiverExpired = IdleExpiry and aged.idle.get(Receiver) >= IdleRounds
    val afterSender = if (senderExpired) retire(aged, Sender) else aged
    s' = if (receiverExpired) retire(afterSender, Receiver) else afterSender
  }

  action receive(p: Packet, duplicate: bool): bool = any {
    receive_data(p, duplicate), receive_ack(p, duplicate),
    receive_keepalive(p, duplicate), receive_reset(p),
  }

  action step = any {
    send_data, send_ack, consume, retry_round,
    all { nondet seq = s.retryDue.oneOf(); resend(seq) },
    all { nondet peer = Peers.oneOf(); close(peer) },
    all { nondet peer = Peers.oneOf(); send_keepalive(peer) },
    all { nondet p = s.network.oneOf(); drop(p) },
    all {
      nondet p = s.network.oneOf()
      nondet duplicate = Bool.oneOf()
      receive(p, duplicate)
    },
  }

  // These checks support the lifecycle claim and catch accidental model drift.
  val safety = and {
    s.open.subseteq(Peers), s.keepaliveDue.subseteq(s.open),
    s.nextSend >= 1, s.nextSend <= MaxData + 1,
    s.recvNext >= 1, s.recvNext <= s.nextSend,
    s.peerLimit >= Window, s.peerLimit <= receiveLimit,
    s.pending.keys().size() <= Window, s.buffered.size() <= Window,
    s.retryDue.subseteq(s.pending.keys()),
    s.pending.keys().forall(seq => and {
      seq >= 1, seq < s.nextSend,
      s.pending.get(seq) >= 0, s.pending.get(seq) < DeliveryRounds,
    }),
    s.buffered.forall(seq => seq >= s.recvNext and seq <= receiveLimit),
    s.ackPending.subseteq(s.accepted), s.acknowledged.subseteq(s.accepted),
    s.accepted.subseteq(1.to(s.nextSend - 1)),
    s.delivered.length() == s.recvNext - 1,
    s.delivered.indices().forall(i => s.delivered.nth(i) == i + 1),
    s.delivered.indices().forall(i => s.accepted.contains(s.delivered.nth(i))),
    s.network.size() <= NetworkCapacity,
    s.network.forall(p => p.life >= 1 and p.life <= PacketLifetime),
    Peers.forall(peer => s.idle.get(peer) >= 0 and s.idle.get(peer) <= IdleRounds),
    not(s.open.contains(Sender)) implies (s.pending == Map() and s.retryDue == Set()),
    not(s.open.contains(Receiver)) implies (s.buffered == Set() and s.ackPending == Set()),
  }

  temporal no_reopen = and {
    always(not(s.open.contains(Sender)) implies always(not(s.open.contains(Sender)))),
    always(not(s.open.contains(Receiver)) implies always(not(s.open.contains(Receiver)))),
  }
  temporal no_linger = always(oneClosed implies eventually(bothClosed))

  // No fairness is required of delivery, RESET, application close, or consumption.
  // The timer worker must keep running. The parity bit makes every round visible
  // even when counters saturate (also useful in the deliberately broken model).
  temporal fair_no_linger = retry_round.weakFair(s) implies no_linger
}
