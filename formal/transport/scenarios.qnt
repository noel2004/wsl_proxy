// Larger instance for deterministic fault scenarios and randomized simulation.
module scenarios {
  import core(
    MaxData = 3, Window = 2, NetworkCapacity = 3, PacketLifetime = 2,
    DeliveryRounds = 3, IdleRounds = 3, IdleExpiry = true
  ).* from "./core"

  action deliver(kind: str, source: str, seq: int, duplicate: bool): bool = {
    nondet p = s.network.filter(p => p.kind == kind and p.source == source and p.seq == seq).oneOf()
    receive(p, duplicate)
  }

  action lose(kind: str, source: str): bool = {
    nondet p = s.network.filter(p => p.kind == kind and p.source == source).oneOf()
    drop(p)
  }

  run senderResetTest = init
    .then(close(Sender)).expect(oneClosed)
    .then(deliver("RESET", Sender, 0, false)).expect(bothClosed and safety)

  run receiverResetTest = init
    .then(close(Receiver)).expect(oneClosed)
    .then(deliver("RESET", Receiver, 0, false)).expect(bothClosed and safety)

  run lostSenderResetTest = init
    .then(close(Sender)).then(lose("RESET", Sender))
    .then(retry_round).then(retry_round).expect(oneClosed)
    .then(retry_round).expect(bothClosed and safety)

  run lostReceiverResetTest = init
    .then(close(Receiver)).then(lose("RESET", Receiver))
    .then(retry_round).then(retry_round).expect(oneClosed)
    .then(retry_round).expect(bothClosed and safety)

  // A duplicate consumed DATA repairs a lost ACK without delivering bytes twice.
  run duplicateAfterConsumptionTest = init.then(send_data)
    .then(deliver("DATA", Sender, 1, true)).then(consume)
    .then(lose("ACK", Receiver))
    .then(deliver("DATA", Sender, 1, false)).then(send_ack)
    .then(deliver("ACK", Receiver, 0, false))
    .expect(s.delivered == List(1) and s.pending == Map() and safety)

  run reorderingTest = init.then(send_data).then(send_data)
    .then(deliver("DATA", Sender, 2, false)).expect(s.recvNext == 1 and s.buffered == Set(2))
    .then(deliver("DATA", Sender, 1, false))
    .then(consume).then(consume)
    .expect(s.delivered == List(1, 2) and safety)

  // ACKs alone free the send window but cannot free the consumer's receive slots.
  // Losing a consumption advertisement is repaired by KEEPALIVE.
  run lostWindowUpdateTest = init.then(send_data).then(send_data)
    .then(deliver("DATA", Sender, 1, false))
    .then(deliver("DATA", Sender, 2, false)).then(send_ack)
    .then(deliver("ACK", Receiver, 0, false))
    .expect(s.nextSend > s.peerLimit).then(consume).then(lose("ACK", Receiver))
    .expect(s.nextSend > s.peerLimit).then(send_keepalive(Receiver))
    .then(deliver("KEEPALIVE", Receiver, 0, false)).then(send_data)
    .expect(s.nextSend == 4 and s.peerLimit == 3 and safety)

  // Valid traffic keeps idle at zero; resending still cannot extend delivery age.
  run deliveryDeadlineTest = init.then(send_data).then(lose("DATA", Sender))
    .then(retry_round).then(resend(1)).then(lose("DATA", Sender))
    .then(send_keepalive(Receiver)).then(deliver("KEEPALIVE", Receiver, 0, false))
    .then(send_keepalive(Sender)).then(deliver("KEEPALIVE", Sender, 0, false))
    .then(retry_round).then(resend(1)).then(lose("DATA", Sender))
    .then(send_keepalive(Receiver)).then(deliver("KEEPALIVE", Receiver, 0, false))
    .then(send_keepalive(Sender)).then(deliver("KEEPALIVE", Sender, 0, false))
    .expect(s.idle.get(Sender) == 0 and s.pending.get(1) == 2)
    .then(retry_round).expect(s.open == Set(Receiver) and safety)
    .then(retry_round).then(retry_round).expect(bothClosed and safety)

  // The closed peer's delayed KEEPALIVE remains valid. Duplicate deliveries do
  // not renew the datagram's lifetime, so they only postpone eventual cleanup.
  run delayedKeepaliveAfterCloseTest = init.then(send_keepalive(Receiver))
    .then(close(Receiver)).then(lose("RESET", Receiver)).then(retry_round)
    .then(deliver("KEEPALIVE", Receiver, 0, true))
    .then(deliver("KEEPALIVE", Receiver, 0, true))
    .expect(oneClosed and s.idle.get(Sender) == 0)
    .then(retry_round).expect(s.network == Set())
    .then(retry_round).expect(oneClosed)
    .then(retry_round).expect(bothClosed and safety)

  run sendingDoesNotRefreshIdleTest = init.then(close(Receiver))
    .then(lose("RESET", Receiver)).then(send_keepalive(Sender))
    .then(retry_round).then(send_keepalive(Sender))
    .then(retry_round).then(send_keepalive(Sender)).expect(oneClosed)
    .then(retry_round).expect(bothClosed and safety)

  run closedPeerCannotReopenTest = init.then(send_data).then(close(Receiver))
    .expect(not(s.open.contains(Receiver)))
    .then(deliver("RESET", Receiver, 0, false))
    .then(retry_round).expect(bothClosed and safety)
    .then(deliver("DATA", Sender, 1, false).fail())

  // These witness states prevent the liveness claim from being mistaken for a
  // requirement to close healthy connections. Keepalives can sustain both peers.
  run healthyIdleConnectionTest = init
    .then(5.reps(_ => retry_round
      .then(send_keepalive(Sender)).then(deliver("KEEPALIVE", Sender, 0, false))
      .then(send_keepalive(Receiver)).then(deliver("KEEPALIVE", Receiver, 0, false))))
    .expect(s.open == Peers and safety)
}
