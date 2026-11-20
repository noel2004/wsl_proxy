// Negative control: retry exhaustion alone cannot retire an idle receiver.
module no_idle {
  import core(
    MaxData = 2, Window = 1, NetworkCapacity = 1, PacketLifetime = 1,
    DeliveryRounds = 2, IdleRounds = 2, IdleExpiry = false
  ).* from "./core"
}
