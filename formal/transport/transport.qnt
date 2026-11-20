// Small finite instance for exhaustive TLC exploration, including two DATA IDs.
module transport {
  import core(
    MaxData = 2, Window = 1, NetworkCapacity = 1, PacketLifetime = 1,
    DeliveryRounds = 2, IdleRounds = 2, IdleExpiry = true
  ).* from "./core"
}
