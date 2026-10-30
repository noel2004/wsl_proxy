// Package transport implements reliable bidirectional streams over UDP using
// only the Go standard library. Listen accepts streams; DialContext opens one
// for an opaque target name. Conn implements net.Conn and reliable ordered EOF
// through CloseWrite. Target resolution and authorization belong to the caller.
//
// The deterministic session state machine owns ordering, bounded flow control,
// ACK batching, retries, and timers. An endpoint serializes it and adapts it to
// sockets, concurrent I/O, and deadlines. See docs/protocol.md and docs/wire-v1.md.
// The protocol is unencrypted and unauthenticated, intended for controlled local
// use rather than untrusted network exposure.
package transport
