# Repository guidance

## Project intent

Build a Go Windows server and a Go Linux client library to proxy Windows-hosted
communication resources across WSL. The initial target is the Windows OpenSSH
agent named pipe. Targets are configured when the server starts. Application
wrappers around the library are a later layer.

Development normally runs in WSL. Cross-compile the Windows executable from Linux
and use WSL interop for Windows integration testing. Mirrored networking is the
baseline deployment assumption.

## Read before implementation

- [docs/protocol.md](docs/protocol.md) is the agreed transport behavior and the
  application integration boundary.
- [docs/wire-v1.md](docs/wire-v1.md) specifies the implemented packet format,
  lifecycle, and Go API.
- [docs/development.md](docs/development.md) describes planned package boundaries,
  build commands, and implementation milestones.
- [README.md](README.md) describes project scope and current implementation state.

The module is `github.com/noel2004/wsl_proxy`, requiring Go 1.26.0 with preferred
toolchain Go 1.26.10. `internal/transport` implements the UDP protocol, stream API,
and deterministic and socket-level tests. The public `client` wrapper, bounded
server core, Windows named-pipe adapter, and foreground Windows CLI are implemented.
Native service integration and Unix-socket application wrappers are deferred.
`docs/windows-ssh-agent.md` records actual Windows/WSL tests against 1Password.

## Preserve agreed decisions

- Implement a custom reliable UDP transport; do not substitute KCP, QUIC, or TCP
  without an explicit design change.
- Default the server to foreground execution with stdout/stderr logging. Native
  Windows Service support is an optional mode sharing the same server core.
- Keep the Linux interface a reusable Go library. Prefer a `net.Conn`-compatible
  stream API when settling its public interface.
- Use a server-wide ceiling of 65,535 logical connections, FIFO allocation of
  nonzero 16-bit IDs, peer UDP endpoint binding, and quarantine before ID reuse.
  Session generations are intentionally omitted. Preserve the documented stale
  packet limitation; do not silently add generations or claim reuse is infallible.
- Use separate wrapping uint32 DATA sequence spaces for each direction. Sequence
  ordering is modular and valid distances stay strictly below 2^31.
- ACK every accepted DATA reception, including duplicates; deliver bytes once.
  Batch ACKs with a fixed deadline measured from the first pending entry.
- Retry only packets whose own retry delay has elapsed. Back off on unsuccessful
  retry rounds without acknowledgment progress, regardless of window fullness.
- Enforce sender and receiver bounds. ACKs carry an inclusive `ReceiveLimit` that
  advances when consumer activity frees capacity, not merely on receipt.
- Recover lost receive-limit updates using keepalive. Start with 2-second
  keepalive and 8-second receive inactivity expiry; keep timing configurable.
- Do not automatically reconnect and replay application bytes after failure.

The protocol identifies exact values that remain starting proposals and details
that still need specification. Resolve those explicitly rather than treating a
suggestion as an agreed wire contract. Keep code and documentation synchronized.

## Implementation and validation

Write human-readable Go with clear names, small cohesive functions, and explicit
state transitions. Comment essential code blocks to explain how the algorithm
works and why its ordering or invariants matter; do not narrate obvious syntax.
Document essential functions and modules with their contract: inputs and outputs,
ownership, concurrency rules, errors, and any relevant wire-protocol behavior.
Keep these comments synchronized with implementation and protocol documentation.
Prefer the standard library. Add dependencies only when justified, preferring
`golang.org/x` packages over additional third-party libraries.

Keep protocol logic platform-independent and Windows adapters behind platform
build constraints. Prefer a typed configuration with validated defaults and
dependencies that do not require CGO for normal Windows builds. Bound per-connection
and aggregate memory, allocate buffers lazily, and propagate target failures.

For transport changes, use deterministic tests with a controllable clock and
simulated loss, duplicates, and reordering. Include wrapping sequence counters,
lost ACKs, lost window updates, blocked consumers, and lifecycle failure cases.
Use the validation expectations in the protocol as behavioral acceptance criteria.

Run `make check` for formatting, vet, tests, and host and Windows AMD64 package
builds. Use `make build-windows WINDOWS_ARCH=arm64` to check Windows ARM64. These
targets also cross-compile the executable. `make test-windows` executes Windows
tests through WSL interop. Use race detection when supported and appropriate. Run actual
Windows integration checks for named pipes and service behavior when available;
cross-compilation alone is not runtime verification. State any untested portion.

Documentation-only edits need link and consistency checks, not synthetic tests.
Do not create application stubs or unrelated features solely to make documentation
commands appear runnable.
