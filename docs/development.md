# Development guide

## Current state

The module is `github.com/noel2004/wsl_proxy`, requiring Go 1.26.0 and preferring
the Go 1.26.10 toolchain. The transport package implements reliable UDP streams
with deterministic state-machine tests and Linux UDP integration tests. The client
wrapper, Windows resource adapters, service integration, and server executable
remain unimplemented. There are no dependencies outside the standard library.

The behavioral contract is in [protocol.md](protocol.md). The implemented wire
encoding, lifecycle, and API are specified in [wire-v1.md](wire-v1.md).

## Planned code boundaries

| Location | Responsibility |
| --- | --- |
| `cmd/wsl-proxy-server/` | Windows entry point, flags, configuration, and mode selection |
| `client/` | Importable Go client library and connection API |
| `internal/transport/` | Platform-independent packet codec, connection state, windows, ACKs, retries, and scheduling |
| `internal/server/` | Target registry, admission, connection lifecycle, and forwarding |
| `internal/target/` | Windows resource adapters, beginning with named pipes |
| `internal/service/` | Optional Windows Service lifecycle and logging integration |

The transport package contains the implementation; other package directories
remain skeletons. The server command directory will be added with its entry point.
Keep the transport testable on Linux without a running Windows host. Use Windows
build constraints or `_windows.go` files for platform-specific implementation.
Prefer dependencies that preserve straightforward cross-compilation without
CGO. Do not introduce KCP or QUIC as the transport without revisiting the agreed
custom-stack decision.

The server's forwarding and protocol behavior must be identical in foreground
and service modes. A foreground server logs to stdout/stderr and handles terminal
shutdown. Service mode adapts Service Control Manager lifecycle and selects a
background logging destination. Service account selection affects access to
Windows resources and remains a deployment setting.

## Build and test workflow

Run the complete setup checks with:

```sh
make check
```

This checks formatting, runs vet and tests, and compiles current packages for the
host and Windows AMD64. Transport tests include simulated loss, duplication,
reordering, wraparound, backpressure, retries, and live UDP stream behavior.
Individual Go checks are:

```sh
go test ./...
go vet ./...
go build ./...
```

Use `make fmt` to format code, and `make build-windows WINDOWS_ARCH=arm64` to
cross-compile the packages for Windows ARM64. `GO` and `WINDOWS_ARCH` are
overridable Make variables.

Run transport tests with the race detector when the development toolchain supports
it (`go test -race ./...`). The packet decoder can be fuzzed with
`go test ./internal/transport -run=^$ -fuzz=FuzzDecode -fuzztime=10s`.
Use a simulated clock and datagram network for loss and timing cases instead
of wall-clock sleeps. Tests must verify observable ordering, reliability,
backpressure, and lifecycle behavior rather than mirror private helper logic.

Once the server command exists, build its Windows AMD64 executable with:

```sh
mkdir -p bin
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/wsl-proxy-server.exe ./cmd/wsl-proxy-server
```

Use `GOARCH=arm64` for an ARM64 Windows host. This executable build command is
for the future server entry point; current `make build-windows` only compiles
existing packages. Windows cross-compilation does not execute Windows tests.

Under WSL with interop enabled, run the built Windows executable from the Linux
shell for foreground integration testing. Test UDP communication in the actual
mirrored-network configuration before relying on localhost connectivity. Check
foreground operation under the launching user's Windows identity first, then
verify service mode under its configured identity.

## Implementation milestones

1. Completed: version 1 packet codec, OPEN/OPENED retries, FIFO ID quarantine,
   reliable FIN, best-effort RESET, and explicit receive limits.
2. Completed: configurable transport, deterministic fault simulation, UDP stream
   integration tests, and concurrency checks.
3. Implement a Linux client and Windows foreground server with a controlled
   stream target. Verify bidirectional traffic and failure propagation.
4. Add Windows named-pipe access and verify the OpenSSH agent use case. Confirm
   target permissions and the chosen authentication and binding policy.
5. Add optional native service integration around the same server core.
6. Build application wrappers on the client library, including a Unix socket
   bridge if desired.

Keep connection admission, buffer budgets, and target handles bounded. Treat the
65,535 connection ceiling as a design capacity until actual load testing
establishes practical limits.

## Completion criteria

A transport change is complete when relevant deterministic tests pass, Linux
library checks pass, and the Windows server cross-compiles. Changes to named-pipe
or service behavior additionally need actual Windows verification, or an explicit
record that this part could not be tested. Update the protocol whenever observable
wire behavior changes.
