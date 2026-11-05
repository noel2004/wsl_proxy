# Development guide

## Current state

The module is `github.com/noel2004/wsl_proxy`, requiring Go 1.26.0 and preferring
the Go 1.26.10 toolchain. The transport package implements reliable UDP streams
with deterministic state-machine tests and Linux/Windows UDP integration tests.
The public client wrapper, bounded forwarding core, Windows named-pipe adapter,
and foreground Windows CLI are implemented. Native service integration and a
Unix-socket application wrapper remain deferred. `golang.org/x/sys/windows`
provides typed Win32 pipe calls; builds do not require CGO.

The behavioral contract is in [protocol.md](protocol.md). The implemented wire
encoding, lifecycle, and API are specified in [wire-v1.md](wire-v1.md).

## Code boundaries

| Location | Responsibility |
| --- | --- |
| `cmd/wsl-proxy-server/` | Windows foreground entry point, flags, and configuration |
| `client/` | Importable Go client library and connection API |
| `internal/transport/` | Platform-independent packet codec, connection state, windows, ACKs, retries, and scheduling |
| `internal/server/` | Target registry, admission, connection lifecycle, and forwarding |
| `internal/target/` | Windows resource adapters, beginning with named pipes |
| `internal/service/` | Optional Windows Service lifecycle and logging integration |
| `internal/testpipe/` | Isolated Windows named-pipe fixtures for runtime tests |
| `e2e-bench/` | Configurable in-process datagram network, end-to-end benchmark, and transport client usage example |

Only `internal/service` remains a reserved package; no service wrapper is implemented.
Keep the transport testable on Linux without a running Windows host. Use Windows
build constraints or `_windows.go` files for platform-specific implementation.
Prefer dependencies that preserve straightforward cross-compilation without
CGO. Do not introduce KCP or QUIC as the transport without revisiting the agreed
custom-stack decision.

The foreground server logs to stdout/stderr and handles terminal shutdown. Any
future Service Control Manager wrapper must share the forwarding core. Service
account selection would affect resource access and remains a deployment setting.

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

`go test -v -count=1 ./e2e-bench` runs the full transport over an in-process
datagram pair with configurable random loss and Gaussian latency. Edit
`e2e-bench/config.json`; see the [benchmark guide](../e2e-bench/README.md) for
alternate profiles and `-bench` mode. This is a real-time performance trial;
its simulator correctness tests use virtual time through `testing/synctest`.

Use `make fmt` to format code, including files behind Windows build constraints,
and `make build-windows WINDOWS_ARCH=arm64` to cross-compile the packages and
executable for Windows ARM64. `GO` and `WINDOWS_ARCH` are
overridable Make variables.

Run transport tests with the race detector when the development toolchain supports
it (`go test -race ./...`). The packet decoder can be fuzzed with
`go test ./internal/transport -run=^$ -fuzz=FuzzDecode -fuzztime=10s`.
Use a simulated clock and datagram network for loss and timing cases instead
of wall-clock sleeps. Tests must verify observable ordering, reliability,
backpressure, and lifecycle behavior rather than mirror private helper logic.

Build the Windows AMD64 executable with:

```sh
make build-windows
./bin/wsl-proxy-server-amd64.exe -ssh-agent
```

Use `WINDOWS_ARCH=arm64` for an ARM64 Windows host; the artifact is then
`bin/wsl-proxy-server-arm64.exe`. Windows cross-compilation does not execute tests.

Under WSL with interop enabled, run the built Windows executable from the Linux
shell for foreground integration testing. Test UDP communication in the actual
mirrored-network configuration before relying on localhost connectivity. Check
foreground operation under the launching user's Windows identity.

Run `make test-windows` from WSL to execute the Windows test suite through
interop. It creates isolated named-pipe fixtures and skips the real agent unless
explicitly selected. With the Windows CLI running, use
`WSL_PROXY_TEST_ADDR=127.0.0.1:55222 go test -v -count=1 ./client -run TestWindowsAgentInterop`
to request public identities through the full Linux/Windows path. See
[Windows SSH-agent findings](windows-ssh-agent.md) for exact scope and results.

## Implementation milestones

1. Completed: version 1 packet codec, OPEN/OPENED retries, FIFO ID quarantine,
   reliable FIN, best-effort RESET, and explicit receive limits.
2. Completed: configurable transport, deterministic fault simulation, UDP stream
   integration tests, and concurrency checks.
3. Completed: public client library, Windows foreground server, fixed target
   registry, bounded forwarding, and failure propagation.
4. Completed for local development: Windows byte-pipe adapter, custom target
   arguments, Windows runtime tests, and actual WSL-to-1Password identity queries.
   Only loopback binds are accepted. Authentication and broader deployment remain
   future work; native OpenSSH runtime/signing still need verification.
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
