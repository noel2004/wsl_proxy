# WSL proxy

A Go project for accessing Windows-hosted communication resources from Linux
under WSL. The first use case is forwarding a byte stream to the Windows OpenSSH
agent named pipe.

The custom UDP transport is implemented in `internal/transport`, with a
`net.Conn` stream API, deterministic fault-injection tests, and UDP socket tests.
Windows resource adapters, the public client wrapper, and the server executable
are the next layers to implement.

## Development setup

The module path is `github.com/noel2004/wsl_proxy`. Go 1.26.0 or newer is required;
the preferred development toolchain is Go 1.26.10.

```sh
go version
make check
```

`make check` verifies formatting, runs vet and tests, and builds the current
packages for the host and Windows AMD64. Transport tests cover reliability,
flow control, timers, and stream lifecycle; no server executable exists yet. Use
`make build-windows WINDOWS_ARCH=arm64` for a Windows ARM64 compile check.

## Architecture

```text
Linux application
    |
Go client library
    |
Custom reliable UDP transport
    |
Windows server
    |
Startup-configured Windows target
```

The Windows server runs in the foreground by default, logging to stdout/stderr.
An optional native Windows Service mode will use the same server core with
different lifecycle and logging integration. Foreground mode also permits use
with third-party service wrappers.

The Linux side is a Go library first. Application wrappers, such as a Unix socket
bridge for ordinary SSH clients, come later. The intended connection abstraction
is an ordered bidirectional byte stream; a `net.Conn`-compatible public API is the
starting proposal, with exact API details still to be settled.

Available targets are fixed by server startup configuration. Clients select
among those targets rather than asking the server to open arbitrary host paths.
Each logical connection uses its own target handle.

Development takes place on Linux, normally WSL. Windows executables are
cross-compiled and can be launched through WSL interop. Mirrored networking is
the baseline network environment.

## Transport decisions

The project implements its own UDP transport. It uses independent sequence
spaces in both directions, fixed maximum window sizes, selective batched ACKs,
per-packet retry eligibility, exponential backoff on missing ACK progress, and
receiver-advertised capacity. Sequence counters wrap at uint32 boundaries.

The server supports an admission ceiling of 65,535 logical connections, using
FIFO ID allocation and quarantine before reuse. Keepalive maintains quiet
connections; initial timing is 2 seconds for keepalive and 8 seconds for receive
inactivity expiry. Session generations are intentionally omitted, with the
remaining stale-packet ambiguity documented in the protocol.

## Documentation

- [Transport protocol](docs/protocol.md): agreed behavior, initial configuration,
  implementation boundaries, and validation expectations.
- [Wire version 1](docs/wire-v1.md): packet encoding, lifecycle, configuration,
  Go API, and implementation limits.
- [Development guide](docs/development.md): planned package boundaries, build
  workflow, and implementation milestones.
- [Agent guidance](AGENTS.md): repository instructions for future development.

Start with the protocol before changing transport behavior. The next milestone
is connecting the transport to the client library and a Windows foreground server
with a controlled stream target, followed by named-pipe integration.
