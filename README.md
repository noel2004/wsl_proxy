# WSL proxy

A Go project for accessing Windows-hosted communication resources from Linux
under WSL. The first use case is forwarding a byte stream to the Windows OpenSSH
agent named pipe.

The foreground Windows CLI proxies a configured local named pipe over the custom
reliable UDP transport. Its default pipe works with Windows OpenSSH and 1Password's
SSH agent. The `client` package provides a reusable `net.Conn`-compatible Go API.
Native Windows Service integration and a Linux Unix-socket wrapper are deferred.

## Run the Windows server from WSL

```sh
make build-windows
./bin/wsl-proxy-server-amd64.exe -ssh-agent
```

The default UDP address is `127.0.0.1:55222`, and the logical target name is
`ssh-agent`. Use `-listen`, `-target`, and `-pipe` to change them; `-help` lists
all options. On Windows, `-ssh-agent` is shorthand for
`-pipe '\\.\pipe\openssh-ssh-agent'`; use one or the other. The command and
these flags exist only in Windows builds; Linux/macOS aliases are not implemented.
Ctrl+C stops the foreground process and closes its active connections.

Only loopback binds are accepted. The transport has no authentication or
encryption: local clients can access the agent through the proxy's Windows
identity. Use this initial version only on a trusted local host. For 1Password,
enable its SSH agent; a stopped native Windows `ssh-agent` service is expected
when 1Password owns the pipe. See [Windows SSH-agent findings](docs/windows-ssh-agent.md)
for protocol details, access implications, and troubleshooting.

Connect from Go using the logical name, without a Windows path:

```go
conn, err := client.DialContext(ctx, "127.0.0.1:55222", "ssh-agent", client.DefaultConfig())
if err != nil {
    return err
}
defer conn.Close()
// Send and receive ordinary SSH-agent protocol frames on conn.
```

Import `github.com/noel2004/wsl_proxy/client`. A successful dial confirms the
transport; target-open failures are reported by stream I/O. Ordinary Linux
`ssh`/`ssh-add` still need a future Unix-socket wrapper; `SSH_AUTH_SOCK` cannot point
at a UDP address.

## Development setup

The module path is `github.com/noel2004/wsl_proxy`. Go 1.26.0 or newer is required;
the preferred development toolchain is Go 1.26.10.

```sh
go version
make check
```

`make check` verifies formatting, runs vet and tests, and builds the current
packages for the host and Windows AMD64. Transport tests cover reliability,
flow control, timers, and stream lifecycle. Use
`make build-windows WINDOWS_ARCH=arm64` for a Windows ARM64 compile check.

Run Windows tests through WSL interop with `make test-windows`. With the foreground
server running in another terminal, verify the live Windows agent from Linux:

```sh
WSL_PROXY_TEST_ADDR=127.0.0.1:55222 go test -v -count=1 ./client -run TestWindowsAgentInterop
```

This opt-in check requests public identities twice and logs only their count.

Run the configurable in-process end-to-end benchmark and client usage example:

```sh
go test -v -count=1 ./e2e-bench
```

[e2e-bench/config.json](e2e-bench/config.json) controls packet loss, Gaussian
latency, segment sizes, and total payload volume. The standard workload verifies
4 MiB in sequential random 4–16 KiB segments and logs speed, ACK sizes, and
DATA resend ratios. See the [benchmark guide](e2e-bench/README.md)
for metrics, custom profiles, and Go benchmark mode. Select the `standard`,
`throttle`, `network`, or `limited` preset with `-args -bench-scene=network`
or `E2E_BENCH_SCENE=network`.

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
bridge for ordinary SSH clients, come later. The connection abstraction is an
ordered bidirectional byte stream implementing `net.Conn`. `client.Config`,
`client.Conn`, `client.DefaultConfig`, and `client.DialContext` expose the transport API.

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
- [Windows SSH-agent findings](docs/windows-ssh-agent.md): named-pipe behavior,
  OpenSSH/1Password compatibility, CLI configuration, and Windows/WSL results.
- [Formal models](formal/README.md): Quint transport lifecycle model, exhaustive
  eventual-closure check, assumptions, and counterexamples.
- [Agent guidance](AGENTS.md): repository instructions for future development.

Start with the protocol before changing transport behavior. The next layers are
client authorization and an application wrapper, such as a Linux Unix socket.
