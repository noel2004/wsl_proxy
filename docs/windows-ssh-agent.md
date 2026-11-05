# Windows SSH-agent proxy

## What the Windows target is

Microsoft's Windows OpenSSH agent listens on `\\.\pipe\openssh-ssh-agent`.
The implementation creates a duplex, byte-type named pipe and rejects remote
pipe clients. It identifies the Windows process connecting to that pipe. This is
a local IPC endpoint, not an SSH server on port 22 and not a filesystem file.
See Microsoft's [agent implementation](https://github.com/PowerShell/openssh-portable/blob/latestw_all/contrib/win32/win32compat/ssh-agent/agent.c).

1Password supports Microsoft OpenSSH clients on Windows through the agent
interface. In this development host, 1Password owns the same pipe while the
native `ssh-agent` service is disabled. Direct pipe tests and the complete proxy
test both succeeded against that agent. Service status alone therefore does not
indicate whether the pipe is available. See 1Password's
[compatibility documentation](https://developer.1password.com/docs/ssh/agent/compatibility/).

The proxy forwards bytes and does not add, remove, interpret, or store SSH keys.
The same adapter works with another local byte-type pipe selected at startup.
Message-type pipes, remote SMB paths, ordinary files, and other Windows devices
are outside this adapter's contract.

## Application protocol

The SSH-agent protocol is a client-driven request/reply protocol. Each message is
a four-byte big-endian length, followed by one byte of message type and its
payload. The length includes the type byte and excludes the four-byte length
itself. Replies preserve request order. Reads and writes can split or combine
frames; neither UDP packets nor pipe I/O calls define application boundaries.
See [RFC 9987, sections 3–5](https://www.rfc-editor.org/rfc/rfc9987.html#section-3).

A read-only probe sends the five bytes `00 00 00 01 0b`:
`SSH_AGENTC_REQUEST_IDENTITIES` (11), with no payload. The reply type is
`SSH_AGENT_IDENTITIES_ANSWER` (12), followed by a uint32 count and, for each key,
two length-prefixed SSH strings: public key blob and comment. Signing uses a
different request (13) and response (14). See
[RFC 9987, sections 5.5–5.6](https://www.rfc-editor.org/rfc/rfc9987.html#section-5.5).

The proxy imposes no SSH-specific message parser or message-size limit; its
transport and forwarding buffers remain bounded regardless of application frame
size. The test probe caps a single reply at 256 KiB before allocating it. Provider
extensions and refusals pass through unchanged.

1Password has its own authorization policy: private-key operations require
consent, and adding/removing keys through `ssh-add` is not its model. Requests
that are syntactically compatible can still be rejected by the provider. See
[1Password agent security](https://developer.1password.com/docs/ssh/agent/security/).

## Windows pipe implementation

`internal/target/pipe_windows.go` opens a fresh handle for each logical connection
using `CreateFileW` with read/write access, `OPEN_EXISTING`, and
`FILE_FLAG_OVERLAPPED`. Busy instances are retried with context-aware waits;
missing pipes and access errors fail immediately. `GetNamedPipeInfo` verifies
byte-type semantics. Microsoft's [named-pipe client reference](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-client)
describes these operations and the `ERROR_PIPE_BUSY` case.

Go 1.26's `os.NewFile` recognizes an overlapped handle and integrates it with the
runtime poller. The adapter checks deadline support before returning the stream;
closing it interrupts outstanding reads and writes. This avoids handwritten I/O
completion machinery. `golang.org/x/sys/windows` is the only added dependency:
it supplies typed Win32 calls and constants without CGO. See the
[Go 1.26 os.NewFile contract](https://pkg.go.dev/os@go1.26.0#NewFile).

The open requests `SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION`, permitting
identity inspection without granting pipe-based impersonation. This worked with
the installed 1Password agent. The native OpenSSH service was not exercised;
its source uses the connecting process's token. A provider requiring a different
impersonation level would need an explicit adapter decision. See Microsoft's
[CreateFileW security flags](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew).

Named pipes do not have TCP-style write-half-close. A client's transport FIN ends
the pipe exchange, so finish reading the response before closing. If the target
returns EOF first, the server queues transport FIN and keeps the connection alive
until the client closes or transport failure occurs, allowing queued reply bytes
to arrive. Any I/O error aborts the stream. Transport failure also cancels target
opening and blocked pipe operations. There is no reconnect or replay.

## Foreground command and bounds

Build and launch from WSL:

```sh
make build-windows
./bin/wsl-proxy-server-amd64.exe \
  -listen 127.0.0.1:55222 \
  -target ssh-agent \
  -ssh-agent
```

These flags belong to the Windows command. The client supplies the logical name
`ssh-agent`; it cannot select a Windows path. The core registry is immutable
after startup, and this first CLI configures one target. Use `-help` for options.

On Windows, `-ssh-agent` replaces the entire
`-pipe '\\.\pipe\openssh-ssh-agent'` argument. Explicit `-pipe` paths remain
supported; combining the two flags is rejected. Linux/macOS meanings of
`-ssh-agent` are not implemented. Omitting both retains the default agent pipe.

| Option | Default | Meaning |
| --- | --- | --- |
| `-listen` | `127.0.0.1:55222` | Literal loopback UDP IP and port; port 0 selects an available port |
| `-target` | `ssh-agent` | Logical name exposed to clients |
| `-pipe` | `\\.\pipe\openssh-ssh-agent` | Local Windows byte-pipe path |
| `-ssh-agent` | `false` | Windows shorthand for selecting the default agent pipe; mutually exclusive with `-pipe` |
| `-max-sessions` | `128` | Active target sessions, including opens in progress |
| `-open-timeout` | `5s` | Maximum wait to open a busy pipe |
| `-debug` | `false` | Log target-open and forwarding failures to stderr |

The stdout readiness line reports the bound address and configured target. It
does not assert the pipe exists: each connection opens it lazily. Normal logs go
to stderr. Ctrl+C cancels pending opens, closes the listener and target handles,
and joins forwarding workers. No Windows Service wrapper is implemented.

The transport retains its server-wide 65,535-ID ceiling, FIFO allocation,
quarantine, and 64 MiB default payload budget. The server separately admits up to
128 target sessions and allocates two 32 KiB forwarding buffers per active
session (at most 8 MiB by default). Pipe buffers, goroutine stacks, and transport
metadata are additional. Target-open failures, unknown names, or server overload
reset the stream; wire-v1 OPENED still confirms only transport admission.

## Access boundary

The initial CLI accepts only literal loopback binds (`127.0.0.1` or `::1`, for
example). The IPv4 default is the tested WSL-to-Windows path. No wildcard or LAN
bind escape hatch is provided. Mirrored networking is the deployment baseline;
the tests established IPv4 loopback reachability on this host, without inferring
the host's complete WSL networking configuration.

Loopback is a network scope, not user authentication. Every local process able to
reach this UDP endpoint can act through the proxy's Windows identity. The UDP
protocol has no authentication, encryption, or cryptographic packet integrity.
Use this version only on a trusted local host; client authorization remains a
prerequisite for broader deployment. Windows pipe ACLs authorize the proxy
process, not individual WSL callers.

1Password associates approvals with an application or terminal session. Because
this proxy opens the pipe, our inference is that provider attribution and approval
can group requests under the Windows proxy or its launching application; it
cannot distinguish Linux callers using UDP alone. Signing and prompt attribution
have not yet been tested. See the provider's
[authorization model](https://developer.1password.com/docs/ssh/agent/security/#authorization-model).

## Repeatable verification

```sh
make check
make build-windows WINDOWS_ARCH=arm64
go test -race ./...
make test-windows
```

`make test-windows` executes real Windows test binaries through WSL interop. It
uses isolated temporary pipe names and does not contact the installed agent by
default. Tests cover duplex transfers, deadlines, close cancellation, busy-pipe
timeouts, path validation, message-pipe rejection, custom CLI pipe/name selection,
shutdown, and the existing transport suite. The server's Linux tests cover
unknown targets, target-open failure, admission, response EOF, blocked consumers,
and disconnect/shutdown cleanup.

For the live agent, start the CLI in one WSL terminal, then run in another:

```sh
WSL_PROXY_TEST_ADDR=127.0.0.1:55222 \
  go test -v -count=1 ./client -run TestWindowsAgentInterop
```

Set `WSL_PROXY_TEST_TARGET` if `-target` was changed. The test sends two identities
requests over one stream, deliberately splits the request frames across writes,
and validates every response string's framing. Only the identity count is logged.
It does not sign, add, remove, or print keys. Stop the foreground CLI with Ctrl+C.

To isolate pipe access from UDP:

```sh
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
  go test -c -o bin/target.test.exe ./internal/target
./bin/target.test.exe -test.v -test.run TestAgentIdentities \
  -agent-pipe '\\.\pipe\openssh-ssh-agent'
```

Observed on 2026-11-05: Windows AMD64 (build 26300), WSL2, Go 1.26.10. Direct
Windows pipe access and Linux → UDP → Windows CLI → 1Password each returned two
valid replies with five identities. Ctrl+C stopped the real CLI successfully.
The native OpenSSH service was disabled and was not enabled; its runtime behavior
was not verified. Windows ARM64 is compile-only. Key signing, 1Password consent
prompts, real SSH login, IPv6 interop, and native Service Control Manager behavior
are not covered by these results.

## Troubleshooting

- A missing-pipe error means the configured provider is unavailable or its name
  differs. Check 1Password's SSH-agent setting first if it owns the pipe. Enabling
  the native agent simultaneously is not required.
- Access denied usually requires checking the launching Windows account and pipe
  ACL. Run the proxy as the normal desktop user associated with the agent.
- `-debug` shows target failures without logging application bytes. A successful
  UDP dial followed by reset can mean a missing target, open failure, or overload.
- A connect timeout can indicate that the Windows process is absent, loopback
  reachability differs from the assumed WSL configuration, or filtering blocks
  traffic. Do not widen the bind to a LAN interface to work around authentication.
- A sandbox may block WSL interop's vsock operations even when Windows executables
  are mounted. Run the integration checks in a normal WSL shell. Cross-compilation
  alone does not verify Windows behavior.
