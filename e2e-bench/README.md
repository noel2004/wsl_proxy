# End to end transport benchmark

This benchmark tests reliable UDP transmission with configurable packet loss and
Gaussian latency. The standard scenario transfers **4 MiB in random 4–16 KiB
segments**, waiting for each echoed segment to be received and verified before
sending the next. It logs transfer speed, ACK sizes, and DATA resend ratios.

Requires **Go 1.26 or newer**. All code uses the standard library and the local
transport package.

## Run

Run from this directory:

```sh
go test -v -count=1
```

Or from the repository root:

```sh
go test -v -count=1 ./e2e-bench
```

Ordinary `go test` is sufficient to execute the configured trial and verify the
echo. `-v` displays measurements; `-count=1` forces a new run instead of reusing a
cached test result. No separate server, Windows process, or network port is needed.

## What is exercised

```text
Client transport -> loss and latency filter -> server transport -> echo service
Client transport <- loss and latency filter <- server transport <- echo service
```

The two endpoints use the actual transport packet codec, OPEN handshake, windows,
ACKs, retries, keepalive, and FIN. An in-process `net.PacketConn` pair substitutes
for UDP sockets. Copies of datagrams are queued by scheduled delivery time, so
independent delays can reorder arrivals. This measures the complete Go transport
and its scheduling overhead, without kernel loopback, WSL, or Windows overhead;
it is not a measurement of actual localhost UDP performance.

The standard workload uploads exactly **4 MiB (4,194,304 bytes)**, split into
seeded random **4–16 KiB application segments**, and verifies the echo of each
segment before sending the next. The echo adds another 4 MiB in the reverse
direction. Segment sizes are independent of UDP datagram boundaries.

A constrained random partition keeps every segment within the requested range,
including the final segment, while preserving the exact total. A segment count
near `total / midpoint(min, max)` is chosen first; each draw preserves enough
capacity in the remaining slots. Sizes therefore are not independent uniform
draws, especially near the end. This workload uses 410 segments by default.

Writing and reading may overlap **within** a segment to avoid bounded-window
deadlocks. There is never a second application segment in flight. Every echoed
byte is checked, as is EOF after the expected payload. Corruption, extra bytes,
or failure to finish before the deadline causes the test to fail.

## Configuration

Edit [config.json](config.json), or select a separate file:

```sh
go test -v -count=1 ./e2e-bench -args -bench-config=/absolute/path/profile.json
```

Relative config paths are resolved from `e2e-bench`, the test working directory.
Unknown JSON fields, invalid ranges, and trailing JSON values are rejected. The
segment range must allow an exact partition of the total.

The standard configuration is:

```json
{
  "seed": 20261102,
  "packet_loss_percent": 2,
  "latency_mean_ms": 2,
  "latency_stddev_ms": 1,
  "timeout_seconds": 60,
  "queue_capacity_packets": 4096,
  "total_bytes": 4194304,
  "segment_min_bytes": 4096,
  "segment_max_bytes": 16384
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `seed` | 20261102 | Seed for independent segment-size and directional network PRNGs |
| `packet_loss_percent` | 2 | Independent drop probability per attempted datagram, from 0 to 100 |
| `latency_mean_ms` | 2 | Mean of the underlying one-way Gaussian delay, in milliseconds |
| `latency_stddev_ms` | 1 | Standard deviation of that Gaussian, in milliseconds |
| `total_bytes` | 4194304 | Exact uploaded payload volume; the echo returns the same volume |
| `segment_min_bytes` | 4096 | Minimum application segment size |
| `segment_max_bytes` | 16384 | Maximum application segment size, at most 64 MiB |
| `timeout_seconds` | 60 | Whole-trial deadline, including opening and transfer |
| `queue_capacity_packets` | 4096 | Maximum queued datagrams per receiving endpoint |

Loss and latency apply in both directions to **all datagrams**, including opening,
ACK, keepalive, FIN, and retransmissions. A dropped datagram still reports a
successful local WriteTo. Loss is probabilistic, so the observed fraction in a
short trial need not equal the configured percentage.

Delays are sampled as `max(0, mean + stddev * Normal(0, 1))`. Clamping negative
samples raises the effective mean when standard deviation is large relative to
the configured mean. Set standard deviation to zero for constant delay; set both
latency fields and loss to zero for an unimpaired baseline. Mean and standard
deviation must each be between 0 and 60,000 ms.

A bounded delivery heap prevents unlimited simulator buffering. Queue overflow
is additional loss reported separately from configured random loss. The seed
reproduces the filter's sequence of random decisions, but goroutine scheduling
changes packet timing, retry counts, and therefore the overall run. Wall-clock
performance is not deterministic.

The transport uses `transport.DefaultConfig()` on both ends. Its fixed delivery
and connection timeouts still apply; the trial timeout does not extend them.
High latency or loss can legitimately fail the test. A 100% loss profile should
fail to connect, not report successful throughput.

## Results and Go benchmarks

`TestE2E` reports:

- Verified payload bytes, segment count and observed size range, handshake time,
  and transfer time.
- Payload transfer speed: `total_bytes / transfer_time`, in MiB/s. This includes
  the wait for each echo and byte verification. Aggregate echo goodput counts
  both directions, so it is twice the payload speed. Transfer time includes
  segment generation, exchange, verification, and EOF, but not opening or cleanup.
- ACK packet count, total ACK bytes, mean ACK packet size, and ACK entry count,
  per direction and combined. Byte sizes include the 16-byte transport header,
  exclude UDP/IP headers, and count window-only ACKs and ACKs lost in transit.
- DATA attempts, DATA resends, and resend ratio:
  `100 * DATA_resends / DATA_attempts`. Each extra transmission of a DATA sequence
  counts as a resend, including repeatedly lost retries. OPEN, FIN, ACK, and
  KEEPALIVE are excluded from this ratio.
- Per-direction datagram totals, random/overflow/closed-endpoint drops, pending
  datagrams, attempted wire bytes, and peak queue occupancy.
- Mean scheduled latency for enqueued datagrams and mean observed latency at
  ReadFrom delivery, including scheduler delays.

Counters are collected at WriteTo before the fault filter and captured before
teardown. Some final ACKs can still be pending; pending packets are not classified
as lost. Wire bytes include headers and retries. The version-1 wire observer uses
per-direction, per-connection sequence ordering before network reordering to
identify DATA retries. Each trial creates a fresh network and connection, so IDs
are not reused within an observation period.

For Go's benchmark reporting, run:

```sh
go test -run='^$' -bench=BenchmarkE2E -benchtime=3x -benchmem ./e2e-bench
```

One benchmark operation runs the complete configured trial, including endpoint
setup, opening, verified transfer, EOF, and cleanup. Its MB/s counts uploaded
application bytes (4 MiB by default); it additionally reports handshake
milliseconds, datagrams and ACK bytes per operation, random loss percentage, and
DATA resend percentage. Setup and cleanup are included in `ns/op`, unlike
TestE2E's transfer-only goodput. Avoid interpreting the race detector's timings as
performance results.

### Example result

One measured run of the standard profile produced the following summary. These
values illustrate the log fields; they are not performance thresholds.

```text
verified 4194304 bytes (4.00 MiB) in 410 segments, sizes 4096..16373 bytes
handshake=4.411497ms transfer=6.517407101s
payload_speed=0.614 MiB/s aggregate_echo_goodput=1.227 MiB/s
TOTAL ACK packets=2051 bytes=63488 mean_size=30.95B entries=7668
DATA attempts=7809 resends=299 resend_ratio=3.829% (resends/DATA attempts)
```

## Client usage example

Read [bench_test.go](bench_test.go), starting at `runTrial`, `exchangeSegments`, and
`serveEcho`. The client sequence is:

1. Call `transport.DialPacket` with a PacketConn, destination, and target name.
2. Set an I/O deadline; the dial context governs opening only.
3. Generate one segment, send it, and drain its echo concurrently.
4. Verify the complete echo and wait for Write to finish before starting the next
   segment. Continue until exactly `total_bytes` have been verified.
5. Call `CloseWrite`, read EOF, then close the connection.

Writing everything before reading can deadlock a bounded echo stream. Also,
`Close` is abortive: the echo server stays alive after queuing FIN until the client
has read it. On failure the trial closes sockets and joins workers.

For real UDP, replace the simulated pair with `transport.Listen` and
`transport.DialContext`. This is an example of the current **transport client**;
the higher-level public `client` wrapper and Windows target adapters remain to be
implemented. `internal/transport` is importable only within this repository tree.

Simulator correctness tests use `testing/synctest` for virtual time, covering
delivery ordering, copy ownership, deadlines, closure, and failed-trial cleanup.
The configured benchmark itself always uses real time.

## Source files and correctness checks

| File | Responsibility |
| --- | --- |
| [config.json](config.json) | Standard workload and network settings |
| [config.go](config.go) | JSON loading and validation |
| [bench_test.go](bench_test.go) | Client example, echo service, test runner, and Go benchmark |
| [segments.go](segments.go) | Seeded random segment sizing with an exact total |
| [network.go](network.go) | In-process datagram delivery, loss, latency, and deadlines |
| [metrics.go](metrics.go) | Version-1 ACK and retransmission accounting |
| [network_test.go](network_test.go) | Simulator, distribution, configuration, and cancellation tests |
| [scenario_test.go](scenario_test.go) | Segment verification barrier, size bounds, and metric tests |

From the repository root, run the benchmark and simulator tests with race
detection:

```sh
go test -race -count=1 ./e2e-bench
```

Protocol behavior and packet layout are documented in
[protocol.md](../docs/protocol.md) and [wire-v1.md](../docs/wire-v1.md).
