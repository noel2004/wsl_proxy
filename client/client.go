package client

import (
	"context"

	"github.com/noel2004/wsl_proxy/internal/transport"
)

// Config controls transport bounds and timing. Start with DefaultConfig; peers
// must agree on the wire-visible timing profile and maximum datagram size.
type Config = transport.Config

// Conn implements net.Conn, with CloseWrite for transport FIN. Close is abortive.
// Named-pipe targets do not support half-close: finish replies before closing.
type Conn = transport.Conn

// Sentinel errors support errors.Is without importing the internal transport.
// I/O deadlines also match os.ErrDeadlineExceeded; cancellation uses ctx.Err().
var (
	ErrRejected        = transport.ErrRejected
	ErrPeerClosed      = transport.ErrPeerClosed
	ErrWriteClosed     = transport.ErrWriteClosed
	ErrDeliveryTimeout = transport.ErrDeliveryTimeout
	ErrIdleTimeout     = transport.ErrIdleTimeout
	ErrConnectTimeout  = transport.ErrConnectTimeout
)

// DefaultConfig returns the transport profile used by the Windows CLI.
func DefaultConfig() Config { return transport.DefaultConfig() }

// DialContext opens one stream and owns its UDP socket until Close or failure.
// ctx bounds opening only; use stream deadlines or Close to cancel later I/O.
// target is a configured name (normally "ssh-agent"), never a Windows path.
func DialContext(ctx context.Context, address, target string, cfg Config) (*Conn, error) {
	return transport.DialContext(ctx, address, target, cfg)
}
