package transport

import "errors"

var (
	// ErrRejected means opening was refused (capacity, profile, or expired request).
	ErrRejected = errors.New("transport: opening rejected")
	// ErrPeerClosed means the peer aborted the connection rather than sending EOF.
	ErrPeerClosed = errors.New("transport: peer reset connection")
	// ErrWriteClosed means CloseWrite has already queued the final sequence number.
	ErrWriteClosed = errors.New("transport: write direction is closed")
	// ErrDeliveryTimeout means accepted local bytes could not be acknowledged in time.
	ErrDeliveryTimeout error = timeoutError("transport: delivery timeout")
	// ErrIdleTimeout means no valid peer traffic arrived before the inactivity limit.
	ErrIdleTimeout error = timeoutError("transport: receive inactivity timeout")
	// ErrConnectTimeout means the opening exchange did not finish before its deadline.
	ErrConnectTimeout error = timeoutError("transport: connection timeout")
)

type timeoutError string

func (e timeoutError) Error() string   { return string(e) }
func (e timeoutError) Timeout() bool   { return true }
func (e timeoutError) Temporary() bool { return false }
