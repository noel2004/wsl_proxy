package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/noel2004/wsl_proxy/internal/transport"
)

// OpenFunc opens one independent, concurrent read/write target stream. It must
// honor ctx during opening. Ownership passes to the server on success; Close
// must unblock both Read and Write. The context does not govern subsequent I/O.
type OpenFunc func(ctx context.Context) (io.ReadWriteCloser, error)

// Config fixes the target registry and resource bounds for a server. Start with
// DefaultConfig, then supply Targets. Openers may run concurrently. Logger must
// be safe for concurrent use; nil uses slog.Default. No application bytes are logged.
type Config struct {
	Targets     map[string]OpenFunc
	MaxSessions int
	OpenTimeout time.Duration
	Logger      *slog.Logger
}

// DefaultConfig bounds forwarding buffers to 8 MiB across 128 active
// sessions. Transport windows and operating-system pipe buffers are additional.
func DefaultConfig() Config {
	return Config{MaxSessions: 128, OpenTimeout: 5 * time.Second}
}

// Server owns a snapshot of its configuration. Each Serve call owns its listener
// and active targets until shutdown; create a single listener for the global ID pool.
type Server struct{ cfg Config }

// New validates configuration and copies the registry, so later map changes
// cannot change the resources exposed by this server.
func New(cfg Config) (*Server, error) {
	if cfg.MaxSessions < 1 || cfg.MaxSessions > 65535 || cfg.OpenTimeout <= 0 {
		return nil, errors.New("server: invalid session limit or open timeout")
	}
	if len(cfg.Targets) == 0 {
		return nil, errors.New("server: no targets configured")
	}
	targets := make(map[string]OpenFunc, len(cfg.Targets))
	for name, open := range cfg.Targets {
		if name == "" || open == nil {
			return nil, errors.New("server: invalid target")
		}
		targets[name] = open
	}
	cfg.Targets = targets
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{cfg: cfg}, nil
}

// Serve accepts until cancellation or listener failure, then closes the listener,
// cancels pending opens, interrupts forwarding and joins every worker. Context
// cancellation is a normal shutdown and returns nil. OPENED confirms transport
// acceptance only: unknown targets, overload and target failures send RESET.
func (s *Server) Serve(ctx context.Context, listener *transport.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); listener.Close(); workers.Wait() }()
	slots := make(chan struct{}, s.cfg.MaxSessions)
	for {
		conn, err := listener.AcceptContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		open, ok := s.cfg.Targets[conn.Target()]
		if !ok {
			conn.Close()
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			defer conn.Close()
			err := s.serveConnection(ctx, conn, open)
			if err != nil && ctx.Err() == nil {
				s.cfg.Logger.Debug("target connection ended", "target", conn.Target(), "peer", conn.RemoteAddr(), "error", err)
			}
		}()
	}
}

// serveConnection ties target lifetime to transport failure, including a peer
// disappearing while the pipe reader and writer are both blocked. Joining the
// watcher prevents shutdown from leaving per-connection goroutines behind.
func (s *Server) serveConnection(ctx context.Context, conn *transport.Conn, open OpenFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case <-conn.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); <-watched }()
	openCtx, stop := context.WithTimeout(ctx, s.cfg.OpenTimeout)
	target, err := open(openCtx)
	stop()
	if err != nil {
		return fmt.Errorf("open target: %w", err)
	}
	return forward(ctx, conn, target)
}

// forward keeps only two 32 KiB copy buffers beyond the transport windows.
// Named pipes have no write-half-close: client FIN ends the entire exchange.
// Target EOF queues reliable FIN and leaves the stream alive for the client to
// consume pending output and close. Never abort immediately after queuing FIN.
func forward(ctx context.Context, conn *transport.Conn, target io.ReadWriteCloser) error {
	var closeOnce sync.Once
	closeBoth := func() { closeOnce.Do(func() { conn.Close(); target.Close() }) }
	defer closeBoth()
	stop := context.AfterFunc(ctx, closeBoth)
	defer stop()
	type result struct {
		toClient bool
		err      error
	}
	done := make(chan result, 2)
	copyStream := func(dst io.Writer, src io.Reader, toClient bool) {
		// Hide ReaderFrom/WriterTo so adapters cannot bypass our buffer bound.
		_, err := io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{src}, make([]byte, 32<<10))
		if toClient && err == nil {
			err = conn.CloseWrite()
		}
		done <- result{toClient, err}
	}
	go copyStream(target, conn, false)
	go copyStream(conn, target, true)
	first := <-done
	if !first.toClient || first.err != nil {
		closeBoth()
	}
	second := <-done
	if first.err != nil {
		return fmt.Errorf("forward: %w", first.err)
	}
	return second.err
}
