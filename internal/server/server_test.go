package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/noel2004/wsl_proxy/client"
	"github.com/noel2004/wsl_proxy/internal/transport"
)

func startServer(t *testing.T, cfg Config) (string, func()) {
	t.Helper()
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := transport.Listen("127.0.0.1:0", transport.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("server shutdown blocked")
		}
	})
	return l.Addr().String(), cancel
}

// blockedTarget models an agent that accepts a connection but services neither
// direction. Closing the peer must release it without needing server shutdown.
type blockedTarget struct {
	closed  chan struct{}
	writing chan struct{}
	once    sync.Once
}

func (b *blockedTarget) Read([]byte) (int, error) { <-b.closed; return 0, net.ErrClosed }
func (b *blockedTarget) Write([]byte) (int, error) {
	close(b.writing)
	<-b.closed
	return 0, net.ErrClosed
}
func (b *blockedTarget) Close() error { b.once.Do(func() { close(b.closed) }); return nil }

func TestDisconnectClosesBlockedTarget(t *testing.T) {
	target := &blockedTarget{closed: make(chan struct{}), writing: make(chan struct{})}
	cfg := DefaultConfig()
	cfg.Targets = map[string]OpenFunc{"blocked": func(context.Context) (io.ReadWriteCloser, error) { return target, nil }}
	address, _ := startServer(t, cfg)
	c := dial(t, address, "blocked")
	if _, err := c.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-target.writing:
	case <-time.After(time.Second):
		t.Fatal("request never reached target")
	}
	c.Close()
	select {
	case <-target.closed:
	case <-time.After(time.Second):
		t.Fatal("disconnect left pipe I/O blocked")
	}
}

func dial(t *testing.T, address, name string) *client.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, address, name, client.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestForwardingAndTargetEOF(t *testing.T) {
	peers := make(chan net.Conn, 2)
	cfg := DefaultConfig()
	cfg.Targets = map[string]OpenFunc{"ssh-agent": func(context.Context) (io.ReadWriteCloser, error) {
		a, b := net.Pipe()
		peers <- b
		return a, nil
	}}
	address, _ := startServer(t, cfg)
	for range 2 {
		c := dial(t, address, "ssh-agent")
		peer := <-peers
		peer.SetDeadline(time.Now().Add(3 * time.Second))
		payload := bytes.Repeat([]byte("opaque duplex stream\x00"), 10000)
		done := make(chan error, 1)
		go func() {
			defer peer.Close()
			got := make([]byte, len(payload))
			_, err := io.ReadFull(peer, got)
			if err == nil && !bytes.Equal(got, payload) {
				err = errors.New("target got corrupted request")
			}
			if err == nil {
				_, err = peer.Write(got)
			}
			done <- err
		}()
		if _, err := c.Write(payload); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(c)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("reply/EOF: bytes=%d error=%v", len(got), err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
}

func TestUnknownAndUnavailableTargets(t *testing.T) {
	var opened atomic.Int32
	cfg := DefaultConfig()
	cfg.Targets = map[string]OpenFunc{"missing": func(context.Context) (io.ReadWriteCloser, error) {
		opened.Add(1)
		return nil, errors.New("pipe unavailable")
	}}
	address, _ := startServer(t, cfg)
	for _, name := range []string{`\\.\pipe\client-supplied`, "missing"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, err := client.DialContext(ctx, address, name, client.DefaultConfig())
		cancel()
		if c != nil {
			c.SetReadDeadline(time.Now().Add(time.Second))
			_, err = c.Read(make([]byte, 1))
			c.Close()
		}
		if !errors.Is(err, transport.ErrPeerClosed) {
			t.Fatalf("target %q: %v", name, err)
		}
	}
	if opened.Load() != 1 {
		t.Fatalf("unregistered target opened: %d", opened.Load())
	}
}

func TestAdmissionAndCancelPendingOpen(t *testing.T) {
	entered := make(chan struct{}, 1)
	finished := make(chan struct{}, 1)
	cfg := DefaultConfig()
	cfg.MaxSessions = 1
	cfg.Targets = map[string]OpenFunc{"slow": func(ctx context.Context) (io.ReadWriteCloser, error) {
		entered <- struct{}{}
		<-ctx.Done()
		finished <- struct{}{}
		return nil, ctx.Err()
	}}
	address, cancel := startServer(t, cfg)
	first := dial(t, address, "slow")
	<-entered
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	second, err := client.DialContext(ctx, address, "slow", client.DefaultConfig())
	stop()
	if second != nil {
		second.SetDeadline(time.Now().Add(time.Second))
		_, err = second.Read(make([]byte, 1))
		second.Close()
	}
	if !errors.Is(err, transport.ErrPeerClosed) {
		t.Fatalf("overload: %v", err)
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("open ignored shutdown")
	}
	if _, err := first.Read(make([]byte, 1)); err == nil {
		t.Fatal("shutdown left stream alive")
	}
}

func TestShutdownClosesBlockedTarget(t *testing.T) {
	peers := make(chan net.Conn, 1)
	cfg := DefaultConfig()
	cfg.Targets = map[string]OpenFunc{"blocked": func(context.Context) (io.ReadWriteCloser, error) {
		a, b := net.Pipe()
		peers <- b
		return a, nil
	}}
	address, cancel := startServer(t, cfg)
	c := dial(t, address, "blocked")
	peer := <-peers
	defer peer.Close()
	peer.SetReadDeadline(time.Now().Add(time.Second))
	// The target never consumes this request; shutdown must interrupt its Write.
	if _, err := c.Write([]byte("blocked request")); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err := io.ReadAll(peer)
	if err != nil {
		t.Fatalf("target close: %v", err)
	}
}
