package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// lossySocket drops the first packet of selected kinds and duplicates DATA. It
// reports successful local writes, matching what actual network loss looks like.
type lossySocket struct {
	net.PacketConn
	mu   sync.Mutex
	drop map[kind]bool
}

func (s *lossySocket) WriteTo(b []byte, peer net.Addr) (int, error) {
	s.mu.Lock()
	k := kind(b[3])
	if s.drop[k] {
		delete(s.drop, k)
		s.mu.Unlock()
		return len(b), nil
	}
	s.mu.Unlock()
	n, err := s.PacketConn.WriteTo(b, peer)
	if err == nil && k == kindData {
		_, err = s.PacketConn.WriteTo(b, peer)
	}
	return n, err
}

func udpSocket(t *testing.T) net.PacketConn {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}

func connectionPair(t *testing.T, cfg Config) (*Conn, *Conn, *Listener) {
	t.Helper()
	l, err := ListenPacket(udpSocket(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, err := DialContext(ctx, l.Addr().String(), "ssh-agent", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	b, err := l.AcceptContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return a, b, l
}

func TestUDPTransferWithLossAndHalfClose(t *testing.T) {
	cfg := DefaultConfig()
	serverSocket := &lossySocket{PacketConn: udpSocket(t), drop: map[kind]bool{kindOpened: true, kindACK: true}}
	l, err := ListenPacket(serverSocket, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientSocket := &lossySocket{PacketConn: udpSocket(t), drop: map[kind]bool{kindData: true, kindFIN: true}}
	c, err := DialPacket(ctx, clientSocket, l.Addr(), "ssh-agent", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := l.AcceptContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Target() != "ssh-agent" {
		t.Fatal("target lost in handshake")
	}
	l.ep.mu.Lock()
	acceptedTwice := len(l.ep.accept) != 0
	l.ep.mu.Unlock()
	if acceptedTwice {
		t.Fatal("duplicate OPEN allocated another connection")
	}
	deadline := time.Now().Add(5 * time.Second)
	c.SetDeadline(deadline)
	s.SetDeadline(deadline)
	want := bytes.Repeat([]byte("a complete duplex byte stream\n"), 4000)
	writer := make(chan error, 1)
	go func() {
		_, err := c.Write(want)
		if err == nil {
			err = c.CloseWrite()
		}
		writer <- err
	}()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("forward transfer: bytes=%d err=%v", len(got), err)
	}
	if err := <-writer; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("late")); !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("write after FIN: %v", err)
	}
	if _, err := s.Write([]byte("reply after peer EOF")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(c)
	if err != nil || string(got) != "reply after peer EOF" {
		t.Fatalf("reverse transfer: %q %v", got, err)
	}
}

func TestDeadlinesBlockedWritersAndClose(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiveWindowPackets = 1
	cfg.SendWindowPackets = 1
	a, b, l := connectionPair(t, cfg)
	if err := a.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline: %v", err)
	}
	a.SetReadDeadline(time.Time{})
	if _, err := a.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	// The receiver does not consume the first packet. Even once it is ACKed,
	// its receive window stays full and the next write must remain blocked.
	written := make(chan error, 1)
	go func() { _, err := a.Write([]byte("second")); written <- err }()
	a.SetWriteDeadline(time.Now().Add(-time.Second))
	select {
	case err := <-written:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("write deadline: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deadline update did not unblock writer")
	}
	a.SetWriteDeadline(time.Time{})
	buf := make([]byte, 5)
	b.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(b, buf); err != nil || string(buf) != "first" {
		t.Fatalf("recover after deadline: %q %v", buf, err)
	}
	read := make(chan error, 1)
	go func() { _, err := a.Read(make([]byte, 1)); read <- err }()
	// Closing the dedicated client socket must wake a blocked application read.
	a.Close()
	select {
	case err := <-read:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock Read")
	}
	l.Close()
	if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("accept after close: %v", err)
	}
}

func TestAdmissionProfileCancellationAndPeerBinding(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConnections = 1
	cfg.AcceptBacklog = 1
	a, b, l := connectionPair(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := DialContext(ctx, l.Addr().String(), "second", cfg); !errors.Is(err, ErrRejected) {
		t.Fatalf("admission ceiling: %v", err)
	}
	bad := cfg
	bad.DeliveryTimeout++
	if _, err := DialContext(ctx, l.Addr().String(), "incompatible", bad); !errors.Is(err, ErrRejected) {
		t.Fatalf("profile mismatch: %v", err)
	}
	// A packet carrying the right ID from a different UDP endpoint is ignored.
	l.ep.mu.Lock()
	id := b.state.id
	l.ep.dispatchLocked(packet{kind: kindReset, id: id}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, time.Now())
	err := b.err
	l.ep.mu.Unlock()
	if err != nil {
		t.Fatal("foreign peer reset connection")
	}
	a.Close()
	b.Close()
	if _, err := DialContext(ctx, l.Addr().String(), "quarantined", cfg); !errors.Is(err, ErrRejected) {
		t.Fatalf("quarantine bypassed: %v", err)
	}
	noReply := udpSocket(t)
	canceled, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := DialContext(canceled, noReply.LocalAddr().String(), "agent", cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestConcurrentConnections(t *testing.T) {
	cfg := DefaultConfig()
	l, err := ListenPacket(udpSocket(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const count = 12
	results := make(chan error, count)
	var writers sync.WaitGroup
	for i := 0; i < count; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			c, err := DialContext(ctx, l.Addr().String(), "agent", cfg)
			if err != nil {
				results <- err
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(4 * time.Second))
			_, err = c.Write([]byte("hello"))
			if err == nil {
				_, err = io.ReadFull(c, make([]byte, 5))
			}
			results <- err
		}()
	}
	for i := 0; i < count; i++ {
		c, err := l.AcceptContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(4 * time.Second))
		buf := make([]byte, 5)
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	writers.Wait()
	for i := 0; i < count; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
