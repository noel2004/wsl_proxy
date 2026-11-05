package target

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"os"
	"testing"
	"time"

	"github.com/noel2004/wsl_proxy/internal/testpipe"
)

var realAgentPipe = flag.String("agent-pipe", "", "opt-in read-only identities test against this Windows agent pipe")

func fixture(t *testing.T, message bool) (*testpipe.Pipe, *NamedPipe) {
	t.Helper()
	f, err := testpipe.New(message)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	p, err := NewNamedPipe(f.Path)
	if err != nil {
		t.Fatal(err)
	}
	return f, p
}

func TestPipeDuplexAndCancellation(t *testing.T) {
	f, p := fixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := p.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := f.Accept()
	if err != nil {
		t.Fatal(err)
	}
	s.SetDeadline(time.Now().Add(3 * time.Second))
	c.(*os.File).SetDeadline(time.Now().Add(3 * time.Second))
	payload := bytes.Repeat([]byte("duplex\x00"), 20000)
	done := make(chan error, 1)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(s, got)
		if err == nil && !bytes.Equal(got, payload) {
			err = errors.New("bad request")
		}
		if err == nil {
			_, err = s.Write(got)
		}
		done <- err
	}()
	if _, err = c.Write(payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err = io.ReadFull(c, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("reply: %v", err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	c.(*os.File).SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err = c.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	c.(*os.File).SetDeadline(time.Time{})
	go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
	c.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("close read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("close left read blocked")
	}
}

func TestBusyPipeTimeout(t *testing.T) {
	f, p := fixture(t, false)
	c, err := p.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = f.Accept(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err = p.Open(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy pipe: %v", err)
	}
}

func TestInvalidMissingAndMessagePipes(t *testing.T) {
	for _, path := range []string{"", `C:\file`, `\\server\pipe\name`, `\\.\pipe\`, `\\.\pipe\a\b`, `\\.\pipe\a` + "\x00"} {
		if _, err := NewNamedPipe(path); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	f, p := fixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Open(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	missing, _ := NewNamedPipe(f.Path + "-missing")
	if _, err := missing.Open(context.Background()); err == nil {
		t.Fatal("missing pipe opened")
	}
	_, message := fixture(t, true)
	if c, err := message.Open(context.Background()); err == nil {
		c.Close()
		t.Fatal("message pipe opened")
	}
}

// TestAgentIdentities touches no private keys and logs neither public keys nor
// comments. It is opt-in so ordinary test runs never contact a user's agent.
func TestAgentIdentities(t *testing.T) {
	if *realAgentPipe == "" {
		t.Skip("set -agent-pipe to test the installed Windows agent")
	}
	p, err := NewNamedPipe(*realAgentPipe)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := p.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.(*os.File).SetDeadline(time.Now().Add(10 * time.Second))
	for range 2 {
		if _, err = c.Write([]byte{0, 0, 0, 1, 11}); err != nil {
			t.Fatal(err)
		}
		var header [4]byte
		if _, err = io.ReadFull(c, header[:]); err != nil {
			t.Fatal(err)
		}
		n := binary.BigEndian.Uint32(header[:])
		if n < 5 || n > 256<<10 {
			t.Fatalf("invalid identities response length %d", n)
		}
		body := make([]byte, n)
		if _, err = io.ReadFull(c, body); err != nil {
			t.Fatal(err)
		}
		if body[0] != 12 {
			t.Fatalf("unexpected response type %d", body[0])
		}
		t.Logf("agent returned identities response (%d identities)", binary.BigEndian.Uint32(body[1:5]))
	}
}
