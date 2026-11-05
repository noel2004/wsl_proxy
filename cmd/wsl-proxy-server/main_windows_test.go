package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/noel2004/wsl_proxy/client"
	"github.com/noel2004/wsl_proxy/internal/testpipe"
)

func TestRunValidation(t *testing.T) {
	for _, args := range [][]string{{"-listen", "0.0.0.0:55222"}, {"-listen", ":55222"}, {"-listen", "localhost:55222"}, {"-pipe", `C:\file`}, {"-target", ""}, {"-max-sessions", "0"}, {"-open-timeout", "0"}, {"unexpected"}} {
		if err := run(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
	if err := run(context.Background(), []string{"-help"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestRunConfiguredPipeAndShutdown(t *testing.T) {
	fixture, err := testpipe.New(false)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		defer writer.Close()
		done <- run(ctx, []string{"-listen", "127.0.0.1:0", "-pipe", fixture.Path, "-target", "custom"}, writer, io.Discard)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("CLI shutdown blocked")
		}
	})
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	address := strings.TrimPrefix(strings.Fields(line)[0], "listening=")
	openCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	c, err := client.DialContext(openCtx, address, "custom", client.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	s, err := fixture.Accept()
	if err != nil {
		t.Fatal(err)
	}
	s.SetDeadline(time.Now().Add(3 * time.Second))
	echoDone := make(chan error, 1)
	payload := bytes.Repeat([]byte("custom pipe\x00"), 10000)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(s, got)
		if err == nil {
			_, err = s.Write(got)
		}
		echoDone <- err
	}()
	if _, err = c.Write(payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err = io.ReadFull(c, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("reply: %v", err)
	}
	if err = <-echoDone; err != nil {
		t.Fatal(err)
	}
	// Cancel while both forwarding reads are waiting, then verify target EOF.
	cancel()
	if _, err = s.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("target shutdown: %v", err)
	}
}
