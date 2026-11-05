package client_test

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"github.com/noel2004/wsl_proxy/client"
)

// TestWindowsAgentInterop is opt-in: start the Windows CLI through WSL interop,
// then set WSL_PROXY_TEST_ADDR to its loopback UDP address. Only public identity
// enumeration is requested; keys and comments are neither logged nor persisted.
func TestWindowsAgentInterop(t *testing.T) {
	address := os.Getenv("WSL_PROXY_TEST_ADDR")
	if address == "" {
		t.Skip("set WSL_PROXY_TEST_ADDR for the live Windows agent test")
	}
	name := os.Getenv("WSL_PROXY_TEST_TARGET")
	if name == "" {
		name = "ssh-agent"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, address, name, client.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(15 * time.Second))
	for range 2 {
		// Deliberately split the agent frame: transport and pipe write boundaries
		// must not be mistaken for application message boundaries.
		for _, part := range [][]byte{{0, 0}, {0, 1, 11}} {
			if _, err := c.Write(part); err != nil {
				t.Fatal(err)
			}
		}
		var header [4]byte
		if _, err := io.ReadFull(c, header[:]); err != nil {
			t.Fatal(err)
		}
		n := binary.BigEndian.Uint32(header[:])
		if n < 5 || n > 256<<10 {
			t.Fatalf("invalid identities reply length %d", n)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(c, body); err != nil {
			t.Fatal(err)
		}
		if body[0] != 12 {
			t.Fatalf("unexpected agent reply %d", body[0])
		}
		count := binary.BigEndian.Uint32(body[1:5])
		remaining := body[5:]
		for i := uint32(0); i < count; i++ {
			// Every identity has two SSH strings: public key blob and comment.
			for range 2 {
				if len(remaining) < 4 {
					t.Fatal("truncated identity string")
				}
				size := binary.BigEndian.Uint32(remaining[:4])
				remaining = remaining[4:]
				if uint64(size) > uint64(len(remaining)) {
					t.Fatal("invalid identity string length")
				}
				remaining = remaining[size:]
			}
		}
		if len(remaining) != 0 {
			t.Fatal("trailing identity reply bytes")
		}
		t.Logf("Windows agent returned %d identities through UDP", count)
	}
}
