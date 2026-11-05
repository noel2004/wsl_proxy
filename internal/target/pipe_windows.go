package target

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

// DefaultAgentPipe is shared by Microsoft OpenSSH and the Windows 1Password agent.
const DefaultAgentPipe = `\\.\pipe\openssh-ssh-agent`

// NamedPipe is an immutable local Windows pipe target. Open returns a fresh
// handle per call. Only byte-type pipes are supported; no message framing or
// agent-specific interpretation is imposed by this adapter.
type NamedPipe struct{ path string }

// NewNamedPipe validates a local pipe path without opening it. Remote SMB pipes,
// files and device paths are deliberately excluded from this adapter.
func NewNamedPipe(path string) (*NamedPipe, error) {
	const prefix = `\\.\pipe\`
	if !strings.HasPrefix(strings.ToLower(path), prefix) || len(path) <= len(prefix) ||
		strings.ContainsAny(path[len(prefix):], `\/:`) || strings.ContainsRune(path, 0) {
		return nil, errors.New(`target: expected a local pipe path \\.\pipe\NAME`)
	}
	return &NamedPipe{path: path}, nil
}

// Open waits only for busy instances, bounded by ctx; missing pipes and access
// failures return immediately. Overlapped handles let Go's poller provide
// concurrent I/O, deadlines and Close cancellation without CGO or per-I/O threads.
// The caller owns the returned stream. ctx controls opening only.
func (p *NamedPipe) Open(ctx context.Context) (io.ReadWriteCloser, error) {
	name, err := windows.UTF16PtrFromString(p.path)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Identification permits the pipe server to identify this Windows user,
		// without granting impersonation rights to an arbitrary configured pipe.
		h, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE, 0, nil,
			windows.OPEN_EXISTING, windows.FILE_FLAG_OVERLAPPED|windows.SECURITY_SQOS_PRESENT|windows.SECURITY_IDENTIFICATION, 0)
		if errors.Is(err, windows.ERROR_PIPE_BUSY) {
			// Poll rather than block in WaitNamedPipe so cancellation stays prompt.
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if err != nil {
			return nil, &os.PathError{Op: "open pipe", Path: p.path, Err: err}
		}
		var flags uint32
		if err := windows.GetNamedPipeInfo(h, &flags, nil, nil, nil); err != nil || flags&windows.PIPE_TYPE_MESSAGE != 0 {
			windows.CloseHandle(h)
			if err != nil {
				return nil, fmt.Errorf("target: inspect pipe: %w", err)
			}
			return nil, errors.New("target: message-type pipes are not supported")
		}
		file := os.NewFile(uintptr(h), p.path)
		// Check poller registration before handing out a handle whose blocked
		// operations must be cancellable. A zero deadline imposes no timeout.
		if err := file.SetDeadline(time.Time{}); err != nil {
			file.Close()
			return nil, fmt.Errorf("target: pipe deadlines: %w", err)
		}
		if err := ctx.Err(); err != nil {
			file.Close()
			return nil, err
		}
		return file, nil
	}
}
