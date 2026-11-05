// Package testpipe supplies an isolated byte-pipe fixture for Windows tests.
package testpipe

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/sys/windows"
)

var serial atomic.Uint64

// Pipe owns one listening instance. Accept and Close must not run concurrently.
// Close releases pending connection I/O and any file returned by Accept.
type Pipe struct {
	Path    string
	handle  windows.Handle
	event   windows.Handle
	ol      windows.Overlapped
	pending bool
	file    *os.File
}

func New(messageMode bool) (*Pipe, error) {
	p := &Pipe{Path: fmt.Sprintf(`\\.\pipe\wsl-proxy-test-%d-%d`, os.Getpid(), serial.Add(1))}
	name, _ := windows.UTF16PtrFromString(p.Path)
	mode := uint32(windows.PIPE_TYPE_BYTE | windows.PIPE_READMODE_BYTE | windows.PIPE_REJECT_REMOTE_CLIENTS)
	if messageMode {
		mode |= windows.PIPE_TYPE_MESSAGE
	}
	h, err := windows.CreateNamedPipe(name, windows.PIPE_ACCESS_DUPLEX|windows.FILE_FLAG_OVERLAPPED|windows.FILE_FLAG_FIRST_PIPE_INSTANCE, mode, 1, 4096, 4096, 0, nil)
	if err != nil {
		return nil, err
	}
	p.handle = h
	p.event, err = windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	p.ol.HEvent = p.event
	err = windows.ConnectNamedPipe(h, &p.ol)
	if errors.Is(err, windows.ERROR_IO_PENDING) {
		p.pending = true
	} else if err != nil && !errors.Is(err, windows.ERROR_PIPE_CONNECTED) {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Pipe) Accept() (*os.File, error) {
	if p.pending {
		var n uint32
		if err := windows.GetOverlappedResult(p.handle, &p.ol, &n, true); err != nil {
			return nil, err
		}
		p.pending = false
	}
	p.file = os.NewFile(uintptr(p.handle), p.Path)
	return p.file, nil
}

func (p *Pipe) Close() {
	if p.file != nil {
		p.file.Close()
	} else {
		if p.pending {
			windows.CancelIoEx(p.handle, &p.ol)
			var n uint32
			windows.GetOverlappedResult(p.handle, &p.ol, &n, true)
		}
		windows.CloseHandle(p.handle)
	}
	windows.CloseHandle(p.event)
}
