// Package server forwards transport streams to a fixed registry of targets.
// It bounds active target handles and copy buffers independently of transport
// admission. Platform adapters must interrupt pending I/O when closed.
package server
