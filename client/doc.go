// Package client opens ordered streams to configured Windows resources over the
// WSL proxy UDP transport. The library is portable; Windows pipe paths belong in
// server configuration, while clients select a logical target name.
//
// Transport acceptance does not guarantee that the target opened successfully.
// Target failures appear as stream errors. There is no automatic reconnect or
// replay, authentication, or encryption; use only in a trusted local environment.
package client
