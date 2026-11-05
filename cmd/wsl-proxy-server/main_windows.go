// wsl-proxy-server is a foreground Windows byte-stream proxy. It deliberately
// exposes no Service Control Manager integration and accepts only loopback binds.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/noel2004/wsl_proxy/internal/server"
	"github.com/noel2004/wsl_proxy/internal/target"
	"github.com/noel2004/wsl_proxy/internal/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("wsl-proxy-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", "127.0.0.1:55222", "loopback UDP IP:port (0 chooses a free port)")
	pipePath := flags.String("pipe", target.DefaultAgentPipe, `Windows target pipe, e.g. \\.\pipe\openssh-ssh-agent`)
	sshAgent := flags.Bool("ssh-agent", false, `shorthand for -pipe \\.\pipe\openssh-ssh-agent (cannot combine with -pipe)`)
	name := flags.String("target", "ssh-agent", "logical target name clients may select")
	maxSessions := flags.Int("max-sessions", 128, "maximum active target connections, including pending opens")
	openTimeout := flags.Duration("open-timeout", 5*time.Second, "maximum wait for a busy target pipe")
	debug := flags.Bool("debug", false, "log connection failures to stderr (never application bytes)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *sshAgent {
		pipeSpecified := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "pipe" {
				pipeSpecified = true
			}
		})
		if pipeSpecified {
			return errors.New("-ssh-agent and -pipe cannot be used together")
		}
		*pipePath = target.DefaultAgentPipe
	}
	host, port, err := net.SplitHostPort(*listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	ip := net.ParseIP(host)
	portNumber, err := strconv.Atoi(port)
	if ip == nil || !ip.IsLoopback() || err != nil || portNumber < 0 || portNumber > 65535 {
		return errors.New("listen must be a loopback IP and numeric port; remote access requires authentication")
	}
	tcfg := transport.DefaultConfig()
	if *name == "" || len(*name) > tcfg.MaxDatagramBytes-64 {
		return errors.New("target name is empty or too long for OPEN")
	}
	pipe, err := target.NewNamedPipe(*pipePath)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	cfg := server.DefaultConfig()
	cfg.Targets = map[string]server.OpenFunc{*name: pipe.Open}
	cfg.MaxSessions, cfg.OpenTimeout, cfg.Logger = *maxSessions, *openTimeout, logger
	s, err := server.New(cfg)
	if err != nil {
		return err
	}
	l, err := transport.Listen(*listen, tcfg)
	if err != nil {
		return err
	}
	// This line is also the interop harness's readiness signal. Pipes are opened
	// lazily per connection, so a stopped/locked agent can recover without restart.
	fmt.Fprintf(stdout, "listening=%s target=%s pipe=%s\n", l.Addr(), *name, *pipePath)
	logger.Info("foreground proxy started; local clients share this Windows user's target access")
	return s.Serve(ctx, l)
}
