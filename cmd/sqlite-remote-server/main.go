// Command sqlite-remote-server runs the page server of sqlite-remote-vfs. It reads its configuration from
// SQLITE_REMOTE_* environment variables, logs JSON to stdout and shuts down gracefully on SIGINT and SIGTERM.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/config"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/platform"
	"github.com/SchwarzDigits/sqlite-remote-server/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sqlite-remote-server:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return config.Named(server.Run(ctx, cfg.Server, platform.NewLogger(cfg.LogLevel)))
}
