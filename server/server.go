// Package server runs the page server of sqlite-remote-vfs: it stores the blocks of SQLite databases whose VFS runs
// on a client, and serves the WebSocket endpoint and the health probes on one address.
//
// Programs that read their configuration their own way build a Config, starting from DefaultConfig, and call Run.
// The command in cmd/sqlite-remote-server reads it from SQLITE_REMOTE_* environment variables.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/platform"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/protocol"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/memory"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/postgres"
)

// Paths served on Config.Addr.
const (
	PathWebSocket = protocol.Path
	PathLive      = platform.PathLive
	PathReady     = platform.PathReady
)

// StoreKind selects where the databases are stored.
type StoreKind string

const (
	// StoreMemory keeps the databases in memory. A restart deletes them. There is no default store, so this must be
	// chosen explicitly.
	StoreMemory StoreKind = "memory"
	// StorePostgres keeps the databases in PostgreSQL. It requires Config.DatabaseURL.
	StorePostgres StoreKind = "postgres"
)

// minMaxFrameBytes is one block of the largest page size (64 KiB) plus 1 KiB for the rest of the frame.
const minMaxFrameBytes = 65536 + 1024

// Config configures Run. Start from DefaultConfig: its zero value is not valid.
type Config struct {
	// Addr is the TCP address to listen on, e.g. ":8080".
	Addr string
	// ServerID identifies this server in the login transcript, normally its public WebSocket URL, e.g.
	// "wss://vfs.example/v1/ws". A client that compares it with the URL it connected to detects a challenge relayed
	// by another server. Required.
	ServerID string
	// Store selects the store. Required.
	Store StoreKind

	// DatabaseURL is the PostgreSQL connection string. Required for StorePostgres.
	DatabaseURL string
	// DBMaxConns and DBMinConns set the connection pool size. 0 keeps the pgxpool default.
	DBMaxConns int32
	DBMinConns int32

	// MaxFrameBytes is the largest WebSocket frame accepted. It must hold one block of the largest page size.
	MaxFrameBytes uint32
	// MaxCommitBytes is the largest commit accepted, over all its parts.
	MaxCommitBytes uint64
	// PingInterval is the interval at which clients ping while idle.
	PingInterval time.Duration
	// LeaseTTL is the time after the last renewal from which another instance can take a lease without takeover.
	// It must be at least twice PingInterval, so that one lost ping does not cost a client its lease.
	LeaseTTL time.Duration
	// HelloTimeout is the time a client has to complete the login.
	HelloTimeout time.Duration
	// AllowedOrigins lists the hosts from which a browser page may connect. In each pattern, `*` matches any part of
	// a host, e.g. `*.example.com` or `127.0.0.1:*`. Empty allows only pages from the server's own origin. Clients
	// other than browsers send no Origin header and are not affected.
	AllowedOrigins []string
}

// DefaultConfig returns the default limits and timeouts. Addr, ServerID and Store are left to the caller.
func DefaultConfig() Config {
	return Config{
		MaxFrameBytes:  1 << 20,
		MaxCommitBytes: 256 << 20,
		PingInterval:   10 * time.Second,
		LeaseTTL:       30 * time.Second,
		HelloTimeout:   5 * time.Second,
	}
}

// ConfigError reports an invalid field of Config. Field is the Go field name, so that a caller that reads the
// configuration from its own sources can name its own setting in the message.
type ConfigError struct {
	Field   string
	Problem string
}

func (e *ConfigError) Error() string {
	return e.Field + ": " + e.Problem
}

func invalid(field, format string, args ...any) error {
	return &ConfigError{Field: field, Problem: fmt.Sprintf(format, args...)}
}

// Validate checks the configuration. It returns a *ConfigError for the first invalid field.
func (c Config) Validate() error {
	switch {
	case c.Addr == "":
		return invalid("Addr", "is required")
	case c.ServerID == "":
		return invalid("ServerID", "is required, e.g. wss://vfs.example/v1/ws")
	case c.Store != StoreMemory && c.Store != StorePostgres:
		return invalid("Store", "must be %q or %q, got %q", StoreMemory, StorePostgres, c.Store)
	case c.Store == StorePostgres && c.DatabaseURL == "":
		return invalid("DatabaseURL", "is required for the %s store", StorePostgres)
	case c.DBMaxConns < 0:
		return invalid("DBMaxConns", "must not be negative, got %d", c.DBMaxConns)
	case c.DBMinConns < 0:
		return invalid("DBMinConns", "must not be negative, got %d", c.DBMinConns)
	case c.DBMaxConns > 0 && c.DBMinConns > c.DBMaxConns:
		return invalid("DBMinConns", "must not exceed the maximum pool size %d, got %d", c.DBMaxConns, c.DBMinConns)
	case c.MaxFrameBytes < minMaxFrameBytes:
		return invalid("MaxFrameBytes", "must be at least %d to hold one block of the largest page size", minMaxFrameBytes)
	case c.MaxCommitBytes < uint64(c.MaxFrameBytes):
		return invalid("MaxCommitBytes", "must be at least the maximum frame size %d", c.MaxFrameBytes)
	case c.PingInterval <= 0:
		return invalid("PingInterval", "must be positive")
	case c.LeaseTTL < 2*c.PingInterval:
		return invalid("LeaseTTL", "must be at least twice the ping interval %s", c.PingInterval)
	case c.HelloTimeout <= 0:
		return invalid("HelloTimeout", "must be positive")
	}
	for _, origin := range c.AllowedOrigins {
		if origin == "" || strings.ContainsAny(origin, "/ ") {
			return invalid("AllowedOrigins", "%q must be a host pattern without scheme or path", origin)
		}
	}
	return nil
}

// Run validates the configuration, opens the store and serves until ctx is canceled. It then closes open connections
// with "going away", so clients reconnect elsewhere, and returns after the shutdown.
func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	st, closeStore, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer closeStore()

	srv := protocol.NewServer(protocol.Options{
		Store:          st,
		Log:            log,
		MaxFrameBytes:  cfg.MaxFrameBytes,
		PingInterval:   cfg.PingInterval,
		LeaseTTL:       cfg.LeaseTTL,
		MaxCommitBytes: cfg.MaxCommitBytes,
		HelloTimeout:   cfg.HelloTimeout,
		AllowedOrigins: cfg.AllowedOrigins,
		ServerID:       cfg.ServerID,
	})
	mux := http.NewServeMux()
	mux.Handle("GET "+PathWebSocket, srv)
	mux.Handle("GET "+PathLive, platform.OKHandler())
	mux.Handle("GET "+PathReady, platform.ReadyHandler(st.Ping))

	log.Info("starting server", "addr", cfg.Addr, "server_id", cfg.ServerID, "store", cfg.Store)
	return platform.Serve(ctx, log, cfg.Addr, platform.Recover(log, mux), srv.Shutdown)
}

// openStore opens the configured store and returns it with a function that closes it.
func openStore(ctx context.Context, cfg Config, log *slog.Logger) (store.Store, func(), error) {
	if cfg.Store == StoreMemory {
		log.Warn("memory store: all databases are lost on restart")
		return memory.New(), func() {}, nil
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("database URL: %w", err)
	}
	if cfg.DBMaxConns > 0 {
		poolCfg.MaxConns = cfg.DBMaxConns
	}
	if cfg.DBMinConns > 0 {
		poolCfg.MinConns = cfg.DBMinConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	// If several instances start at once, a session lock lets one migrate while the others wait.
	if err := postgres.Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, nil, err
	}
	log.Info("database ready, migrations applied", "max_conns", poolCfg.MaxConns, "min_conns", poolCfg.MinConns)
	return postgres.New(pool), pool.Close, nil
}
