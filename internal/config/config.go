// Package config reads the SQLITE_REMOTE_* environment variables of the command. No other package reads the
// environment. Load parses them into a server.Config, validates it and names the variable in every error.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SchwarzDigits/sqlite-remote-server/server"
)

// Environment variable names.
const (
	EnvPort        = "SQLITE_REMOTE_PORT"
	EnvLogLevel    = "SQLITE_REMOTE_LOG_LEVEL"
	EnvServerID    = "SQLITE_REMOTE_SERVER_ID"
	EnvStore       = "SQLITE_REMOTE_STORE"
	EnvDatabaseURL = "SQLITE_REMOTE_DATABASE_URL"
	EnvDBMaxConns  = "SQLITE_REMOTE_DB_MAX_CONNS"
	EnvDBMinConns  = "SQLITE_REMOTE_DB_MIN_CONNS"
	// EnvRequireSyncReplication is true or false.
	EnvRequireSyncReplication = "SQLITE_REMOTE_REQUIRE_SYNC_REPLICATION"
	EnvMaxFrameBytes          = "SQLITE_REMOTE_MAX_FRAME_BYTES"
	EnvMaxCommitBytes         = "SQLITE_REMOTE_MAX_COMMIT_BYTES"
	EnvPingInterval           = "SQLITE_REMOTE_PING_INTERVAL"
	EnvLeaseTTL               = "SQLITE_REMOTE_LEASE_TTL"
	EnvHelloTimeout           = "SQLITE_REMOTE_HELLO_TIMEOUT"
	// EnvAllowedOrigins is a comma-separated list of host patterns.
	EnvAllowedOrigins = "SQLITE_REMOTE_ALLOWED_ORIGINS"
)

const defaultPort = 8080

// envOf maps the fields of server.Config to the variables that set them, for error messages.
var envOf = map[string]string{
	"Addr":                   EnvPort,
	"ServerID":               EnvServerID,
	"Store":                  EnvStore,
	"DatabaseURL":            EnvDatabaseURL,
	"DBMaxConns":             EnvDBMaxConns,
	"DBMinConns":             EnvDBMinConns,
	"RequireSyncReplication": EnvRequireSyncReplication,
	"MaxFrameBytes":          EnvMaxFrameBytes,
	"MaxCommitBytes":         EnvMaxCommitBytes,
	"PingInterval":           EnvPingInterval,
	"LeaseTTL":               EnvLeaseTTL,
	"HelloTimeout":           EnvHelloTimeout,
	"AllowedOrigins":         EnvAllowedOrigins,
}

// Config is the configuration of the command.
type Config struct {
	Server   server.Config
	LogLevel slog.Level
}

// Load reads and validates the environment variables.
func Load() (Config, error) {
	cfg := Config{Server: server.DefaultConfig(), LogLevel: slog.LevelInfo}
	s := &cfg.Server

	port := defaultPort
	if v := os.Getenv(EnvPort); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 65535 {
			return Config{}, fmt.Errorf("%s: must be a port from 1 to 65535, got %q", EnvPort, v)
		}
		port = n
	}
	s.Addr = fmt.Sprintf(":%d", port)

	if v := os.Getenv(EnvLogLevel); v != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			return Config{}, fmt.Errorf("%s: %w", EnvLogLevel, err)
		}
	}
	s.ServerID = os.Getenv(EnvServerID)
	s.Store = server.StoreKind(os.Getenv(EnvStore))
	s.DatabaseURL = os.Getenv(EnvDatabaseURL)

	var err error
	if s.DBMaxConns, err = connsVar(EnvDBMaxConns); err != nil {
		return Config{}, err
	}
	if s.DBMinConns, err = connsVar(EnvDBMinConns); err != nil {
		return Config{}, err
	}
	if v := os.Getenv(EnvRequireSyncReplication); v != "" {
		if s.RequireSyncReplication, err = strconv.ParseBool(v); err != nil {
			return Config{}, fmt.Errorf("%s: must be true or false, got %q", EnvRequireSyncReplication, v)
		}
	}
	if err := uint32Var(EnvMaxFrameBytes, &s.MaxFrameBytes); err != nil {
		return Config{}, err
	}
	if err := uint64Var(EnvMaxCommitBytes, &s.MaxCommitBytes); err != nil {
		return Config{}, err
	}
	for _, d := range []struct {
		name  string
		value *time.Duration
	}{{EnvPingInterval, &s.PingInterval}, {EnvLeaseTTL, &s.LeaseTTL}, {EnvHelloTimeout, &s.HelloTimeout}} {
		if err := durationVar(d.name, d.value); err != nil {
			return Config{}, err
		}
	}
	for _, origin := range strings.Split(os.Getenv(EnvAllowedOrigins), ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			s.AllowedOrigins = append(s.AllowedOrigins, origin)
		}
	}

	if err := s.Validate(); err != nil {
		var invalid *server.ConfigError
		if errors.As(err, &invalid) {
			if name, ok := envOf[invalid.Field]; ok {
				return Config{}, fmt.Errorf("%s: %s", name, invalid.Problem)
			}
		}
		return Config{}, err
	}
	return cfg, nil
}

// connsVar reads a pool size. An unset variable means 0, which keeps the pgxpool default.
func connsVar(name string) (int32, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s: must be a number of at least 1, got %q", name, v)
	}
	return int32(n), nil
}

func uint32Var(name string, target *uint32) error {
	v := os.Getenv(name)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = uint32(n)
	return nil
}

func uint64Var(name string, target *uint64) error {
	v := os.Getenv(name)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = n
	return nil
}

func durationVar(name string, target *time.Duration) error {
	v := os.Getenv(name)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if d <= 0 {
		return fmt.Errorf("%s: must be positive, got %s", name, v)
	}
	*target = d
	return nil
}
