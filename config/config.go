// Package config reads the SQLITE_REMOTE_* environment variables of the command. No other package reads the
// environment. Load parses them into a server.Config, validates it and names the variable in every error.
//
// A program that receives the settings under other names, e.g. from a platform, calls LoadFrom with a function that
// translates the names.
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
	// EnvDeleteUnusedAfterDays is a number of days. 0 turns the deletion of unused databases off.
	EnvDeleteUnusedAfterDays = "SQLITE_REMOTE_DELETE_UNUSED_AFTER_DAYS"
	EnvTokenJWKSURL          = "SQLITE_REMOTE_TOKEN_JWKS_URL"
	EnvTokenJWKSFile         = "SQLITE_REMOTE_TOKEN_JWKS_FILE"
	EnvTokenIssuer           = "SQLITE_REMOTE_TOKEN_ISSUER"
	EnvTokenAudience         = "SQLITE_REMOTE_TOKEN_AUDIENCE"
	EnvTokenLeeway           = "SQLITE_REMOTE_TOKEN_LEEWAY"
	EnvTokenSlotLabelClaim   = "SQLITE_REMOTE_TOKEN_SLOT_LABEL_CLAIM"
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
	"DeleteUnusedAfter":      EnvDeleteUnusedAfterDays,
	"TokenJWKSURL":           EnvTokenJWKSURL,
	"TokenJWKSFile":          EnvTokenJWKSFile,
	"TokenIssuer":            EnvTokenIssuer,
	"TokenAudience":          EnvTokenAudience,
	"TokenLeeway":            EnvTokenLeeway,
	"TokenSlotLabelClaim":    EnvTokenSlotLabelClaim,
}

// Config is the configuration of the command.
type Config struct {
	Server   server.Config
	LogLevel slog.Level
}

// Load reads and validates the environment variables.
func Load() (Config, error) {
	return LoadFrom(os.Getenv)
}

// LoadFrom reads and validates the variables through getenv, which returns "" for an unset variable.
func LoadFrom(getenv func(string) string) (Config, error) {
	cfg := Config{Server: server.DefaultConfig(), LogLevel: slog.LevelInfo}
	s := &cfg.Server

	port := defaultPort
	if v := getenv(EnvPort); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 65535 {
			return Config{}, fmt.Errorf("%s: must be a port from 1 to 65535, got %q", EnvPort, v)
		}
		port = n
	}
	s.Addr = fmt.Sprintf(":%d", port)

	if v := getenv(EnvLogLevel); v != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			return Config{}, fmt.Errorf("%s: %w", EnvLogLevel, err)
		}
	}
	s.ServerID = getenv(EnvServerID)
	s.Store = server.StoreKind(getenv(EnvStore))
	s.DatabaseURL = getenv(EnvDatabaseURL)
	s.TokenJWKSURL = getenv(EnvTokenJWKSURL)
	s.TokenJWKSFile = getenv(EnvTokenJWKSFile)
	s.TokenIssuer = getenv(EnvTokenIssuer)
	s.TokenAudience = getenv(EnvTokenAudience)
	s.TokenSlotLabelClaim = getenv(EnvTokenSlotLabelClaim)

	var err error
	if s.DBMaxConns, err = connsVar(getenv, EnvDBMaxConns); err != nil {
		return Config{}, err
	}
	if s.DBMinConns, err = connsVar(getenv, EnvDBMinConns); err != nil {
		return Config{}, err
	}
	if v := getenv(EnvRequireSyncReplication); v != "" {
		if s.RequireSyncReplication, err = strconv.ParseBool(v); err != nil {
			return Config{}, fmt.Errorf("%s: must be true or false, got %q", EnvRequireSyncReplication, v)
		}
	}
	if err := uint32Var(getenv, EnvMaxFrameBytes, &s.MaxFrameBytes); err != nil {
		return Config{}, err
	}
	if err := uint64Var(getenv, EnvMaxCommitBytes, &s.MaxCommitBytes); err != nil {
		return Config{}, err
	}
	for _, d := range []struct {
		name  string
		value *time.Duration
	}{
		{EnvPingInterval, &s.PingInterval},
		{EnvLeaseTTL, &s.LeaseTTL},
		{EnvHelloTimeout, &s.HelloTimeout},
		{EnvTokenLeeway, &s.TokenLeeway},
	} {
		if err := durationVar(getenv, d.name, d.value); err != nil {
			return Config{}, err
		}
	}
	if v := getenv(EnvDeleteUnusedAfterDays); v != "" {
		days, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return Config{}, fmt.Errorf("%s: must be a number of days, 0 for off, got %q", EnvDeleteUnusedAfterDays, v)
		}
		s.DeleteUnusedAfter = time.Duration(days) * 24 * time.Hour
	}
	for _, origin := range strings.Split(getenv(EnvAllowedOrigins), ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			s.AllowedOrigins = append(s.AllowedOrigins, origin)
		}
	}

	if err := s.Validate(); err != nil {
		return Config{}, Named(err)
	}
	return cfg, nil
}

// Named replaces the field name in a *server.ConfigError with the variable that sets the field. server.Run returns
// such errors too, for checks it can only make at start. Other errors, and nil, are returned unchanged.
func Named(err error) error {
	var invalid *server.ConfigError
	if errors.As(err, &invalid) {
		if name, ok := envOf[invalid.Field]; ok {
			return fmt.Errorf("%s: %s", name, invalid.Problem)
		}
	}
	return err
}

// connsVar reads a pool size. An unset variable means 0, which keeps the pgxpool default.
func connsVar(getenv func(string) string, name string) (int32, error) {
	v := getenv(name)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s: must be a number of at least 1, got %q", name, v)
	}
	return int32(n), nil
}

func uint32Var(getenv func(string) string, name string, target *uint32) error {
	v := getenv(name)
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

func uint64Var(getenv func(string) string, name string, target *uint64) error {
	v := getenv(name)
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

func durationVar(getenv func(string) string, name string, target *time.Duration) error {
	v := getenv(name)
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
