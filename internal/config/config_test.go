package config_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/config"
	"github.com/SchwarzDigits/sqlite-remote-server/server"
)

func setMinimal(t *testing.T) {
	t.Helper()
	t.Setenv(config.EnvServerID, "wss://vfs.test/v1/ws")
	t.Setenv(config.EnvStore, "memory")
}

func TestDefaults(t *testing.T) {
	setMinimal(t)
	cfg, err := config.Load()
	require.NoError(t, err)
	want := server.DefaultConfig()
	want.Addr = ":8080"
	want.ServerID = "wss://vfs.test/v1/ws"
	want.Store = server.StoreMemory
	require.Equal(t, config.Config{Server: want, LogLevel: slog.LevelInfo}, cfg)
}

func TestOverrides(t *testing.T) {
	setMinimal(t)
	t.Setenv(config.EnvPort, "9000")
	t.Setenv(config.EnvLogLevel, "debug")
	t.Setenv(config.EnvMaxFrameBytes, "2097152")
	t.Setenv(config.EnvMaxCommitBytes, "1073741824")
	t.Setenv(config.EnvPingInterval, "5s")
	t.Setenv(config.EnvLeaseTTL, "1m")
	t.Setenv(config.EnvHelloTimeout, "2s")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, ":9000", cfg.Server.Addr)
	require.Equal(t, slog.LevelDebug, cfg.LogLevel)
	require.Equal(t, uint32(2<<20), cfg.Server.MaxFrameBytes)
	require.Equal(t, uint64(1<<30), cfg.Server.MaxCommitBytes)
	require.Equal(t, 5*time.Second, cfg.Server.PingInterval)
	require.Equal(t, time.Minute, cfg.Server.LeaseTTL)
	require.Equal(t, 2*time.Second, cfg.Server.HelloTimeout)
}

func TestPostgresRequiresDatabaseURL(t *testing.T) {
	setMinimal(t)
	t.Setenv(config.EnvStore, "postgres")
	_, err := config.Load()
	require.ErrorContains(t, err, config.EnvDatabaseURL)

	t.Setenv(config.EnvDatabaseURL, "postgres://user:secret@db:5432/vfs")
	t.Setenv(config.EnvDBMaxConns, "20")
	t.Setenv(config.EnvDBMinConns, "2")
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, server.StorePostgres, cfg.Server.Store)
	require.Equal(t, "postgres://user:secret@db:5432/vfs", cfg.Server.DatabaseURL)
	require.Equal(t, int32(20), cfg.Server.DBMaxConns)
	require.Equal(t, int32(2), cfg.Server.DBMinConns)
}

func TestAllowedOrigins(t *testing.T) {
	setMinimal(t)
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Empty(t, cfg.Server.AllowedOrigins, "default must be same origin only")

	t.Setenv(config.EnvAllowedOrigins, "client.example, *.pages.example ,")
	cfg, err = config.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"client.example", "*.pages.example"}, cfg.Server.AllowedOrigins)

	t.Setenv(config.EnvAllowedOrigins, "https://client.example")
	_, err = config.Load()
	require.ErrorContains(t, err, config.EnvAllowedOrigins)
}

func TestInvalidVariableIsNamedInError(t *testing.T) {
	for _, tc := range []struct {
		name, value string
	}{
		{config.EnvPort, "0"},
		{config.EnvPort, "70000"},
		{config.EnvServerID, ""},
		{config.EnvStore, ""},
		{config.EnvStore, "sqlite"},
		{config.EnvLogLevel, "loud"},
		{config.EnvMaxFrameBytes, "4096"},
		{config.EnvMaxFrameBytes, "many"},
		{config.EnvMaxCommitBytes, "1024"},
		{config.EnvPingInterval, "0s"},
		{config.EnvLeaseTTL, "15s"},
		{config.EnvHelloTimeout, "soon"},
		{config.EnvDBMaxConns, "0"},
		{config.EnvDBMaxConns, "some"},
		{config.EnvDBMinConns, "-1"},
		{config.EnvDBMinConns, "21"},
	} {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			setMinimal(t)
			t.Setenv(config.EnvStore, "postgres")
			t.Setenv(config.EnvDatabaseURL, "postgres://db/vfs")
			t.Setenv(config.EnvDBMaxConns, "20")
			t.Setenv(tc.name, tc.value)
			_, err := config.Load()
			require.ErrorContains(t, err, tc.name)
		})
	}
}
