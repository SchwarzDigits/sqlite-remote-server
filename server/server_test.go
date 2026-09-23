package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SchwarzDigits/sqlite-remote-server/server"
)

func valid() server.Config {
	cfg := server.DefaultConfig()
	cfg.Addr = ":8080"
	cfg.ServerID = "wss://vfs.test/v1/ws"
	cfg.Store = server.StoreMemory
	return cfg
}

func TestDefaultConfigNeedsAddrServerIDAndStore(t *testing.T) {
	var invalid *server.ConfigError
	require.ErrorAs(t, server.DefaultConfig().Validate(), &invalid)
	require.NoError(t, valid().Validate())
}

func TestValidateNamesTheField(t *testing.T) {
	for _, tc := range []struct {
		field  string
		change func(*server.Config)
	}{
		{"Addr", func(c *server.Config) { c.Addr = "" }},
		{"ServerID", func(c *server.Config) { c.ServerID = "" }},
		{"Store", func(c *server.Config) { c.Store = "sqlite" }},
		{"DatabaseURL", func(c *server.Config) { c.Store = server.StorePostgres }},
		{"DBMaxConns", func(c *server.Config) { c.DBMaxConns = -1 }},
		{"DBMinConns", func(c *server.Config) { c.DBMaxConns, c.DBMinConns = 2, 3 }},
		{"MaxFrameBytes", func(c *server.Config) { c.MaxFrameBytes = 4096 }},
		{"MaxCommitBytes", func(c *server.Config) { c.MaxCommitBytes = 1024 }},
		{"PingInterval", func(c *server.Config) { c.PingInterval = 0 }},
		{"LeaseTTL", func(c *server.Config) { c.LeaseTTL = c.PingInterval }},
		{"HelloTimeout", func(c *server.Config) { c.HelloTimeout = 0 }},
		{"AllowedOrigins", func(c *server.Config) { c.AllowedOrigins = []string{"https://client.example"} }},
	} {
		t.Run(tc.field, func(t *testing.T) {
			cfg := valid()
			tc.change(&cfg)
			var invalid *server.ConfigError
			require.ErrorAs(t, cfg.Validate(), &invalid)
			require.Equal(t, tc.field, invalid.Field)
		})
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	var invalid *server.ConfigError
	err := server.Run(context.Background(), server.DefaultConfig(), slog.New(slog.DiscardHandler))
	require.ErrorAs(t, err, &invalid)
}

func TestRunServesUntilCanceled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	cfg := valid()
	cfg.Addr = addr
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()

	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + server.PathReady)
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond, "readiness probe must answer 200")

	cancel()
	select {
	case err := <-done:
		require.True(t, err == nil || errors.Is(err, context.Canceled), "Run must return after cancel: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
