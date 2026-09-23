package platform_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/platform"
)

func get(t *testing.T, h http.Handler) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Code, rec.Body.String()
}

func TestLiveness(t *testing.T) {
	code, body := get(t, platform.OKHandler())
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ok", body)
}

func TestReadiness(t *testing.T) {
	code, _ := get(t, platform.ReadyHandler(func(context.Context) error { return nil }))
	require.Equal(t, http.StatusOK, code)

	code, body := get(t, platform.ReadyHandler(func(context.Context) error { return errors.New("down") }))
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "not ready", body)
}

func TestRecoverReturns500OnPanic(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	code, _ := get(t, platform.Recover(log, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	require.Equal(t, http.StatusInternalServerError, code)
}
