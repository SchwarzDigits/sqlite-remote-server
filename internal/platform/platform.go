// Package platform provides logging, health endpoints, middleware and the HTTP server lifecycle.
package platform

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Paths of the liveness and readiness probes. They are served on the same address as the WebSocket. A reverse proxy
// in front of the server should not expose them.
const (
	PathLive  = "/.well-known/live"
	PathReady = "/.well-known/ready"
)

const (
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 10 * time.Second
	readyPingTimeout  = 1 * time.Second
)

// NewLogger returns a JSON logger that writes to stdout.
func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

// OKHandler returns 200 "ok". It serves the liveness probe.
func OKHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

// ReadyHandler returns 200 if ping succeeds within one second, and 503 otherwise. It serves the readiness probe.
func ReadyHandler(ping func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyPingTimeout)
		defer cancel()
		if err := ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

// Recover logs a panic in next and returns 500 instead of ending the process.
func Recover(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("panic in http handler", "panic", recovered, "path", r.URL.Path)
				// Has no effect if the handler already wrote the headers.
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Serve runs the HTTP server until ctx is canceled and then shuts it down within shutdownTimeout.
// http.Server.Shutdown does not wait for hijacked connections such as WebSockets. onShutdown closes them.
func Serve(ctx context.Context, log *slog.Logger, addr string, h http.Handler, onShutdown func(context.Context) error) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: readHeaderTimeout}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("http server listening", "addr", addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		err := srv.Shutdown(shutdownCtx)
		if onShutdown != nil {
			err = errors.Join(err, onShutdown(shutdownCtx))
		}
		return err
	}
}
