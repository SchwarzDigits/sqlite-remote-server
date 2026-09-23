package protocol

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

// Path is the WebSocket endpoint. It is the only path the ingress exposes publicly.
const Path = "/v1/ws"

// Version is the protocol version of this server.
const Version = 1

// Options configures a Server.
type Options struct {
	Store store.Store
	Log   *slog.Logger
	// MaxFrameBytes limits the size of every frame in both directions.
	MaxFrameBytes uint32
	// PingInterval is the ping interval sent to clients. A connection without a frame for three intervals is
	// closed.
	PingInterval time.Duration
	LeaseTTL     time.Duration
	// MaxCommitBytes limits the total block data of one commit across all its parts.
	MaxCommitBytes uint64
	// HelloTimeout is the time a new connection has to send its Hello.
	HelloTimeout time.Duration
	// AllowedOrigins lists the hosts from which a browser page may connect. In each pattern, `*` matches any part
	// of a host, e.g. `*.example.com` or `127.0.0.1:*`. Empty means same origin only. Clients other than browsers
	// send no Origin header and are not affected.
	AllowedOrigins []string
	// ServerID is the public address of this server, e.g. "wss://vfs.example/v1/ws". It is part of the transcript a
	// client signs at login. If it is empty, every login is rejected.
	ServerID string
	// ChallengeTTL is how long a challenge is valid. Zero means 30 s.
	ChallengeTTL time.Duration
	// Now returns the current time. Nil means time.Now.
	Now func() time.Time
}

// Server serves the protocol on WebSocket connections. A client logs in by signing a challenge with its private
// key. The server derives the client's subject from the public key.
type Server struct {
	opts    Options
	holders *holders

	mu      sync.Mutex
	stopped bool
	stop    chan struct{}
	conns   sync.WaitGroup
}

// NewServer returns a Server for opts.
func NewServer(opts Options) *Server {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Server{opts: opts, holders: newHolders(), stop: make(chan struct{})}
}

// ServeHTTP upgrades the request to a WebSocket and serves it until the connection ends.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	s.conns.Add(1)
	s.mu.Unlock()
	defer s.conns.Done()

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.opts.AllowedOrigins})
	if err != nil {
		// Accept has already written the HTTP response.
		s.opts.Log.Info("websocket upgrade failed", "error", err)
		return
	}
	ws.SetReadLimit(int64(s.opts.MaxFrameBytes))
	newConnection(s, ws).serve(r.Context())
}

// Shutdown closes every connection with StatusGoingAway and waits until all connections have ended or ctx is done.
// Clients reconnect, possibly to another server instance, and resume their leases there.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stop)
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.conns.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
