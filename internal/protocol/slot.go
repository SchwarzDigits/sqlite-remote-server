package protocol

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/token"
)

// SlotPath is the HTTP endpoint that returns the slot of a token's owner. It needs Options.Tokens.
const SlotPath = "/v1/slot"

// SlotResponse is the JSON body of GET SlotPath. Slot is null if the owner has no slot.
type SlotResponse struct {
	Slot *SlotInfo `json:"slot"`
}

// SlotInfo describes a slot. The response does not name the key that holds it.
type SlotInfo struct {
	Label       string `json:"label"`
	ClaimedAtMs int64  `json:"claimedAtMs"`
}

// ServeSlot serves GET and OPTIONS on SlotPath. GET needs an access token in the Authorization header as
// "Bearer <token>". The token need not be bound to a key: the response says which slot the owner has, not what is
// in it. A client calls it before it has its key, to learn whether the owner already has a slot and with which label.
//
// Browsers may call it from the same origin and from the hosts in Options.AllowedOrigins. Other origins get 403.
func (s *Server) ServeSlot(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		if !s.originAllowed(r.Host, origin) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Add("Vary", "Origin")
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization")
		w.Header().Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if s.opts.Tokens == nil {
		http.Error(w, "slots need access tokens", http.StatusNotFound)
		return
	}

	bearer, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	grant, err := s.opts.Tokens.VerifyUnbound(r.Context(), bearer)
	if err == nil && grant.Owner == "" {
		err = &token.DeniedError{Reason: "no sub claim"}
	}
	if err != nil {
		var rejected *token.DeniedError
		if errors.As(err, &rejected) {
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			http.Error(w, rejected.Error(), http.StatusUnauthorized)
			return
		}
		s.opts.Log.Error("slot lookup failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	slot, ok, err := s.opts.Store.GetSlot(r.Context(), grant.Owner)
	if err != nil {
		s.opts.Log.Error("slot lookup failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	var res SlotResponse
	if ok {
		res.Slot = &SlotInfo{Label: slot.Label, ClaimedAtMs: slot.ClaimedAt.UnixMilli()}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// originAllowed applies the rules of the WebSocket endpoint: the server's own host, or a host that matches one of
// Options.AllowedOrigins.
func (s *Server) originAllowed(host, origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, host) {
		return true
	}
	for _, pattern := range s.opts.AllowedOrigins {
		if ok, _ := path.Match(strings.ToLower(pattern), strings.ToLower(u.Host)); ok {
			return true
		}
	}
	return false
}
