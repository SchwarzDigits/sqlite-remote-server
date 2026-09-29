package token

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// maxCacheAge caps how long a fetched JWKS is used without fetching it again, whatever its max-age says. A
	// token service that rotates its key publishes the new key at least this long before it signs with it.
	maxCacheAge = 5 * time.Minute
	// minCacheAge keeps a JWKS with a max-age of 0 from being fetched for every login.
	minCacheAge = 10 * time.Second
	// refetchInterval limits the fetches caused by tokens with an unknown key ID.
	refetchInterval = time.Minute
	// maxStale is how long a JWKS is still used after it expired, while fetching it fails. It keeps logins working
	// during a short outage of the token service.
	maxStale          = time.Hour
	fetchTimeout      = 10 * time.Second
	maxJWKSBytes      = 1 << 20
	uncompressedPoint = 0x04
)

// publicKey is one key of a JWKS: an ed25519.PublicKey or an *ecdsa.PublicKey on P-256, and the alg the JWKS names
// for it, if any.
type publicKey struct {
	key any
	alg string
}

// keySet holds the keys of the token service by key ID.
type keySet struct {
	url    string
	client *http.Client
	now    func() time.Time
	log    *slog.Logger

	mu   sync.Mutex
	keys map[string]publicKey
	// fresh is the time until which keys are used without fetching the JWKS again.
	fresh time.Time
	// fetched is the time of the last fetch attempt, successful or not.
	fetched time.Time
}

func newKeySet(cfg Config, log *slog.Logger) (*keySet, error) {
	s := &keySet{url: cfg.JWKSURL, client: cfg.HTTPClient, now: cfg.Now, log: log}
	if cfg.JWKSFile == "" {
		return s, nil
	}
	data, err := os.ReadFile(cfg.JWKSFile)
	if err != nil {
		return nil, fmt.Errorf("read the JWKS file: %w", err)
	}
	keys, err := parseJWKS(data)
	if err != nil {
		return nil, fmt.Errorf("JWKS file %s: %w", cfg.JWKSFile, err)
	}
	s.keys = keys
	return s, nil
}

// find returns the key with ID kid. It fetches the JWKS if the cached one has expired, and once more if kid is
// unknown, at most once per refetchInterval.
func (s *keySet) find(ctx context.Context, kid string) (publicKey, error) {
	if s.url == "" {
		if key, ok := s.keys[kid]; ok {
			return key, nil
		}
		return publicKey{}, denied("unknown key ID %q", kid)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !now.Before(s.fresh) && now.Sub(s.fetched) >= minCacheAge {
		s.refresh(ctx, now)
	}
	if !now.Before(s.fresh.Add(maxStale)) {
		return publicKey{}, denied("the token service's keys are not available")
	}
	if key, ok := s.keys[kid]; ok {
		return key, nil
	}
	// A new key ID can mean a key rotation.
	if now.Sub(s.fetched) >= refetchInterval {
		s.refresh(ctx, now)
		if key, ok := s.keys[kid]; ok {
			return key, nil
		}
	}
	return publicKey{}, denied("unknown key ID %q", kid)
}

// refresh fetches the JWKS. If that fails, the keys fetched before stay in use.
func (s *keySet) refresh(ctx context.Context, now time.Time) {
	s.fetched = now
	keys, age, err := s.fetch(ctx)
	if err != nil {
		s.log.Warn("fetching the JWKS failed", "url", s.url, "error", err)
		return
	}
	s.keys = keys
	s.fresh = now.Add(age)
}

func (s *keySet) fetch(ctx context.Context) (map[string]publicKey, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("status %s", resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return nil, 0, err
	}
	if len(data) > maxJWKSBytes {
		return nil, 0, fmt.Errorf("the JWKS exceeds %d bytes", maxJWKSBytes)
	}
	keys, err := parseJWKS(data)
	if err != nil {
		return nil, 0, err
	}
	return keys, cacheAge(resp.Header.Get("Cache-Control")), nil
}

// cacheAge returns the max-age of a Cache-Control header, limited to minCacheAge and maxCacheAge. Without a max-age
// it returns maxCacheAge.
func cacheAge(header string) time.Duration {
	for directive := range strings.SplitSeq(header, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(directive), "=")
		if !ok || !strings.EqualFold(name, "max-age") {
			continue
		}
		seconds, err := strconv.ParseInt(strings.Trim(value, `"`), 10, 64)
		if err != nil || seconds < 0 {
			break
		}
		return min(max(time.Duration(seconds)*time.Second, minCacheAge), maxCacheAge)
	}
	return maxCacheAge
}

type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
}

// parseJWKS returns the signature keys of a JWKS by key ID. Keys without an ID, for another use or of another type
// are skipped. A JWKS without a usable key is an error.
func parseJWKS(data []byte) (map[string]publicKey, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("invalid JWKS: %w", err)
	}
	keys := make(map[string]publicKey)
	for _, k := range set.Keys {
		if k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		key, err := parseKey(k)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k.Kid, err)
		}
		if key.key != nil {
			keys[k.Kid] = key
		}
	}
	if len(keys) == 0 {
		return nil, errors.New("the JWKS has no Ed25519 or P-256 signature key with a key ID")
	}
	return keys, nil
}

// parseKey returns an Ed25519 or P-256 key. For other key types it returns no key and no error.
func parseKey(k jwk) (publicKey, error) {
	switch {
	case k.Kty == "OKP" && k.Crv == "Ed25519":
		if k.Alg != "" && k.Alg != "EdDSA" {
			return publicKey{}, fmt.Errorf("an Ed25519 key cannot be for %s", k.Alg)
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return publicKey{}, errors.New("x is not a base64url Ed25519 key")
		}
		return publicKey{key: ed25519.PublicKey(x), alg: k.Alg}, nil
	case k.Kty == "EC" && k.Crv == "P-256":
		if k.Alg != "" && k.Alg != "ES256" {
			return publicKey{}, fmt.Errorf("a P-256 key cannot be for %s", k.Alg)
		}
		x, errX := base64.RawURLEncoding.DecodeString(k.X)
		y, errY := base64.RawURLEncoding.DecodeString(k.Y)
		if errX != nil || errY != nil || len(x) != 32 || len(y) != 32 {
			return publicKey{}, errors.New("x and y must be base64url coordinates of 32 bytes")
		}
		point := append(append([]byte{uncompressedPoint}, x...), y...)
		key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
		if err != nil {
			return publicKey{}, fmt.Errorf("not a point on P-256: %w", err)
		}
		return publicKey{key: key, alg: k.Alg}, nil
	default:
		return publicKey{}, nil
	}
}
