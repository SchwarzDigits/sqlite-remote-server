package token

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const (
	issuer   = "https://tokens.test"
	audience = "wss://vfs.test/v1/ws"
)

var t0 = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

func b64(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

// service is a token service for the tests: it signs tokens and serves its JWKS.
type service struct {
	t   *testing.T
	kid string
	ed  ed25519.PrivateKey
	ec  *ecdsa.PrivateKey
}

func newService(t *testing.T, kid string) *service {
	t.Helper()
	_, ed, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return &service{t: t, kid: kid, ed: ed, ec: ec}
}

func (s *service) jwks() []byte {
	point, err := s.ec.PublicKey.Bytes()
	require.NoError(s.t, err)
	data, err := json.Marshal(map[string]any{"keys": []map[string]string{
		{"kty": "OKP", "crv": "Ed25519", "x": b64(s.ed.Public().(ed25519.PublicKey)), "kid": s.kid, "alg": "EdDSA", "use": "sig"},
		{"kty": "EC", "crv": "P-256", "x": b64(point[1:33]), "y": b64(point[33:]), "kid": s.kid + "-ec", "alg": "ES256"},
		{"kty": "RSA", "n": "AQAB", "e": "AQAB", "kid": "rsa"},
	}})
	require.NoError(s.t, err)
	return data
}

// claims returns valid claims for a client with the given key. change adjusts them.
func claimsFor(client ed25519.PublicKey, change func(jwt.MapClaims)) jwt.MapClaims {
	c := jwt.MapClaims{
		"iss": issuer,
		"aud": audience,
		"sub": "0b4c3e2a-5d1f-4c1a-9e3b-7a2d1f0e9c8b@example.test",
		"iat": t0.Unix(),
		"nbf": t0.Unix(),
		"exp": t0.Add(time.Hour).Unix(),
		"jti": "token-1",
		"cnf": map[string]any{"jwk": map[string]string{"kty": "OKP", "crv": "Ed25519", "x": b64(client)}},
	}
	if change != nil {
		change(c)
	}
	return c
}

func (s *service) sign(method jwt.SigningMethod, kid string, c jwt.MapClaims) string {
	token := jwt.NewWithClaims(method, c)
	token.Header["kid"] = kid
	var key any = s.ed
	if method == jwt.SigningMethodES256 {
		key = s.ec
	}
	signed, err := token.SignedString(key)
	require.NoError(s.t, err)
	return signed
}

func clientKey(t *testing.T) ed25519.PublicKey {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return public
}

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fileVerifier(t *testing.T, s *service, now *time.Time) *Verifier {
	t.Helper()
	return labelVerifier(t, s, now, "")
}

func labelVerifier(t *testing.T, s *service, now *time.Time, labelClaim string) *Verifier {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jwks.json")
	require.NoError(t, os.WriteFile(path, s.jwks(), 0o600))
	v, err := New(Config{
		JWKSFile: path, Issuer: issuer, Audience: audience, Leeway: time.Minute, LabelClaim: labelClaim,
		Now: func() time.Time { return *now },
	}, quiet())
	require.NoError(t, err)
	return v
}

func TestLabelComesFromTheConfiguredClaim(t *testing.T) {
	s := newService(t, "k1")
	now := t0.Add(time.Minute)
	v := labelVerifier(t, s, &now, "device")
	client := clientKey(t)

	withDevice := claimsFor(client, func(c jwt.MapClaims) { c["device"] = "c0ffee" })
	grant, err := v.Verify(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", withDevice), client)
	require.NoError(t, err)
	require.Equal(t, "c0ffee", grant.Label)

	grant, err = v.Verify(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil)), client)
	require.NoError(t, err)
	require.Empty(t, grant.Label, "without the claim")

	notString := claimsFor(client, func(c jwt.MapClaims) { c["device"] = 7 })
	grant, err = v.Verify(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", notString), client)
	require.NoError(t, err)
	require.Empty(t, grant.Label, "a claim that is not a string gives no label")
}

func TestUnboundTokensAreAcceptedOnlyWithoutKeyCheck(t *testing.T) {
	s := newService(t, "k1")
	now := t0.Add(time.Minute)
	v := fileVerifier(t, s, &now)
	client := clientKey(t)

	unbound := s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, func(c jwt.MapClaims) { delete(c, "cnf") }))
	grant, err := v.VerifyUnbound(context.Background(), unbound)
	require.NoError(t, err)
	require.Equal(t, "0b4c3e2a-5d1f-4c1a-9e3b-7a2d1f0e9c8b@example.test", grant.Owner)
	_, err = v.Verify(context.Background(), unbound, client)
	requireDenied(t, err, "a login needs a bound token")

	_, err = v.VerifyUnbound(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil)))
	require.NoError(t, err, "a bound token is accepted too")
	wrongAudience := claimsFor(client, func(c jwt.MapClaims) { c["aud"] = "other" })
	_, err = v.VerifyUnbound(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", wrongAudience))
	requireDenied(t, err, "the audience is still checked")
	_, err = v.VerifyUnbound(context.Background(), "")
	requireDenied(t, err)
}

func requireDenied(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	var rejected *DeniedError
	require.ErrorAs(t, err, &rejected, msgAndArgs...)
}

func TestValidTokensAreAccepted(t *testing.T) {
	s := newService(t, "k1")
	now := t0.Add(time.Minute)
	v := fileVerifier(t, s, &now)
	client := clientKey(t)

	grant, err := v.Verify(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil)), client)
	require.NoError(t, err)
	require.True(t, t0.Add(time.Hour).Equal(grant.Expires), "expiry %s", grant.Expires)
	require.Equal(t, "0b4c3e2a-5d1f-4c1a-9e3b-7a2d1f0e9c8b@example.test", grant.Owner)
	require.Empty(t, grant.Label, "no label claim is configured")

	_, err = v.Verify(context.Background(), s.sign(jwt.SigningMethodES256, "k1-ec", claimsFor(client, nil)), client)
	require.NoError(t, err, "ES256 is accepted too")

	withList := claimsFor(client, func(c jwt.MapClaims) { c["aud"] = []string{"other", audience} })
	_, err = v.Verify(context.Background(), s.sign(jwt.SigningMethodEdDSA, "k1", withList), client)
	require.NoError(t, err, "aud may be a list that contains the audience")
}

func TestInvalidTokensAreRejected(t *testing.T) {
	s := newService(t, "k1")
	other := newService(t, "k1")
	now := t0.Add(time.Minute)
	v := fileVerifier(t, s, &now)
	client := clientKey(t)

	valid := s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil))
	tampered := valid[:len(valid)-4] + "AAAA"
	unsigned := func() string {
		token := jwt.NewWithClaims(jwt.SigningMethodNone, claimsFor(client, nil))
		token.Header["kid"] = "k1"
		signed, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
		require.NoError(t, err)
		return signed
	}()
	hmac := func() string {
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claimsFor(client, nil))
		token.Header["kid"] = "k1"
		signed, err := token.SignedString([]byte(s.ed.Public().(ed25519.PublicKey)))
		require.NoError(t, err)
		return signed
	}()
	noKid := func() string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claimsFor(client, nil))
		signed, err := token.SignedString(s.ed)
		require.NoError(t, err)
		return signed
	}()
	change := func(f func(jwt.MapClaims)) string {
		return s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, f))
	}

	for name, token := range map[string]string{
		"empty":             "",
		"garbage":           "not a token",
		"tampered":          tampered,
		"alg none":          unsigned,
		"HS256":             hmac,
		"no kid":            noKid,
		"unknown kid":       s.sign(jwt.SigningMethodEdDSA, "k9", claimsFor(client, nil)),
		"other signer":      other.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil)),
		"EdDSA with EC key": s.sign(jwt.SigningMethodEdDSA, "k1-ec", claimsFor(client, nil)),
		"expired":           change(func(c jwt.MapClaims) { c["exp"] = t0.Add(-2 * time.Minute).Unix() }),
		"no exp":            change(func(c jwt.MapClaims) { delete(c, "exp") }),
		"not yet valid":     change(func(c jwt.MapClaims) { c["nbf"] = t0.Add(10 * time.Minute).Unix() }),
		"wrong issuer":      change(func(c jwt.MapClaims) { c["iss"] = "https://other.test" }),
		"no issuer":         change(func(c jwt.MapClaims) { delete(c, "iss") }),
		"wrong audience":    change(func(c jwt.MapClaims) { c["aud"] = "wss://other.test/v1/ws" }),
		"no cnf":            change(func(c jwt.MapClaims) { delete(c, "cnf") }),
		"cnf of other key":  s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(clientKey(t), nil)),
		"cnf not Ed25519": change(func(c jwt.MapClaims) {
			c["cnf"] = map[string]any{"jwk": map[string]string{"kty": "EC", "crv": "P-256", "x": b64(client)}}
		}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := v.Verify(context.Background(), token, client)
			requireDenied(t, err)
		})
	}
}

func TestLeewayAllowsClockDifferences(t *testing.T) {
	s := newService(t, "k1")
	now := t0.Add(time.Hour + 30*time.Second)
	v := fileVerifier(t, s, &now)
	client := clientKey(t)
	token := s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil))

	_, err := v.Verify(context.Background(), token, client)
	require.NoError(t, err, "30 s after exp is within the leeway of a minute")
	now = t0.Add(time.Hour + 2*time.Minute)
	_, err = v.Verify(context.Background(), token, client)
	requireDenied(t, err)
}

// jwksServer serves a JWKS that the test can replace, and counts the requests.
type jwksServer struct {
	mu      sync.Mutex
	body    []byte
	maxAge  string
	fetches atomic.Int32
	*httptest.Server
}

func newJWKSServer(t *testing.T, body []byte, maxAge string) *jwksServer {
	t.Helper()
	j := &jwksServer{body: body, maxAge: maxAge}
	j.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		j.fetches.Add(1)
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.maxAge != "" {
			w.Header().Set("Cache-Control", "public, max-age="+j.maxAge)
		}
		_, _ = w.Write(j.body)
	}))
	t.Cleanup(j.Close)
	return j
}

func (j *jwksServer) serve(body []byte) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.body = body
}

func urlVerifier(t *testing.T, url string, now *time.Time) *Verifier {
	t.Helper()
	v, err := New(Config{
		JWKSURL: url, Issuer: issuer, Audience: audience,
		Now: func() time.Time { return *now },
	}, quiet())
	require.NoError(t, err)
	return v
}

func TestJWKSIsCachedForItsMaxAge(t *testing.T) {
	s := newService(t, "k1")
	j := newJWKSServer(t, s.jwks(), "60")
	now := t0.Add(time.Minute)
	v := urlVerifier(t, j.URL, &now)
	client := clientKey(t)
	token := s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil))

	for range 3 {
		_, err := v.Verify(context.Background(), token, client)
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), j.fetches.Load(), "cached within max-age")

	now = now.Add(61 * time.Second)
	_, err := v.Verify(context.Background(), token, client)
	require.NoError(t, err)
	require.Equal(t, int32(2), j.fetches.Load(), "fetched again after max-age")
}

func TestMaxAgeIsCappedAtFiveMinutes(t *testing.T) {
	require.Equal(t, 5*time.Minute, cacheAge("public, max-age=86400"))
	require.Equal(t, 5*time.Minute, cacheAge(""))
	require.Equal(t, 300*time.Second, cacheAge("max-age=300"))
	require.Equal(t, minCacheAge, cacheAge("max-age=0"))
}

func TestUnknownKeyIDFetchesAtMostOncePerMinute(t *testing.T) {
	old := newService(t, "k1")
	j := newJWKSServer(t, old.jwks(), "300")
	now := t0.Add(time.Minute)
	v := urlVerifier(t, j.URL, &now)
	client := clientKey(t)
	_, err := v.Verify(context.Background(), old.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, nil)), client)
	require.NoError(t, err)

	// The token service rotates to k2. Its first token has an unknown key ID and causes a fetch.
	rotated := newService(t, "k2")
	j.serve(rotated.jwks())
	now = now.Add(2 * time.Minute)
	token := rotated.sign(jwt.SigningMethodEdDSA, "k2", claimsFor(client, nil))
	_, err = v.Verify(context.Background(), token, client)
	require.NoError(t, err, "the new key is fetched")
	require.Equal(t, int32(2), j.fetches.Load())

	// Further unknown key IDs within a minute do not fetch again.
	for range 5 {
		_, err = v.Verify(context.Background(), rotated.sign(jwt.SigningMethodEdDSA, "k7", claimsFor(client, nil)), client)
		requireDenied(t, err)
	}
	require.Equal(t, int32(2), j.fetches.Load())
}

func TestStaleJWKSIsUsedDuringAnOutage(t *testing.T) {
	s := newService(t, "k1")
	j := newJWKSServer(t, s.jwks(), "300")
	now := t0.Add(time.Minute)
	v := urlVerifier(t, j.URL, &now)
	client := clientKey(t)
	token := func() string {
		return s.sign(jwt.SigningMethodEdDSA, "k1", claimsFor(client, func(c jwt.MapClaims) {
			c["exp"] = now.Add(time.Hour).Unix()
		}))
	}
	_, err := v.Verify(context.Background(), token(), client)
	require.NoError(t, err)

	j.Close()
	now = now.Add(30 * time.Minute)
	_, err = v.Verify(context.Background(), token(), client)
	require.NoError(t, err, "the keys fetched before are used while the JWKS cannot be fetched")

	now = now.Add(2 * time.Hour)
	_, err = v.Verify(context.Background(), token(), client)
	requireDenied(t, err, "not after more than an hour")
}

func TestJWKSFileMustContainAKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jwks.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"keys":[{"kty":"RSA","kid":"r"}]}`), 0o600))
	_, err := New(Config{JWKSFile: path, Issuer: issuer, Audience: audience}, quiet())
	require.ErrorContains(t, err, "no Ed25519 or P-256")

	_, err = New(Config{Issuer: issuer, Audience: audience}, quiet())
	require.Error(t, err, "a JWKS source is required")
}
