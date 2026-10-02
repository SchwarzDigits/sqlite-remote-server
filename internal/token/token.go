// Package token checks access tokens. With them, a server admits only clients that a token service has admitted.
//
// A token is a JWT signed with EdDSA (Ed25519) or ES256. Its cnf claim (RFC 7800) holds the client's Ed25519 public
// key, which must equal the key the client logs in with. A token is therefore useless without the private key. The
// server learns the token service's public keys from a JWKS, fetched from a URL or read from a file.
package token

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Algorithms accepted for token signatures. Symmetric algorithms and "none" are never accepted.
var algorithms = []string{"EdDSA", "ES256"}

// Config configures a Verifier.
type Config struct {
	// JWKSURL is the URL of the token service's JWKS. Exactly one of JWKSURL and JWKSFile is required.
	JWKSURL string
	// JWKSFile is a file with the JWKS, read once, for tests and for networks without access to the token service.
	JWKSFile string
	// Issuer is the required iss claim.
	Issuer string
	// Audience must be the aud claim, or one of its values.
	Audience string
	// LabelClaim names the claim that holds the label of the client's slot, e.g. the id of its device. Empty means
	// slots have no label.
	LabelClaim string
	// Leeway allows for clock differences in exp and nbf.
	Leeway time.Duration
	// HTTPClient fetches the JWKS. Nil means a client with a timeout of 10 s.
	HTTPClient *http.Client
	// Now returns the current time. Nil means time.Now.
	Now func() time.Time
}

// Verifier checks tokens. It is safe for concurrent use.
type Verifier struct {
	keys       *keySet
	parser     *jwt.Parser
	leeway     time.Duration
	now        func() time.Time
	labelClaim string
}

// Grant is what a valid token says about its client.
type Grant struct {
	Expires time.Time
	// Owner is the token's sub: the owner of the client's key and of its slot. Empty if the token has no sub.
	Owner string
	// Label is the value of Config.LabelClaim, empty if that is not configured or the token has no such string claim.
	Label string
}

// DeniedError reports why a token was rejected.
type DeniedError struct {
	Reason string
}

func (e *DeniedError) Error() string {
	return "access token rejected: " + e.Reason
}

func denied(format string, args ...any) error {
	return &DeniedError{Reason: fmt.Sprintf(format, args...)}
}

// New returns a Verifier. It reads JWKSFile right away. A JWKS URL is fetched on the first token.
func New(cfg Config, log *slog.Logger) (*Verifier, error) {
	if (cfg.JWKSURL == "") == (cfg.JWKSFile == "") {
		return nil, errors.New("exactly one of the JWKS URL and the JWKS file is required")
	}
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("issuer and audience are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: fetchTimeout}
	}
	keys, err := newKeySet(cfg, log)
	if err != nil {
		return nil, err
	}
	return &Verifier{
		keys: keys,
		parser: jwt.NewParser(
			jwt.WithValidMethods(algorithms),
			jwt.WithIssuer(cfg.Issuer),
			jwt.WithAudience(cfg.Audience),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(cfg.Leeway),
			jwt.WithTimeFunc(cfg.Now),
		),
		leeway:     cfg.Leeway,
		now:        cfg.Now,
		labelClaim: cfg.LabelClaim,
	}, nil
}

// Leeway returns the allowance for clock differences.
func (v *Verifier) Leeway() time.Duration {
	return v.leeway
}

type claims struct {
	jwt.RegisteredClaims
	Cnf *struct {
		JWK *struct {
			Kty string `json:"kty"`
			Crv string `json:"crv"`
			X   string `json:"x"`
		} `json:"jwk"`
	} `json:"cnf"`
	// all holds every claim, for the label claim, whose name is configured.
	all map[string]json.RawMessage
}

func (c *claims) UnmarshalJSON(data []byte) error {
	type fields claims
	if err := json.Unmarshal(data, (*fields)(c)); err != nil {
		return err
	}
	return json.Unmarshal(data, &c.all)
}

// label returns the string claim name, or "" if name is empty or the claim is missing or not a string.
func (c *claims) label(name string) string {
	var label string
	if name == "" || json.Unmarshal(c.all[name], &label) != nil {
		return ""
	}
	return label
}

// Verify checks a token for a client that logs in with the Ed25519 key publicKey. A rejected token returns a
// *DeniedError.
func (v *Verifier) Verify(ctx context.Context, token string, publicKey []byte) (Grant, error) {
	c, err := v.parse(ctx, token)
	if err != nil {
		return Grant{}, err
	}
	if c.Cnf == nil || c.Cnf.JWK == nil {
		return Grant{}, denied("no cnf.jwk claim that binds the token to a key")
	}
	jwk := c.Cnf.JWK
	if jwk.Kty != "OKP" || jwk.Crv != "Ed25519" {
		return Grant{}, denied("cnf.jwk must be an Ed25519 key, got %s %s", jwk.Kty, jwk.Crv)
	}
	bound, err := base64.RawURLEncoding.DecodeString(jwk.X)
	if err != nil || len(bound) != ed25519.PublicKeySize {
		return Grant{}, denied("cnf.jwk.x is not a base64url Ed25519 key")
	}
	if !bytes.Equal(bound, publicKey) {
		return Grant{}, denied("the token is bound to another key")
	}
	return v.grant(c), nil
}

// VerifyUnbound checks a token without requiring that it is bound to a key. Only requests that need no proof of a key
// accept such a token, e.g. looking up the owner's slot. A rejected token returns a *DeniedError.
func (v *Verifier) VerifyUnbound(ctx context.Context, token string) (Grant, error) {
	c, err := v.parse(ctx, token)
	if err != nil {
		return Grant{}, err
	}
	return v.grant(c), nil
}

func (v *Verifier) grant(c *claims) Grant {
	return Grant{Expires: c.ExpiresAt.Time, Owner: c.Subject, Label: c.label(v.labelClaim)}
}

// parse checks the signature, issuer, audience and times of a token and returns its claims.
func (v *Verifier) parse(ctx context.Context, token string) (*claims, error) {
	if token == "" {
		return nil, denied("no access token")
	}
	var c claims
	_, err := v.parser.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("no kid in the header")
		}
		key, err := v.keys.find(ctx, kid)
		if err != nil {
			return nil, err
		}
		alg := t.Method.Alg()
		if key.alg != "" && key.alg != alg {
			return nil, fmt.Errorf("key %q is for %s, the token uses %s", kid, key.alg, alg)
		}
		switch k := key.key.(type) {
		case ed25519.PublicKey:
			if alg != "EdDSA" {
				return nil, fmt.Errorf("key %q is an Ed25519 key, the token uses %s", kid, alg)
			}
			return k, nil
		case *ecdsa.PublicKey:
			if alg != "ES256" {
				return nil, fmt.Errorf("key %q is a P-256 key, the token uses %s", kid, alg)
			}
			return k, nil
		default:
			return nil, fmt.Errorf("key %q has an unsupported type", kid)
		}
	})
	if err != nil {
		var rejected *DeniedError
		if errors.As(err, &rejected) {
			return nil, rejected
		}
		return nil, denied("%v", err)
	}
	return &c, nil
}
