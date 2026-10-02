package protocol_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/protocol"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/token"
)

const tokenIssuer = "https://tokens.test"

// issuer is a token service for the tests.
type issuer struct {
	t   *testing.T
	key ed25519.PrivateKey
}

// withTokens starts the server with an access token requirement. It returns the token service.
func withTokens(t *testing.T) (*issuer, func(*protocol.Options)) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public), "kid": "k1",
	}}})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "jwks.json")
	require.NoError(t, os.WriteFile(path, jwks, 0o600))
	return &issuer{t: t, key: private}, func(o *protocol.Options) {
		verifier, err := token.New(token.Config{
			JWKSFile: path, Issuer: tokenIssuer, Audience: testServerID, Leeway: time.Minute, Now: o.Now,
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		o.Tokens = verifier
	}
}

// issue returns a token for client that expires at expires.
func (i *issuer) issue(client ed25519.PublicKey, expires time.Time) string {
	i.t.Helper()
	return i.sign(jwt.MapClaims{
		"sub": "user@example.test",
		"exp": expires.Unix(),
		"cnf": map[string]any{"jwk": map[string]string{
			"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(client),
		}},
	})
}

// sign adds iss and aud to claims and signs them.
func (i *issuer) sign(claims jwt.MapClaims) string {
	i.t.Helper()
	claims["iss"] = tokenIssuer
	claims["aud"] = testServerID
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = "k1"
	signed, err := tok.SignedString(i.key)
	require.NoError(i.t, err)
	return signed
}

// loginWithToken runs the login with an access token and returns the server's last answer: HelloOk, or the Error
// that ended the login.
func (c *client) loginWithToken(public ed25519.PublicKey, private ed25519.PrivateKey, accessToken string) *pb.ServerFrame {
	c.t.Helper()
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Hello{Hello: &pb.Hello{
		ProtocolVersion: protocol.Version, InstanceId: c.instance,
		SigAlg: pb.SigAlg_SIG_ALG_ED25519, PublicKey: public, AccessToken: accessToken,
	}}})
	ch := answer.GetChallenge()
	if ch == nil {
		return answer
	}
	signed := transcript(ch.GetServerId(), ch.GetNonce(), c.instance, pb.SigAlg_SIG_ALG_ED25519, public)
	return c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
		Signature: ed25519.Sign(private, signed),
	}}})
}

// requireClosed checks that the server closes the connection with the given status.
func (c *client) requireClosed(status websocket.StatusCode) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_, _, err := c.ws.Read(ctx)
	require.Error(c.t, err)
	require.Equal(c.t, status, websocket.CloseStatus(err), "%v", err)
}

func TestLoginWithoutAccessTokenIsDenied(t *testing.T) {
	_, gate := withTokens(t)
	e := start(t, gate)
	public, private := keyPair(t)

	c := e.connect(t, 0xa)
	answer := c.loginWithToken(public, private, "")
	require.Equal(t, pb.ErrorCode_ERROR_CODE_ACCESS_DENIED, answer.GetError().GetCode(), "answer %v", answer)
	c.requireClosed(websocket.StatusPolicyViolation)
}

func TestAccessTokenAdmitsOnlyItsKey(t *testing.T) {
	tokens, gate := withTokens(t)
	e := start(t, gate)
	public, private := keyPair(t)
	accessToken := tokens.issue(public, t0.Add(time.Hour))

	c := e.connect(t, 0xa)
	answer := c.loginWithToken(public, private, accessToken)
	require.NotNil(t, answer.GetHelloOk(), "answer %v", answer)
	require.Equal(t, uint64(time.Hour.Milliseconds()), answer.GetHelloOk().GetAccessTokenTtlMs())

	// The same token with another key: stolen tokens are useless without the private key.
	other, otherPrivate := keyPair(t)
	thief := e.connect(t, 0xb)
	answer = thief.loginWithToken(other, otherPrivate, accessToken)
	require.Equal(t, pb.ErrorCode_ERROR_CODE_ACCESS_DENIED, answer.GetError().GetCode(), "answer %v", answer)
}

func TestExpiredTokenClosesTheConnectionAtTheNextRequest(t *testing.T) {
	tokens, gate := withTokens(t)
	e := start(t, gate)
	public, private := keyPair(t)

	c := e.connect(t, 0xa)
	answer := c.loginWithToken(public, private, tokens.issue(public, t0.Add(time.Hour)))
	require.NotNil(t, answer.GetHelloOk(), "answer %v", answer)
	opened := c.open(testDB, true, false)

	// Within the leeway of a minute after the expiry, requests are still served.
	e.clock.Advance(time.Hour + 30*time.Second)
	require.NotNil(t, c.call(commitFrame(opened.GetLeaseEpoch(), 0, 1, commitIDOf(1), block(0, 0xa0))).GetCommitAck())

	// After it, pings still renew the leases, but any other request closes the connection unanswered.
	e.clock.Advance(time.Minute)
	require.NotNil(t, c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Ping{Ping: &pb.Ping{}}}).GetPong())
	c.send(commitFrame(opened.GetLeaseEpoch(), 1, 1, commitIDOf(2), block(0, 0xa1)))
	c.requireClosed(websocket.StatusPolicyViolation)

	// With a new token the client resumes its lease on a new connection.
	again := e.connect(t, 0xa)
	answer = again.loginWithToken(public, private, tokens.issue(public, e.clock.Now().Add(time.Hour)))
	require.NotNil(t, answer.GetHelloOk(), "answer %v", answer)
	resumed := again.call(openFrame(testDB, false, false, &pb.Resume{
		LeaseId: opened.GetLeaseId(), LeaseEpoch: opened.GetLeaseEpoch(), KnownVersion: 1,
	}))
	require.NotNil(t, resumed.GetOpened(), "answer %v", resumed)
	require.Equal(t, uint64(1), resumed.GetOpened().GetVersion(), "the unanswered commit was not applied")
}

func TestServerWithoutTokensIgnoresThem(t *testing.T) {
	e := start(t)
	public, private := keyPair(t)
	c := e.connect(t, 0xa)
	answer := c.loginWithToken(public, private, "not a token")
	require.NotNil(t, answer.GetHelloOk(), "answer %v", answer)
	require.Zero(t, answer.GetHelloOk().GetAccessTokenTtlMs())
}
