package protocol_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/protocol"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/memory"
)

const testOwner = "owner-1"

// device returns a token for the key of name, owned by owner and labeled label.
func (i *issuer) device(name, owner, label string) string {
	i.t.Helper()
	public, _ := keyFor(name)
	return i.sign(jwt.MapClaims{
		"sub":          owner,
		testLabelClaim: label,
		"exp":          t0.Add(time.Hour).Unix(),
		"cnf": map[string]any{"jwk": map[string]string{
			"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public),
		}},
	})
}

// unbound returns a token of owner without key binding.
func (i *issuer) unbound(owner string) string {
	i.t.Helper()
	return i.sign(jwt.MapClaims{"sub": owner, "exp": t0.Add(time.Hour).Unix()})
}

// loginAs logs in with the key of name and its token.
func (c *client) loginAs(name, accessToken string) {
	c.t.Helper()
	public, private := keyFor(name)
	answer := c.loginWithToken(public, private, accessToken)
	require.NotNil(c.t, answer.GetHelloOk(), "answer %v", answer)
}

func claimFrame() *pb.ClientFrame {
	return &pb.ClientFrame{Body: &pb.ClientFrame_ClaimSlot{ClaimSlot: &pb.ClaimSlot{}}}
}

func deleteSlotFrame() *pb.ClientFrame {
	return &pb.ClientFrame{Body: &pb.ClientFrame_DeleteSlot{DeleteSlot: &pb.DeleteSlot{}}}
}

// getSlot calls GET SlotPath with the token and returns the status and the decoded body of a 200.
func getSlot(t *testing.T, e *env, accessToken string, header http.Header) (*http.Response, protocol.SlotResponse) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(e.server.ServeSlot))
	t.Cleanup(srv.Close)
	req, err := http.NewRequest(http.MethodGet, srv.URL+protocol.SlotPath, nil)
	require.NoError(t, err)
	for k, v := range header {
		req.Header[k] = v
	}
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	res, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	var body protocol.SlotResponse
	if res.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(res.Body).Decode(&body))
	}
	return res, body
}

func TestSlotsNeedAccessTokens(t *testing.T) {
	e := start(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	requireError(t, a.call(claimFrame()), pb.ErrorCode_ERROR_CODE_BAD_REQUEST)
	requireError(t, a.call(deleteSlotFrame()), pb.ErrorCode_ERROR_CODE_BAD_REQUEST)

	res, _ := getSlot(t, e, "", nil)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func TestClaimSlotReturnsTheSlot(t *testing.T) {
	tokens, gate := withTokens(t)
	e := start(t, gate)
	a := e.connect(t, 0xa)
	a.loginAs("alice", tokens.device("alice", testOwner, "device-a"))

	answer := a.call(claimFrame())
	require.Equal(t, &pb.Slot{Label: "device-a", ClaimedAtMs: uint64(t0.UnixMilli())}, answer.GetSlot(), "answer %v", answer)

	e.clock.Advance(time.Minute)
	answer = a.call(claimFrame())
	require.Equal(t, uint64(t0.UnixMilli()), answer.GetSlot().GetClaimedAtMs(), "claiming again keeps the time")
}

func TestClaimSlotReplacesTheOtherKey(t *testing.T) {
	tokens, gate := withTokens(t)
	e := start(t, gate)
	a := e.connect(t, 0xa)
	a.loginAs("alice", tokens.device("alice", testOwner, "device-a"))
	a.call(claimFrame())
	a.open(testDB, true, false)
	answer := a.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))
	require.NotNil(t, answer.GetCommitAck(), "answer %v", answer)

	b := e.connect(t, 0xb)
	b.loginAs("bob", tokens.device("bob", testOwner, "device-b"))
	requireError(t, b.call(openFrame(testDB, true, false, nil)), pb.ErrorCode_ERROR_CODE_SLOT_TAKEN)
	answer = b.call(claimFrame())
	require.Equal(t, "device-b", answer.GetSlot().GetLabel(), "answer %v", answer)
	require.Equal(t, "device-a", answer.GetSlot().GetReplacedLabel())

	push := a.recv()
	require.Equal(t, uint64(0), push.GetRequestId())
	require.Equal(t, testDB, push.GetLeaseRevoked().GetDbId(), "push %v", push)
	requireError(t, a.call(commitFrame(1, 1, 1, commitIDOf(2), block(0, 0xa1))), pb.ErrorCode_ERROR_CODE_FENCED)

	again := e.connect(t, 0xa)
	again.loginAs("alice", tokens.device("alice", testOwner, "device-a"))
	requireError(t, again.call(openFrame(testDB, false, false, nil)), pb.ErrorCode_ERROR_CODE_SLOT_TAKEN)

	opened := b.open(testDB, true, false)
	require.Zero(t, opened.GetPageCount(), "the new key starts empty")
}

// Without the LeaseRevoked push, e.g. when the old key is connected to another server instance, its next request on
// the deleted database fails with FENCED.
func TestReplacedKeyOnAnotherInstanceIsFenced(t *testing.T) {
	tokens, gate := withTokens(t)
	shared := memory.New()
	useShared := func(o *protocol.Options) { o.Store = shared }
	e1 := start(t, gate, useShared)
	e2 := start(t, gate, useShared)

	a := e1.connect(t, 0xa)
	a.loginAs("alice", tokens.device("alice", testOwner, "device-a"))
	a.call(claimFrame())
	a.open(testDB, true, false)

	b := e2.connect(t, 0xb)
	b.loginAs("bob", tokens.device("bob", testOwner, "device-b"))
	require.NotNil(t, b.call(claimFrame()).GetSlot())

	requireError(t, a.call(fetchFrame(0, 0, 1)), pb.ErrorCode_ERROR_CODE_FENCED)
	requireError(t, a.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0))), pb.ErrorCode_ERROR_CODE_FENCED)
}

func TestDeleteSlotRemovesSlotAndDatabases(t *testing.T) {
	tokens, gate := withTokens(t)
	e := start(t, gate)
	a := e.connect(t, 0xa)
	a.loginAs("alice", tokens.device("alice", testOwner, "device-a"))
	a.call(claimFrame())
	opened := a.open(testDB, true, false)
	requireError(t, a.call(deleteSlotFrame()), pb.ErrorCode_ERROR_CODE_BAD_REQUEST)

	b := e.connect(t, 0xb)
	b.loginAs("bob", tokens.device("bob", testOwner, "device-b"))
	requireError(t, b.call(deleteSlotFrame()), pb.ErrorCode_ERROR_CODE_SLOT_TAKEN)

	a.call(&pb.ClientFrame{Body: &pb.ClientFrame_CloseDb{CloseDb: &pb.CloseDb{
		DbId: testDB, LeaseEpoch: opened.GetLeaseEpoch(),
	}}})
	require.NotNil(t, a.call(deleteSlotFrame()).GetOk())
	_, body := getSlot(t, e, tokens.unbound(testOwner), nil)
	require.Nil(t, body.Slot)
	requireError(t, a.call(openFrame(testDB, false, false, nil)), pb.ErrorCode_ERROR_CODE_NOT_FOUND)
}

func TestSlotEndpoint(t *testing.T) {
	tokens, gate := withTokens(t)
	e := start(t, gate, func(o *protocol.Options) { o.AllowedOrigins = []string{"client.example"} })

	res, body := getSlot(t, e, tokens.unbound(testOwner), nil)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "no-store", res.Header.Get("Cache-Control"))
	require.Nil(t, body.Slot)

	a := e.connect(t, 0xa)
	a.loginAs("alice", tokens.device("alice", testOwner, "device-a"))
	a.call(claimFrame())
	_, body = getSlot(t, e, tokens.unbound(testOwner), nil)
	require.Equal(t, &protocol.SlotInfo{Label: "device-a", ClaimedAtMs: t0.UnixMilli()}, body.Slot)
	_, body = getSlot(t, e, tokens.device("alice", testOwner, "device-a"), nil)
	require.NotNil(t, body.Slot, "a bound token works too")
	_, body = getSlot(t, e, tokens.unbound("owner-2"), nil)
	require.Nil(t, body.Slot, "other owners see only their own slot")

	t.Run("token required", func(t *testing.T) {
		res, _ := getSlot(t, e, "", nil)
		require.Equal(t, http.StatusUnauthorized, res.StatusCode)
		res, _ = getSlot(t, e, tokens.sign(jwt.MapClaims{"exp": t0.Add(time.Hour).Unix()}), nil)
		require.Equal(t, http.StatusUnauthorized, res.StatusCode, "a token without sub")
	})

	t.Run("browsers", func(t *testing.T) {
		res, _ := getSlot(t, e, tokens.unbound(testOwner), http.Header{"Origin": {"https://client.example"}})
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, "https://client.example", res.Header.Get("Access-Control-Allow-Origin"))

		res, _ = getSlot(t, e, tokens.unbound(testOwner), http.Header{"Origin": {"https://somewhere.else"}})
		require.Equal(t, http.StatusForbidden, res.StatusCode)
	})

	t.Run("preflight", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(e.server.ServeSlot))
		defer srv.Close()
		req, err := http.NewRequest(http.MethodOptions, srv.URL+protocol.SlotPath, nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "https://client.example")
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "authorization")
		res, err := srv.Client().Do(req)
		require.NoError(t, err)
		_ = res.Body.Close()
		require.Equal(t, http.StatusNoContent, res.StatusCode)
		require.Equal(t, "https://client.example", res.Header.Get("Access-Control-Allow-Origin"))
		require.Equal(t, "Authorization", res.Header.Get("Access-Control-Allow-Headers"))
	})
}
