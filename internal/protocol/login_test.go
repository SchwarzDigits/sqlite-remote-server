package protocol_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/protocol"
)

const testServerID = "wss://vfs.test/v1/ws"

// transcript builds the login transcript independently of the server code. A test that called the server's function
// would not detect a mistake in it.
func transcript(serverID string, nonce, instance []byte, alg pb.SigAlg, publicKey []byte) []byte {
	var out []byte
	for _, part := range [][]byte{
		[]byte("sqlite-remote-auth-v1"),
		[]byte(serverID),
		nonce,
		instance,
		binary.BigEndian.AppendUint32(nil, uint32(alg)),
		publicKey,
	} {
		out = binary.BigEndian.AppendUint32(out, uint32(len(part)))
		out = append(out, part...)
	}
	return out
}

// keyFor returns a key pair derived from name. The same name always gives the same key pair.
func keyFor(name string) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte("sqlite-remote-server test key: " + name))
	private := ed25519.NewKeyFromSeed(seed[:])
	return private.Public().(ed25519.PublicKey), private
}

func keyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return public, private
}

// challenge sends a Hello with public and returns the server's Challenge.
func (c *client) challenge(public ed25519.PublicKey) *pb.Challenge {
	c.t.Helper()
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Hello{Hello: &pb.Hello{
		ProtocolVersion: protocol.Version, InstanceId: c.instance,
		SigAlg: pb.SigAlg_SIG_ALG_ED25519, PublicKey: public,
	}}})
	require.NotNil(c.t, answer.GetChallenge(), "answer %v", answer)
	return answer.GetChallenge()
}

// login runs the full login and returns the server's response to the Proof.
func (c *client) login(public ed25519.PublicKey, private ed25519.PrivateKey) *pb.ServerFrame {
	c.t.Helper()
	ch := c.challenge(public)
	signed := transcript(ch.GetServerId(), ch.GetNonce(), c.instance, pb.SigAlg_SIG_ALG_ED25519, public)
	return c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
		Signature: ed25519.Sign(private, signed),
	}}})
}

func TestLoginSubjectIsDerivedFromKey(t *testing.T) {
	e := start(t)
	public, private := keyPair(t)

	c := e.connect(t, 0xa)
	require.NotNil(t, c.login(public, private).GetHelloOk())
	c.open(testDB, true, false)
	c.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))

	// The same key on a new connection opens the same database.
	again := e.connect(t, 0xb)
	require.NotNil(t, again.login(public, private).GetHelloOk())
	opened := again.open(testDB, false, true)
	require.Equal(t, uint64(1), opened.GetVersion())
}

func TestDifferentKeyIsDifferentSubject(t *testing.T) {
	e := start(t)
	mine, minePrivate := keyPair(t)
	theirs, theirsPrivate := keyPair(t)

	c := e.connect(t, 0xa)
	c.login(mine, minePrivate)
	c.open(testDB, true, false)
	c.call(commitFrame(1, 0, 1, commitIDOf(1), block(0, 0xa0)))

	// Same database ID, different key: a different subject, where the database does not exist.
	other := e.connect(t, 0xb)
	other.login(theirs, theirsPrivate)
	answer := other.call(openFrame(testDB, false, false, nil))
	require.NotNil(t, answer.GetError(), "answer %v", answer)
	require.Equal(t, pb.ErrorCode_ERROR_CODE_NOT_FOUND, answer.GetError().GetCode())
}

func TestProofWithWrongKeyIsRejected(t *testing.T) {
	e := start(t)
	public, _ := keyPair(t)
	_, otherPrivate := keyPair(t)

	c := e.connect(t, 0xa)
	ch := c.challenge(public)
	signed := transcript(ch.GetServerId(), ch.GetNonce(), c.instance, pb.SigAlg_SIG_ALG_ED25519, public)
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
		Signature: ed25519.Sign(otherPrivate, signed),
	}}})
	require.Equal(t, pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, answer.GetError().GetCode(), "answer %v", answer)
}

func TestProofForOtherServerIsRejected(t *testing.T) {
	// A signature over a transcript that names another server ID is rejected.
	e := start(t)
	public, private := keyPair(t)

	c := e.connect(t, 0xa)
	ch := c.challenge(public)
	signed := transcript("wss://somewhere.else/v1/ws", ch.GetNonce(), c.instance, pb.SigAlg_SIG_ALG_ED25519, public)
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
		Signature: ed25519.Sign(private, signed),
	}}})
	require.Equal(t, pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, answer.GetError().GetCode(), "answer %v", answer)
}

func TestProofForOtherNonceIsRejected(t *testing.T) {
	// Every connection gets its own nonce, so a Proof for one connection is invalid on another.
	e := start(t)
	public, private := keyPair(t)

	first := e.connect(t, 0xa)
	stale := first.challenge(public)

	second := e.connect(t, 0xb)
	second.challenge(public)
	signed := transcript(testServerID, stale.GetNonce(), second.instance, pb.SigAlg_SIG_ALG_ED25519, public)
	answer := second.call(&pb.ClientFrame{Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
		Signature: ed25519.Sign(private, signed),
	}}})
	require.Equal(t, pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, answer.GetError().GetCode(), "answer %v", answer)
}

func TestExpiredChallengeIsRejected(t *testing.T) {
	e := start(t, func(o *protocol.Options) { o.ChallengeTTL = time.Minute })
	public, private := keyPair(t)

	c := e.connect(t, 0xa)
	ch := c.challenge(public)
	e.clock.Advance(2 * time.Minute)

	signed := transcript(ch.GetServerId(), ch.GetNonce(), c.instance, pb.SigAlg_SIG_ALG_ED25519, public)
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
		Signature: ed25519.Sign(private, signed),
	}}})
	require.Equal(t, pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, answer.GetError().GetCode(), "answer %v", answer)
}

func TestHelloWithoutKeyIsRejected(t *testing.T) {
	// A client cannot choose its subject. The only remaining way to skip the proof is a Hello without a public key.
	e := start(t)
	c := e.connect(t, 0xa)
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Hello{Hello: &pb.Hello{
		ProtocolVersion: protocol.Version, InstanceId: c.instance,
	}}})
	require.Equal(t, pb.ErrorCode_ERROR_CODE_UNAUTHENTICATED, answer.GetError().GetCode(), "answer %v", answer)
}

func TestEmptyServerIDRejectsLogin(t *testing.T) {
	// Without a server ID the transcript cannot bind the signature to the server. The server must reject the login
	// instead of accepting clients without a proof.
	e := start(t, func(o *protocol.Options) { o.ServerID = "" })
	public, _ := keyFor("alice")
	c := e.connect(t, 0xa)
	answer := c.call(&pb.ClientFrame{Body: &pb.ClientFrame_Hello{Hello: &pb.Hello{
		ProtocolVersion: protocol.Version, InstanceId: c.instance,
		SigAlg: pb.SigAlg_SIG_ALG_ED25519, PublicKey: public,
	}}})
	require.NotNil(t, answer.GetError(), "login must fail: %v", answer)
	require.Nil(t, answer.GetHelloOk())
}
