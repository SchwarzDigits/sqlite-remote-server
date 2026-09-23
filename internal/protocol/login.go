package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"time"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

const (
	nonceBytes = 32
	// defaultChallengeTTL is how long a challenge is valid. It allows for slow connections and limits the time
	// in which an intercepted challenge could be used.
	defaultChallengeTTL = 30 * time.Second
	// transcriptLabel separates login signatures from signatures the same key makes for other purposes.
	transcriptLabel = "sqlite-remote-auth-v1"
	// subjectLabel separates the subject hash from other hashes of the same key.
	subjectLabel = "sqlite-remote-subject-v1"
)

// newNonce returns a random nonce for a challenge.
func newNonce() ([]byte, error) {
	nonce := make([]byte, nonceBytes)
	_, err := rand.Read(nonce)
	return nonce, err
}

// transcript returns the bytes the client signs at login. Each part is prefixed with its length as a 4-byte
// big-endian integer, so two different logins cannot produce the same bytes.
//
// The transcript contains the server ID, because a browser cannot bind a signature to the TLS connection. It binds
// the signature to this server only if the client compares the server ID with the URL it connected to. Without that
// check, a server the client connects to can relay this server's challenge. sqlite-remote-vfs does not check it and
// relies on a separate key for each server.
func transcript(serverID string, nonce, instanceID []byte, alg pb.SigAlg, publicKey []byte) []byte {
	var out []byte
	for _, part := range [][]byte{
		[]byte(transcriptLabel),
		[]byte(serverID),
		nonce,
		instanceID,
		binary.BigEndian.AppendUint32(nil, uint32(alg)),
		publicKey,
	} {
		out = binary.BigEndian.AppendUint32(out, uint32(len(part)))
		out = append(out, part...)
	}
	return out
}

// subjectOf returns the subject for a public key: the unpadded base64url encoding of a SHA-256 hash over the
// algorithm and the key.
//
// The hash gives every subject the same length regardless of the algorithm. Including the algorithm keeps the
// subjects of different algorithms apart.
func subjectOf(alg pb.SigAlg, publicKey []byte) string {
	sum := sha256.Sum256(transcript(subjectLabel, nil, nil, alg, publicKey))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// checkProof verifies the signature of a Proof over the login transcript.
func checkProof(alg pb.SigAlg, publicKey, signed, signature []byte) error {
	switch alg {
	case pb.SigAlg_SIG_ALG_ED25519:
		if len(publicKey) != ed25519.PublicKeySize {
			return unauthenticated("ed25519 public key must have %d bytes, got %d",
				ed25519.PublicKeySize, len(publicKey))
		}
		if !ed25519.Verify(publicKey, signed, signature) {
			return unauthenticated("signature does not match the challenge")
		}
		return nil
	default:
		return store.BadRequest("signature algorithm %s is not supported", alg)
	}
}
