package protocol_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
)

// goldenDir holds the golden files. sqlite-remote-vfs writes them (crates/sqlite-remote-protocol/tests/golden.rs), and
// scripts/sync-proto.sh copies them here. The tests build the same messages in Go. Decoding a file must give the
// sample, and encoding the sample must give the file byte for byte.
const goldenDir = "../../proto/testdata/v1"

const (
	dbID   = "keystore"
	timeMs = 1_758_550_000_000
)

var (
	instanceID       = bytes.Repeat([]byte{0x01}, 16)
	leaseID          = bytes.Repeat([]byte{0xaa}, 16)
	previousCommitID = bytes.Repeat([]byte{0xbb}, 16)
	commitID         = bytes.Repeat([]byte{0xcc}, 16)
	publicKey        = bytes.Repeat([]byte{0x7e}, 32)
	nonce            = bytes.Repeat([]byte{0x5c}, 32)
	signature        = bytes.Repeat([]byte{0x3f}, 64)
)

type sample struct {
	name string
	msg  proto.Message
}

func samples() []sample {
	return []sample{
		{"client_hello", &pb.ClientFrame{RequestId: 1, Body: &pb.ClientFrame_Hello{Hello: &pb.Hello{
			ProtocolVersion: 1, InstanceId: instanceID,
			SigAlg: pb.SigAlg_SIG_ALG_ED25519, PublicKey: publicKey,
		}}}},
		{"server_challenge", &pb.ServerFrame{RequestId: 1, Body: &pb.ServerFrame_Challenge{Challenge: &pb.Challenge{
			Nonce: nonce, ServerId: "wss://vfs.example/v1/ws", ExpiresAtMs: 1_758_550_030_000,
		}}}},
		{"client_proof", &pb.ClientFrame{RequestId: 2, Body: &pb.ClientFrame_Proof{Proof: &pb.Proof{
			Signature: signature,
		}}}},
		{"server_hello_ok", &pb.ServerFrame{RequestId: 1, Body: &pb.ServerFrame_HelloOk{HelloOk: &pb.HelloOk{
			ProtocolVersion: 1, MaxFrameBytes: 1_048_576, PingIntervalMs: 10_000, LeaseTtlMs: 30_000,
		}}}},
		{"client_open", &pb.ClientFrame{RequestId: 2, Body: &pb.ClientFrame_Open{Open: &pb.Open{
			DbId: dbID, PageSize: 4096, CreateIfMissing: true,
		}}}},
		{"client_open_resume", &pb.ClientFrame{RequestId: 2, Body: &pb.ClientFrame_Open{Open: &pb.Open{
			DbId: dbID, PageSize: 4096, Takeover: true,
			Resume: &pb.Resume{LeaseId: leaseID, LeaseEpoch: 3, KnownVersion: 41, PendingCommitId: commitID},
		}}}},
		{"server_opened", &pb.ServerFrame{RequestId: 2, Body: &pb.ServerFrame_Opened{Opened: &pb.Opened{
			LeaseId: leaseID, LeaseEpoch: 3, Version: 41, PageSize: 4096, PageCount: 12, LastCommitId: previousCommitID,
		}}}},
		{"client_fetch", &pb.ClientFrame{RequestId: 3, Body: &pb.ClientFrame_Fetch{Fetch: &pb.Fetch{
			DbId: dbID, Version: 41, FirstBlock: 0, Count: 12,
		}}}},
		{"server_pages", &pb.ServerFrame{RequestId: 3, Body: &pb.ServerFrame_Pages{Pages: &pb.Pages{
			FirstBlock: 10, Blocks: [][]byte{bytes.Repeat([]byte{0x10}, 8), bytes.Repeat([]byte{0x11}, 8)}, Last: true,
		}}}},
		{"client_fetch_ranges", &pb.ClientFrame{RequestId: 3, Body: &pb.ClientFrame_Fetch{Fetch: &pb.Fetch{
			DbId: dbID, Version: 41,
			Ranges: []*pb.Range{{FirstBlock: 0, Count: 2}, {FirstBlock: 9, Count: 1}},
		}}}},
		{"client_changed", &pb.ClientFrame{RequestId: 3, Body: &pb.ClientFrame_Changed{Changed: &pb.Changed{
			DbId: dbID, FromVersion: 38,
		}}}},
		{"server_changes", &pb.ServerFrame{RequestId: 3, Body: &pb.ServerFrame_Changes{Changes: &pb.Changes{
			FromVersion: 38, ToVersion: 41, Blocks: []uint64{0, 3, 4, 9}, Complete: true,
		}}}},
		{"client_commit", &pb.ClientFrame{RequestId: 4, Body: &pb.ClientFrame_Commit{Commit: &pb.Commit{
			DbId: dbID, CommitId: commitID, LeaseEpoch: 3, BaseVersion: 41, PageCount: 13,
			Blocks: []*pb.Block{
				{Index: 0, Data: bytes.Repeat([]byte{0x20}, 8)},
				{Index: 12, Data: bytes.Repeat([]byte{0x21}, 8)},
			},
			More: true,
			Part: 1,
		}}}},
		{"server_commit_ack", &pb.ServerFrame{RequestId: 4, Body: &pb.ServerFrame_CommitAck{CommitAck: &pb.CommitAck{
			CommitId: commitID, Version: 42,
		}}}},
		{"client_ping", &pb.ClientFrame{RequestId: 5, Body: &pb.ClientFrame_Ping{Ping: &pb.Ping{ClientTimeMs: timeMs}}}},
		{"server_pong", &pb.ServerFrame{RequestId: 5, Body: &pb.ServerFrame_Pong{Pong: &pb.Pong{ClientTimeMs: timeMs}}}},
		{"client_close_db", &pb.ClientFrame{RequestId: 6, Body: &pb.ClientFrame_CloseDb{CloseDb: &pb.CloseDb{
			DbId: dbID, LeaseEpoch: 3,
		}}}},
		{"server_ok", &pb.ServerFrame{RequestId: 6, Body: &pb.ServerFrame_Ok{Ok: &pb.Ok{}}}},
		{"server_lease_revoked", &pb.ServerFrame{RequestId: 0, Body: &pb.ServerFrame_LeaseRevoked{
			LeaseRevoked: &pb.LeaseRevoked{DbId: dbID, NewLeaseEpoch: 4},
		}}},
		{"server_error_fenced", &pb.ServerFrame{RequestId: 7, Body: &pb.ServerFrame_Error{Error: &pb.Error{
			Code: pb.ErrorCode_ERROR_CODE_FENCED, Detail: "lease epoch 3 is stale", CurrentVersion: 42,
		}}}},
		{"server_error_lease_held", &pb.ServerFrame{RequestId: 2, Body: &pb.ServerFrame_Error{Error: &pb.Error{
			Code: pb.ErrorCode_ERROR_CODE_LEASE_HELD, Detail: "another instance holds the lease", LeaseHolderSinceMs: timeMs,
		}}}},
	}
}

func TestGoldenFilesRoundTrip(t *testing.T) {
	for _, s := range samples() {
		t.Run(s.name, func(t *testing.T) {
			golden, err := os.ReadFile(filepath.Join(goldenDir, s.name+".binpb"))
			require.NoError(t, err)

			decoded := s.msg.ProtoReflect().New().Interface()
			require.NoError(t, proto.Unmarshal(golden, decoded))
			require.Truef(t, proto.Equal(s.msg, decoded), "decoded %v, want %v", decoded, s.msg)

			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(s.msg)
			require.NoError(t, err)
			require.Equal(t, golden, encoded, "Go encodes the sample differently")
		})
	}
}

func TestGoldenDirectoryMatchesSamples(t *testing.T) {
	entries, err := os.ReadDir(goldenDir)
	require.NoError(t, err)
	var files []string
	for _, entry := range entries {
		files = append(files, entry.Name())
	}
	var want []string
	for _, s := range samples() {
		want = append(want, s.name+".binpb")
	}
	sort.Strings(files)
	sort.Strings(want)
	require.Equal(t, want, files)
}
