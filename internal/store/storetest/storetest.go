// Package storetest contains the contract tests that every store.Store implementation must pass.
package storetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

const (
	pageSize = 512
	ttl      = 30 * time.Second
)

var (
	t0        = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	instanceA = []byte("instance-a")
	instanceB = []byte("instance-b")
	commit1   = bytes.Repeat([]byte{0x01}, 16)
	commit2   = bytes.Repeat([]byte{0x02}, 16)
)

// Run runs every contract test against a new store from newStore.
func Run(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, s store.Store)
	}{
		{"OpenCreatesEmptyDatabase", openCreatesEmptyDatabase},
		{"OpenWithoutCreateRequiresDatabase", openWithoutCreateRequiresDatabase},
		{"OpenRejectsInvalidPageSizes", openRejectsInvalidPageSizes},
		{"CommitThenFetch", commitThenFetch},
		{"RepeatedCommitReturnsSameVersion", repeatedCommitReturnsSameVersion},
		{"StaleBaseVersionConflicts", staleBaseVersionConflicts},
		{"InvalidCommitChangesNothing", invalidCommitChangesNothing},
		{"CommitTruncates", commitTruncates},
		{"FetchChecksVersionAndRange", fetchChecksVersionAndRange},
		{"ActiveLeaseBlocksOpen", activeLeaseBlocksOpen},
		{"TakeoverFencesOldLease", takeoverFencesOldLease},
		{"ExpiredLeaseNeedsNoTakeover", expiredLeaseNeedsNoTakeover},
		{"ExpiredUntakenLeaseStillCommits", expiredUntakenLeaseStillCommits},
		{"RenewExtendsLease", renewExtendsLease},
		{"ResumeKeepsLease", resumeKeepsLease},
		{"ResumeAfterTakeoverIsFenced", resumeAfterTakeoverIsFenced},
		{"ReleaseFreesLease", releaseFreesLease},
		{"ChangesListChangedBlocks", changesListChangedBlocks},
		{"ChangesBeyondWindowAreIncomplete", changesBeyondWindowAreIncomplete},
		{"ChangesRejectFutureVersion", changesRejectFutureVersion},
		{"SubjectsAreSeparate", subjectsAreSeparate},
		{"BlocksAreCopies", blocksAreCopies},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newStore(t))
		})
	}
}

// run is a random ID for this test run. A persistent store such as PostgreSQL is shared between subtests and between
// runs. Each subtest uses subjects that contain the test name and this ID, so no cleanup is needed.
var run = runID()

func runID() string {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(id[:])
}

func subject(t *testing.T, name string) string {
	t.Helper()
	return name + "-" + t.Name() + "-" + run
}

func key(t *testing.T, db string) store.Key {
	t.Helper()
	return store.Key{Subject: subject(t, "alice"), DBID: db}
}

func block(index uint64, fill byte) store.Block {
	return store.Block{Index: index, Data: bytes.Repeat([]byte{fill}, pageSize)}
}

func create(t *testing.T, s store.Store, k store.Key, instance []byte, now time.Time) store.OpenResult {
	t.Helper()
	res, err := s.Open(context.Background(), store.OpenRequest{
		Key: k, InstanceID: instance, PageSize: pageSize, Create: true, Now: now, TTL: ttl,
	})
	require.NoError(t, err)
	return res
}

func open(s store.Store, k store.Key, instance []byte, takeover bool, now time.Time) (store.OpenResult, error) {
	return s.Open(context.Background(), store.OpenRequest{
		Key: k, InstanceID: instance, Takeover: takeover, Now: now, TTL: ttl,
	})
}

func commit(s store.Store, k store.Key, epoch, base, pageCount uint64, id []byte, now time.Time, blocks ...store.Block) (uint64, error) {
	return s.Commit(context.Background(), store.CommitRequest{
		Key: k, CommitID: id, LeaseEpoch: epoch, BaseVersion: base, PageCount: pageCount, Blocks: blocks, Now: now, TTL: ttl,
	})
}

func requireFenced(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, store.ErrFenced)
}

func requireBadRequest(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	var bad *store.BadRequestError
	require.ErrorAs(t, err, &bad, msgAndArgs...)
}

func openCreatesEmptyDatabase(t *testing.T, s store.Store) {
	res := create(t, s, key(t, "db"), instanceA, t0)
	require.Equal(t, uint64(1), res.Lease.Epoch)
	require.NotEmpty(t, res.Lease.ID)
	require.Equal(t, store.State{PageSize: pageSize}, res.State)
	require.False(t, res.Revoked)
}

func openWithoutCreateRequiresDatabase(t *testing.T, s store.Store) {
	_, err := open(s, key(t, "missing"), instanceA, false, t0)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func openRejectsInvalidPageSizes(t *testing.T, s store.Store) {
	for _, size := range []uint32{0, 256, 1000, 131072} {
		_, err := s.Open(context.Background(), store.OpenRequest{
			Key: key(t, "db"), InstanceID: instanceA, PageSize: size, Create: true, Now: t0, TTL: ttl,
		})
		requireBadRequest(t, err)
	}
	create(t, s, key(t, "db"), instanceA, t0)
	_, err := s.Open(context.Background(), store.OpenRequest{
		Key: key(t, "db"), InstanceID: instanceA, PageSize: 4096, Takeover: true, Now: t0, TTL: ttl,
	})
	requireBadRequest(t, err)
}

func commitThenFetch(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	version, err := commit(s, k, lease.Epoch, 0, 3, commit1, t0, block(0, 0xa0), block(2, 0xa2))
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	blocks, err := s.Fetch(context.Background(), k, 1, 0, 3)
	require.NoError(t, err)
	require.Equal(t, [][]byte{block(0, 0xa0).Data, make([]byte, pageSize), block(2, 0xa2).Data}, blocks,
		"block 1 was never written and must read as zeros")

	res, err := open(s, k, instanceA, true, t0)
	require.NoError(t, err)
	require.Equal(t, store.State{PageSize: pageSize, PageCount: 3, Version: 1, LastCommitID: commit1}, res.State)
}

func repeatedCommitReturnsSameVersion(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	for range 2 {
		version, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
		require.NoError(t, err)
		require.Equal(t, uint64(1), version)
	}
}

func staleBaseVersionConflicts(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	require.NoError(t, err)

	_, err = commit(s, k, lease.Epoch, 0, 1, commit2, t0, block(0, 0xb0))
	var conflict *store.VersionConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, uint64(1), conflict.Current)
}

func invalidCommitChangesNothing(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	short := store.Block{Index: 0, Data: []byte{1, 2, 3}}
	_, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, short)
	requireBadRequest(t, err)
	_, err = commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(1, 0xa1))
	requireBadRequest(t, err, "block index beyond page count")
	_, err = commit(s, k, lease.Epoch, 0, 1, nil, t0, block(0, 0xa0))
	requireBadRequest(t, err, "empty commit id")
	_, err = commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0), block(0, 0xa1))
	requireBadRequest(t, err, "same block twice in one commit")

	res, err := open(s, k, instanceA, true, t0)
	require.NoError(t, err)
	require.Equal(t, store.State{PageSize: pageSize}, res.State)
}

func commitTruncates(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 3, commit1, t0, block(0, 0xa0), block(1, 0xa1), block(2, 0xa2))
	require.NoError(t, err)
	_, err = commit(s, k, lease.Epoch, 1, 1, commit2, t0)
	require.NoError(t, err)

	blocks, err := s.Fetch(context.Background(), k, 2, 0, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{block(0, 0xa0).Data}, blocks)
	_, err = s.Fetch(context.Background(), k, 2, 0, 3)
	requireBadRequest(t, err)
}

func fetchChecksVersionAndRange(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 2, commit1, t0, block(0, 0xa0), block(1, 0xa1))
	require.NoError(t, err)

	_, err = s.Fetch(context.Background(), k, 0, 0, 2)
	var conflict *store.VersionConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, uint64(1), conflict.Current)

	_, err = s.Fetch(context.Background(), k, 1, 1, 2)
	requireBadRequest(t, err)
	_, err = s.Fetch(context.Background(), k, 1, 3, 0)
	requireBadRequest(t, err)

	blocks, err := s.Fetch(context.Background(), k, 1, 2, 0)
	require.NoError(t, err)
	require.Empty(t, blocks)

	_, err = s.Fetch(context.Background(), key(t, "missing"), 0, 0, 0)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func activeLeaseBlocksOpen(t *testing.T, s store.Store) {
	k := key(t, "db")
	create(t, s, k, instanceA, t0)
	_, err := open(s, k, instanceB, false, t0.Add(ttl-time.Second))
	var held *store.LeaseHeldError
	require.ErrorAs(t, err, &held)
	require.True(t, held.Since.Equal(t0))
}

func takeoverFencesOldLease(t *testing.T, s store.Store) {
	k := key(t, "db")
	old := create(t, s, k, instanceA, t0).Lease
	res, err := open(s, k, instanceB, true, t0.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, old.Epoch+1, res.Lease.Epoch)
	require.True(t, res.Revoked)

	_, err = commit(s, k, old.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	requireFenced(t, err)
	requireFenced(t, s.Renew(context.Background(), k, old.Epoch, t0, ttl))
	requireFenced(t, s.Release(context.Background(), k, old.Epoch))

	_, err = commit(s, k, res.Lease.Epoch, 0, 1, commit1, t0, block(0, 0xb0))
	require.NoError(t, err)
}

func expiredLeaseNeedsNoTakeover(t *testing.T, s store.Store) {
	k := key(t, "db")
	old := create(t, s, k, instanceA, t0).Lease
	res, err := open(s, k, instanceB, false, t0.Add(ttl))
	require.NoError(t, err)
	require.Equal(t, old.Epoch+1, res.Lease.Epoch)
	require.False(t, res.Revoked, "the old lease had expired, so nothing was revoked")

	_, err = commit(s, k, old.Epoch, 0, 1, commit1, t0.Add(ttl), block(0, 0xa0))
	requireFenced(t, err)
}

func expiredUntakenLeaseStillCommits(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	later := t0.Add(10 * ttl)
	_, err := commit(s, k, lease.Epoch, 0, 1, commit1, later, block(0, 0xa0))
	require.NoError(t, err)

	_, err = open(s, k, instanceB, false, later.Add(ttl-time.Second))
	var held *store.LeaseHeldError
	require.ErrorAs(t, err, &held, "the commit must have renewed the lease")
}

func renewExtendsLease(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	require.NoError(t, s.Renew(context.Background(), k, lease.Epoch, t0.Add(25*time.Second), ttl))

	_, err := open(s, k, instanceB, false, t0.Add(50*time.Second))
	var held *store.LeaseHeldError
	require.ErrorAs(t, err, &held)
}

func resumeKeepsLease(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	require.NoError(t, err)

	res, err := s.Open(context.Background(), store.OpenRequest{
		Key: k, InstanceID: instanceA, Resume: &store.Resume{LeaseID: lease.ID, LeaseEpoch: lease.Epoch},
		Now: t0.Add(5 * ttl), TTL: ttl,
	})
	require.NoError(t, err)
	require.Equal(t, lease, res.Lease)
	require.Equal(t, uint64(1), res.State.Version)
	require.Equal(t, commit1, res.State.LastCommitID)
}

func resumeAfterTakeoverIsFenced(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := open(s, k, instanceB, true, t0)
	require.NoError(t, err)

	_, err = s.Open(context.Background(), store.OpenRequest{
		Key: k, InstanceID: instanceA, Resume: &store.Resume{LeaseID: lease.ID, LeaseEpoch: lease.Epoch},
		Now: t0, TTL: ttl,
	})
	requireFenced(t, err)
}

func releaseFreesLease(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	require.NoError(t, s.Release(context.Background(), k, lease.Epoch))

	res, err := open(s, k, instanceB, false, t0)
	require.NoError(t, err)
	require.Equal(t, lease.Epoch+1, res.Lease.Epoch)
	require.False(t, res.Revoked)

	_, err = commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	requireFenced(t, err)
}

func subjectsAreSeparate(t *testing.T, s store.Store) {
	alice := key(t, "db")
	bob := store.Key{Subject: subject(t, "bob"), DBID: "db"}
	lease := create(t, s, alice, instanceA, t0).Lease
	_, err := commit(s, alice, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	require.NoError(t, err)

	_, err = open(s, bob, instanceB, false, t0)
	require.ErrorIs(t, err, store.ErrNotFound)
	res := create(t, s, bob, instanceB, t0)
	require.Equal(t, uint64(0), res.State.Version)
}

func blocksAreCopies(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	written := block(0, 0xa0)
	_, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, written)
	require.NoError(t, err)
	written.Data[0] = 0xff

	fetched, err := s.Fetch(context.Background(), k, 1, 0, 1)
	require.NoError(t, err)
	require.Equal(t, byte(0xa0), fetched[0][0], "the store must copy committed blocks")
	fetched[0][0] = 0xff

	again, err := s.Fetch(context.Background(), k, 1, 0, 1)
	require.NoError(t, err)
	require.Equal(t, byte(0xa0), again[0][0], "the store must return copies of its blocks")
}

func changesListChangedBlocks(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 8, commit1, t0, block(0, 0xa0), block(1, 0xa1))
	require.NoError(t, err)
	_, err = commit(s, k, lease.Epoch, 1, 8, []byte("commit-2........"), t0, block(4, 0xb4))
	require.NoError(t, err)
	_, err = commit(s, k, lease.Epoch, 2, 8, []byte("commit-3........"), t0, block(1, 0xc1), block(7, 0xc7))
	require.NoError(t, err)

	// From version 0: every changed block, sorted and without duplicates.
	set, err := s.Changes(context.Background(), k, 0)
	require.NoError(t, err)
	require.True(t, set.Complete)
	require.Equal(t, uint64(3), set.ToVersion)
	require.Equal(t, []uint64{0, 1, 4, 7}, set.Blocks)

	// From version 1: only blocks changed after version 1. Block 0 is unchanged since version 1.
	set, err = s.Changes(context.Background(), k, 1)
	require.NoError(t, err)
	require.True(t, set.Complete)
	require.Equal(t, []uint64{1, 4, 7}, set.Blocks)

	// From the current version: no changes.
	set, err = s.Changes(context.Background(), k, 3)
	require.NoError(t, err)
	require.True(t, set.Complete)
	require.Empty(t, set.Blocks)
}

func changesBeyondWindowAreIncomplete(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	id := make([]byte, 16)
	for n := range uint64(store.ChangeWindow + 2) {
		binary.BigEndian.PutUint64(id, n)
		_, err := commit(s, k, lease.Epoch, n, 4, id, t0, block(n%4, byte(n)))
		require.NoError(t, err)
	}

	// The two oldest commits are no longer in the change log, so version 1 cannot be caught up.
	set, err := s.Changes(context.Background(), k, 1)
	require.NoError(t, err)
	require.False(t, set.Complete, "the change log no longer reaches back to version 1")
	require.Empty(t, set.Blocks, "an incomplete change set must list no blocks")

	// A version inside the window can be caught up.
	set, err = s.Changes(context.Background(), k, store.ChangeWindow+1)
	require.NoError(t, err)
	require.True(t, set.Complete)
	require.Equal(t, []uint64{(store.ChangeWindow + 1) % 4}, set.Blocks)
}

func changesRejectFutureVersion(t *testing.T, s store.Store) {
	k := key(t, "db")
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	require.NoError(t, err)

	// A version ahead of the server means the server lost commits. Changes must return a bad request.
	_, err = s.Changes(context.Background(), k, 2)
	requireBadRequest(t, err)
}
