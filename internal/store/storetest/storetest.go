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
		{"SameInstanceTakesItsLeaseBack", sameInstanceTakesItsLeaseBack},
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
		{"DeleteRemovesDatabase", deleteRemovesDatabase},
		{"DeleteFencesEveryEarlierLease", deleteFencesEveryEarlierLease},
		{"ActiveLeaseBlocksDelete", activeLeaseBlocksDelete},
		{"DeleteIsIdempotent", deleteIsIdempotent},
		{"RecreatedDatabaseHasNoChangeLog", recreatedDatabaseHasNoChangeLog},
		{"RecreatedDatabaseMayChangePageSize", recreatedDatabaseMayChangePageSize},
		{"DeleteUnusedKeepsUsedDatabases", deleteUnusedKeepsUsedDatabases},
		{"DeleteUnusedRespectsLimit", deleteUnusedRespectsLimit},
		{"ClaimFreeSlot", claimFreeSlot},
		{"ReclaimKeepsClaimTime", reclaimKeepsClaimTime},
		{"ClaimReplacesOtherKey", claimReplacesOtherKey},
		{"SlotBlocksOwnersOtherKeys", slotBlocksOwnersOtherKeys},
		{"DeleteSlotPurgesHolder", deleteSlotPurgesHolder},
		{"DeleteUnusedReleasesSlots", deleteUnusedReleasesSlots},
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

// A client that restarts with the same instance ID gets a new lease without takeover. Its earlier lease is fenced.
// Another instance is still refused.
func sameInstanceTakesItsLeaseBack(t *testing.T, s store.Store) {
	k := key(t, "db")
	old := create(t, s, k, instanceA, t0).Lease

	res, err := open(s, k, instanceA, false, t0.Add(time.Second))
	require.NoError(t, err, "the holder's own instance needs no takeover")
	require.Greater(t, res.Lease.Epoch, old.Epoch)
	require.True(t, res.Revoked, "the earlier lease had not expired")
	_, err = commit(s, k, old.Epoch, 0, 1, commit1, t0.Add(time.Second), block(0, 0xa0))
	requireFenced(t, err)
	_, err = commit(s, k, res.Lease.Epoch, 0, 1, commit1, t0.Add(time.Second), block(0, 0xa1))
	require.NoError(t, err)

	_, err = open(s, k, instanceB, false, t0.Add(2*time.Second))
	var held *store.LeaseHeldError
	require.ErrorAs(t, err, &held, "another instance still needs takeover")
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

func remove(s store.Store, k store.Key, takeover bool, now time.Time) (store.DeleteResult, error) {
	return s.Delete(context.Background(), store.DeleteRequest{Key: k, Takeover: takeover, Now: now})
}

// deleteAfterCommits creates a database with two commits, releases its lease and deletes it. It returns the lease
// of the deleted database and the version it had.
func deleteAfterCommits(t *testing.T, s store.Store, k store.Key) (store.Lease, uint64) {
	t.Helper()
	lease := create(t, s, k, instanceA, t0).Lease
	_, err := commit(s, k, lease.Epoch, 0, 2, commit1, t0, block(0, 0xa0), block(1, 0xa1))
	require.NoError(t, err)
	version, err := commit(s, k, lease.Epoch, 1, 2, commit2, t0, block(1, 0xa2))
	require.NoError(t, err)
	require.NoError(t, s.Release(context.Background(), k, lease.Epoch))
	res, err := remove(s, k, false, t0.Add(time.Second))
	require.NoError(t, err)
	require.Greater(t, res.Epoch, lease.Epoch, "deleting must increase the epoch")
	require.False(t, res.Revoked, "a released lease is not revoked")
	return lease, version
}

func deleteRemovesDatabase(t *testing.T, s store.Store) {
	k := key(t, "db")
	_, version := deleteAfterCommits(t, s, k)

	_, err := open(s, k, instanceA, false, t0.Add(2*time.Second))
	require.ErrorIs(t, err, store.ErrNotFound, "a deleted database is not found without create")
	_, err = s.Fetch(context.Background(), k, version, 0, 1)
	require.ErrorIs(t, err, store.ErrNotFound)
	_, err = s.Changes(context.Background(), k, 0)
	require.ErrorIs(t, err, store.ErrNotFound)

	res := create(t, s, k, instanceA, t0.Add(2*time.Second))
	require.Zero(t, res.State.PageCount, "a recreated database is empty")
	require.Empty(t, res.State.LastCommitID)
	require.Greater(t, res.State.Version, version, "the version continues after a deletion")
	blocks, err := s.Fetch(context.Background(), k, res.State.Version, 0, 0)
	require.NoError(t, err)
	require.Empty(t, blocks)
}

func deleteFencesEveryEarlierLease(t *testing.T, s store.Store) {
	k := key(t, "db")
	old := create(t, s, k, instanceA, t0).Lease
	res, err := remove(s, k, true, t0.Add(time.Second))
	require.NoError(t, err)
	require.True(t, res.Revoked, "deleting with takeover revokes an unexpired lease")

	_, err = commit(s, k, old.Epoch, 0, 1, commit1, t0, block(0, 0xa0))
	requireFenced(t, err)
	requireFenced(t, s.Renew(context.Background(), k, old.Epoch, t0, ttl))
	_, err = s.Open(context.Background(), store.OpenRequest{
		Key: k, InstanceID: instanceA, Resume: &store.Resume{LeaseID: old.ID, LeaseEpoch: old.Epoch}, Now: t0, TTL: ttl,
	})
	requireFenced(t, err)

	// After the database is created anew, the old lease must still not commit. Its epoch must not come back.
	recreated := create(t, s, k, instanceB, t0.Add(2*time.Second))
	require.Greater(t, recreated.Lease.Epoch, res.Epoch)
	_, err = commit(s, k, old.Epoch, recreated.State.Version, 1, commit1, t0, block(0, 0xa0))
	requireFenced(t, err)
	_, err = commit(s, k, recreated.Lease.Epoch, recreated.State.Version, 1, commit1, t0, block(0, 0xb0))
	require.NoError(t, err)
}

func activeLeaseBlocksDelete(t *testing.T, s store.Store) {
	k := key(t, "db")
	create(t, s, k, instanceA, t0)
	_, err := remove(s, k, false, t0.Add(time.Second))
	var held *store.LeaseHeldError
	require.ErrorAs(t, err, &held)

	res, err := remove(s, k, false, t0.Add(ttl+time.Second))
	require.NoError(t, err, "an expired lease does not block a deletion")
	require.False(t, res.Revoked)
}

func deleteIsIdempotent(t *testing.T, s store.Store) {
	res, err := remove(s, key(t, "missing"), false, t0)
	require.NoError(t, err)
	require.Zero(t, res.Epoch)

	k := key(t, "db")
	deleteAfterCommits(t, s, k)
	res, err = remove(s, k, false, t0.Add(2*time.Second))
	require.NoError(t, err, "deleting a deleted database succeeds")
	require.Zero(t, res.Epoch)
}

func recreatedDatabaseHasNoChangeLog(t *testing.T, s store.Store) {
	k := key(t, "db")
	_, version := deleteAfterCommits(t, s, k)
	res := create(t, s, k, instanceA, t0.Add(2*time.Second))

	// A local copy from before the deletion must not be caught up: the change log does not reach back to it.
	set, err := s.Changes(context.Background(), k, version)
	require.NoError(t, err)
	require.False(t, set.Complete)
	require.Equal(t, res.State.Version, set.ToVersion)
}

func recreatedDatabaseMayChangePageSize(t *testing.T, s store.Store) {
	k := key(t, "db")
	deleteAfterCommits(t, s, k)
	res, err := s.Open(context.Background(), store.OpenRequest{
		Key: k, InstanceID: instanceA, PageSize: 4 * pageSize, Create: true, Now: t0.Add(2 * time.Second), TTL: ttl,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(4*pageSize), res.State.PageSize)
}

// long ago is the time of the databases in the DeleteUnused tests. A persistent store is shared with the other tests
// and with earlier runs, whose databases are used around t0. A cutoff this far back reaches none of them.
var longAgo = t0.AddDate(-10, 0, 0)

// deleteUnused calls DeleteUnused until it returns nothing and returns the deleted keys of this test.
func deleteUnused(t *testing.T, s store.Store, cutoff time.Time) []store.Key {
	t.Helper()
	var mine []store.Key
	for {
		keys, err := s.DeleteUnused(context.Background(), cutoff, 100)
		require.NoError(t, err)
		if len(keys) == 0 {
			return mine
		}
		for _, k := range keys {
			if k.Subject == subject(t, "alice") {
				mine = append(mine, k)
			}
		}
	}
}

func deleteUnusedKeepsUsedDatabases(t *testing.T, s store.Store) {
	cutoff := longAgo.AddDate(0, 6, 0)

	// Released long before the cutoff.
	released := key(t, "released")
	lease := create(t, s, released, instanceA, longAgo).Lease
	version, err := commit(s, released, lease.Epoch, 0, 1, commit1, longAgo, block(0, 0xa0))
	require.NoError(t, err)
	require.NoError(t, s.Release(context.Background(), released, lease.Epoch))

	// Still held, but the lease expired long before the cutoff: the holder has not been seen since.
	abandoned := key(t, "abandoned")
	create(t, s, abandoned, instanceA, longAgo)

	// Renewed after the cutoff, e.g. by a device that only reads.
	renewed := key(t, "renewed")
	lease = create(t, s, renewed, instanceA, longAgo).Lease
	require.NoError(t, s.Renew(context.Background(), renewed, lease.Epoch, cutoff, ttl))

	// Committed after the cutoff.
	committed := key(t, "committed")
	lease = create(t, s, committed, instanceA, longAgo).Lease
	_, err = commit(s, committed, lease.Epoch, 0, 1, commit1, cutoff, block(0, 0xc0))
	require.NoError(t, err)

	require.ElementsMatch(t, []store.Key{released, abandoned}, deleteUnused(t, s, cutoff))
	require.Empty(t, deleteUnused(t, s, cutoff), "a deleted database is not deleted again")

	_, err = open(s, released, instanceA, false, t0)
	require.ErrorIs(t, err, store.ErrNotFound)
	recreated := create(t, s, released, instanceA, t0)
	require.Greater(t, recreated.State.Version, version, "the version continues after the deletion")
	require.Zero(t, recreated.State.PageCount)

	for _, k := range []store.Key{renewed, committed} {
		_, err := open(s, k, instanceB, true, t0)
		require.NoError(t, err, "%s was used after the cutoff", k.DBID)
	}
}

func deleteUnusedRespectsLimit(t *testing.T, s store.Store) {
	for _, name := range []string{"a", "b", "c"} {
		create(t, s, key(t, name), instanceA, longAgo)
	}
	cutoff := longAgo.AddDate(0, 6, 0)
	keys, err := s.DeleteUnused(context.Background(), cutoff, 1)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	mine := 0
	if keys[0].Subject == subject(t, "alice") {
		mine = 1
	}
	require.Len(t, deleteUnused(t, s, cutoff), 3-mine, "later calls delete the rest")
}

func owner(t *testing.T) string {
	t.Helper()
	return "owner-" + t.Name() + "-" + run
}

func claim(t *testing.T, s store.Store, owner, subject, label string, now time.Time) store.ClaimResult {
	t.Helper()
	res, err := s.ClaimSlot(context.Background(), store.ClaimRequest{Owner: owner, Subject: subject, Label: label, Now: now})
	require.NoError(t, err)
	return res
}

func openAs(s store.Store, owner string, k store.Key, create bool, now time.Time) (store.OpenResult, error) {
	return s.Open(context.Background(), store.OpenRequest{
		Key: k, Owner: owner, InstanceID: instanceA, PageSize: pageSize, Create: create, Now: now, TTL: ttl,
	})
}

func claimFreeSlot(t *testing.T, s store.Store) {
	o, alice := owner(t), subject(t, "alice")
	_, ok, err := s.GetSlot(context.Background(), o)
	require.NoError(t, err)
	require.False(t, ok)

	res := claim(t, s, o, alice, "device-1", t0)
	require.Nil(t, res.Replaced)
	require.Empty(t, res.Deleted)
	want := store.Slot{Subject: alice, Label: "device-1", ClaimedAt: t0}
	require.Equal(t, want.Subject, res.Slot.Subject)
	require.Equal(t, want.Label, res.Slot.Label)
	require.True(t, want.ClaimedAt.Equal(res.Slot.ClaimedAt))

	got, ok, err := s.GetSlot(context.Background(), o)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, alice, got.Subject)
	require.Equal(t, "device-1", got.Label)
	require.True(t, t0.Equal(got.ClaimedAt))
}

func reclaimKeepsClaimTime(t *testing.T, s store.Store) {
	o, alice := owner(t), subject(t, "alice")
	k := key(t, "db")
	claim(t, s, o, alice, "device-1", t0)
	create(t, s, k, instanceA, t0)

	res := claim(t, s, o, alice, "renamed", t0.Add(time.Hour))
	require.Nil(t, res.Replaced, "the slot did not change hands")
	require.Empty(t, res.Deleted)
	require.Equal(t, "renamed", res.Slot.Label)
	require.True(t, t0.Equal(res.Slot.ClaimedAt), "the claim time stays")
	_, err := openAs(s, o, k, false, t0.Add(time.Hour))
	require.NoError(t, err, "the holder's databases stay")
}

func claimReplacesOtherKey(t *testing.T, s store.Store) {
	o, alice, bob := owner(t), subject(t, "alice"), subject(t, "bob")
	claim(t, s, o, alice, "device-1", t0)
	first, second := key(t, "first"), key(t, "second")
	for _, k := range []store.Key{second, first} {
		lease := create(t, s, k, instanceA, t0).Lease
		_, err := commit(s, k, lease.Epoch, 0, 1, commit1, t0, block(0, 0xa1))
		require.NoError(t, err)
	}
	deleted := key(t, "deleted")
	create(t, s, deleted, instanceA, t0)
	_, err := remove(s, deleted, true, t0)
	require.NoError(t, err)

	res := claim(t, s, o, bob, "device-2", t0.Add(time.Minute))
	require.NotNil(t, res.Replaced)
	require.Equal(t, alice, res.Replaced.Subject)
	require.Equal(t, "device-1", res.Replaced.Label)
	require.Equal(t, []store.Key{first, second}, res.Deleted, "the replaced key's databases, sorted, without deleted ones")
	require.Equal(t, bob, res.Slot.Subject)
	require.True(t, t0.Add(time.Minute).Equal(res.Slot.ClaimedAt))

	_, err = openAs(s, o, first, false, t0.Add(time.Minute))
	require.ErrorIs(t, err, store.ErrSlotTaken, "the owner's old key is locked out")
	for _, k := range []store.Key{first, deleted} {
		_, err = openAs(s, "", k, false, t0.Add(time.Minute))
		require.ErrorIs(t, err, store.ErrNotFound, "%s is gone", k.DBID)
		again, err := openAs(s, "", k, true, t0.Add(time.Minute))
		require.NoError(t, err)
		require.Zero(t, again.State.Version, "%s left no record", k.DBID)
	}
}

func slotBlocksOwnersOtherKeys(t *testing.T, s store.Store) {
	o, alice, bob := owner(t), subject(t, "alice"), subject(t, "bob")
	ka := key(t, "db")
	kb := store.Key{Subject: bob, DBID: "db"}
	// Created with the instance openAs uses, so that the instance's own lease does not block the opens below.
	lease := create(t, s, kb, instanceA, t0).Lease
	claim(t, s, o, alice, "device-1", t0)

	_, err := openAs(s, o, ka, true, t0)
	require.NoError(t, err, "the holder opens")
	_, err = openAs(s, o, kb, false, t0)
	require.ErrorIs(t, err, store.ErrSlotTaken)
	_, err = s.Open(context.Background(), store.OpenRequest{
		Key: kb, Owner: o, InstanceID: instanceA, Resume: &store.Resume{LeaseID: lease.ID, LeaseEpoch: lease.Epoch},
		Now: t0, TTL: ttl,
	})
	require.ErrorIs(t, err, store.ErrSlotTaken, "also when resuming")
	_, err = openAs(s, "", kb, false, t0)
	require.NoError(t, err, "without an owner the slot does not apply")
	_, err = openAs(s, owner(t)+"-other", kb, false, t0)
	require.NoError(t, err, "another owner's slot does not apply")
}

func deleteSlotPurgesHolder(t *testing.T, s store.Store) {
	o, alice, bob := owner(t), subject(t, "alice"), subject(t, "bob")
	k := key(t, "db")
	claim(t, s, o, alice, "device-1", t0)
	create(t, s, k, instanceA, t0)

	_, err := s.DeleteSlot(context.Background(), o, bob)
	require.ErrorIs(t, err, store.ErrSlotTaken, "only the holder deletes the slot")
	_, err = openAs(s, o, k, false, t0)
	require.NoError(t, err, "nothing was deleted")

	deleted, err := s.DeleteSlot(context.Background(), o, alice)
	require.NoError(t, err)
	require.Equal(t, []store.Key{k}, deleted)
	_, ok, err := s.GetSlot(context.Background(), o)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = openAs(s, o, k, false, t0)
	require.ErrorIs(t, err, store.ErrNotFound)
	again, err := openAs(s, o, k, true, t0)
	require.NoError(t, err)
	require.Zero(t, again.State.Version, "no record stays")

	deleted, err = s.DeleteSlot(context.Background(), o, alice)
	require.NoError(t, err, "without a slot only the key's databases are deleted")
	require.Equal(t, []store.Key{k}, deleted)
}

func deleteUnusedReleasesSlots(t *testing.T, s store.Store) {
	cutoff := longAgo.AddDate(0, 6, 0)
	o, alice := owner(t), subject(t, "alice")
	k := key(t, "db")
	claim(t, s, o, alice, "device-1", longAgo)
	create(t, s, k, instanceA, longAgo)

	// A slot claimed after the cutoff whose key has no databases yet stays.
	fresh := owner(t) + "-fresh"
	claim(t, s, fresh, subject(t, "carol"), "device-3", cutoff)

	require.Equal(t, []store.Key{k}, deleteUnused(t, s, cutoff))
	_, ok, err := s.GetSlot(context.Background(), o)
	require.NoError(t, err)
	require.False(t, ok, "the slot of a key without databases is released")
	_, ok, err = s.GetSlot(context.Background(), fresh)
	require.NoError(t, err)
	require.True(t, ok, "a slot claimed after the cutoff stays")

	again, err := openAs(s, o, k, true, t0)
	require.NoError(t, err)
	require.Zero(t, again.State.Version, "the database of a slot holder left no record")
}
