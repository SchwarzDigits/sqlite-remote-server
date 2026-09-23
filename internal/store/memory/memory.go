// Package memory implements store.Store in memory, for tests and local runs. A restart deletes all databases.
package memory

import (
	"bytes"
	"context"
	"crypto/rand"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

const leaseIDBytes = 16

// change is one change log entry: the version a commit produced and the sorted indexes of the blocks it changed.
type change struct {
	version uint64
	blocks  []uint64
}

type database struct {
	// deleted marks a deleted database. Its record stays, so that epoch and version continue if it is created again.
	deleted bool
	state   store.State
	blocks  map[uint64][]byte
	// changes is the change log: the last store.ChangeWindow commits, oldest first.
	changes []change
	lease   store.Lease
	// holder is the instance ID of the lease holder. Nil after the lease was released.
	holder    []byte
	grantedAt time.Time
	expiresAt time.Time
}

// Store keeps the databases in memory.
type Store struct {
	mu  sync.Mutex
	dbs map[store.Key]*database
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{dbs: make(map[store.Key]*database)}
}

// Open implements store.Store.
func (s *Store) Open(_ context.Context, req store.OpenRequest) (store.OpenResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[req.Key]
	if ok && db.deleted {
		switch {
		case req.Resume != nil:
			return store.OpenResult{}, store.ErrFenced
		case !req.Create:
			return store.OpenResult{}, store.ErrNotFound
		case !store.ValidPageSize(req.PageSize):
			return store.OpenResult{}, store.BadRequest("page size %d is not a power of two from %d to %d",
				req.PageSize, store.MinPageSize, store.MaxPageSize)
		}
		// Create the deleted database anew, empty. Epoch and version continue.
		db.deleted = false
		db.state.PageSize = req.PageSize
	}
	switch {
	case !ok && (!req.Create || req.Resume != nil):
		return store.OpenResult{}, store.ErrNotFound
	case !ok:
		if !store.ValidPageSize(req.PageSize) {
			return store.OpenResult{}, store.BadRequest("page size %d is not a power of two from %d to %d",
				req.PageSize, store.MinPageSize, store.MaxPageSize)
		}
		db = &database{state: store.State{PageSize: req.PageSize}, blocks: make(map[uint64][]byte)}
		s.dbs[req.Key] = db
	case req.PageSize != 0 && req.PageSize != db.state.PageSize:
		return store.OpenResult{}, store.BadRequest("page size %d, the database has %d", req.PageSize, db.state.PageSize)
	}

	if req.Resume != nil {
		if db.holder == nil || db.lease.Epoch != req.Resume.LeaseEpoch || !bytes.Equal(db.lease.ID, req.Resume.LeaseID) {
			return store.OpenResult{}, store.ErrFenced
		}
		db.expiresAt = req.Now.Add(req.TTL)
		return store.OpenResult{Lease: cloneLease(db.lease), State: cloneState(db.state)}, nil
	}

	active := db.holder != nil && req.Now.Before(db.expiresAt)
	if active && !req.Takeover {
		return store.OpenResult{}, &store.LeaseHeldError{Since: db.grantedAt}
	}
	db.lease = store.Lease{ID: newLeaseID(), Epoch: db.lease.Epoch + 1}
	db.holder = bytes.Clone(req.InstanceID)
	db.grantedAt = req.Now
	db.expiresAt = req.Now.Add(req.TTL)
	return store.OpenResult{Lease: cloneLease(db.lease), State: cloneState(db.state), Revoked: active}, nil
}

// Fetch implements store.Store.
func (s *Store) Fetch(_ context.Context, key store.Key, version, first, count uint64) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[key]
	if !ok || db.deleted {
		return nil, store.ErrNotFound
	}
	if version != db.state.Version {
		return nil, &store.VersionConflictError{Current: db.state.Version}
	}
	if err := store.CheckRange(first, count, db.state.PageCount); err != nil {
		return nil, err
	}
	blocks := make([][]byte, count)
	for i := range blocks {
		if data, ok := db.blocks[first+uint64(i)]; ok {
			blocks[i] = bytes.Clone(data)
		} else {
			// A block that was never written reads as zeros, as in a sparse file.
			blocks[i] = make([]byte, db.state.PageSize)
		}
	}
	return blocks, nil
}

// Commit implements store.Store.
func (s *Store) Commit(_ context.Context, req store.CommitRequest) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[req.Key]
	if !ok {
		return 0, store.ErrNotFound
	}
	if db.holder == nil || db.lease.Epoch != req.LeaseEpoch {
		return 0, store.ErrFenced
	}
	if db.state.Version == req.BaseVersion+1 && len(req.CommitID) > 0 && bytes.Equal(db.state.LastCommitID, req.CommitID) {
		// Repeated commit, e.g. after a lost acknowledgement. Return its version without applying it again.
		db.expiresAt = req.Now.Add(req.TTL)
		return db.state.Version, nil
	}
	if db.state.Version != req.BaseVersion {
		return 0, &store.VersionConflictError{Current: db.state.Version}
	}
	if err := store.ValidateCommit(db.state.PageSize, req); err != nil {
		return 0, err
	}

	for _, block := range req.Blocks {
		db.blocks[block.Index] = bytes.Clone(block.Data)
	}
	for index := range db.blocks {
		if index >= req.PageCount {
			delete(db.blocks, index)
		}
	}
	db.state.PageCount = req.PageCount
	db.state.Version++
	db.state.LastCommitID = bytes.Clone(req.CommitID)
	db.expiresAt = req.Now.Add(req.TTL)

	touched := make([]uint64, 0, len(req.Blocks))
	for _, block := range req.Blocks {
		touched = append(touched, block.Index)
	}
	slices.Sort(touched)
	db.changes = append(db.changes, change{version: db.state.Version, blocks: touched})
	if len(db.changes) > store.ChangeWindow {
		db.changes = slices.Clone(db.changes[len(db.changes)-store.ChangeWindow:])
	}
	return db.state.Version, nil
}

// Changes implements store.Store.
func (s *Store) Changes(_ context.Context, key store.Key, fromVersion uint64) (store.ChangeSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[key]
	if !ok || db.deleted {
		return store.ChangeSet{}, store.ErrNotFound
	}
	if fromVersion > db.state.Version {
		return store.ChangeSet{}, store.BadRequest("version %d is ahead of %d", fromVersion, db.state.Version)
	}
	set := store.ChangeSet{FromVersion: fromVersion, ToVersion: db.state.Version, Complete: true}
	if fromVersion == db.state.Version {
		return set, nil
	}
	// The log must contain every version after fromVersion. Otherwise the change set would be incomplete.
	if len(db.changes) == 0 || db.changes[0].version > fromVersion+1 {
		return store.ChangeSet{FromVersion: fromVersion, ToVersion: db.state.Version}, nil
	}
	seen := make(map[uint64]struct{})
	for _, entry := range db.changes {
		if entry.version <= fromVersion {
			continue
		}
		for _, index := range entry.blocks {
			seen[index] = struct{}{}
		}
	}
	set.Blocks = slices.Sorted(maps.Keys(seen))
	return set, nil
}

// Renew implements store.Store.
func (s *Store) Renew(_ context.Context, key store.Key, epoch uint64, now time.Time, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[key]
	if !ok {
		return store.ErrNotFound
	}
	if db.holder == nil || db.lease.Epoch != epoch {
		return store.ErrFenced
	}
	db.expiresAt = now.Add(ttl)
	return nil
}

// Release implements store.Store.
func (s *Store) Release(_ context.Context, key store.Key, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[key]
	if !ok {
		return store.ErrNotFound
	}
	if db.holder == nil || db.lease.Epoch != epoch {
		return store.ErrFenced
	}
	db.holder = nil
	return nil
}

func (s *Store) Delete(_ context.Context, req store.DeleteRequest) (store.DeleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, ok := s.dbs[req.Key]
	if !ok || db.deleted {
		return store.DeleteResult{}, nil
	}
	active := db.holder != nil && req.Now.Before(db.expiresAt)
	if active && !req.Takeover {
		return store.DeleteResult{}, &store.LeaseHeldError{Since: db.grantedAt}
	}
	db.deleted = true
	db.blocks = make(map[uint64][]byte)
	db.changes = nil
	db.state = store.State{PageSize: db.state.PageSize, Version: db.state.Version + 1}
	db.lease = store.Lease{Epoch: db.lease.Epoch + 1}
	db.holder = nil
	return store.DeleteResult{Epoch: db.lease.Epoch, Revoked: active}, nil
}

// Ping implements store.Store. It always succeeds.
func (s *Store) Ping(context.Context) error {
	return nil
}

func newLeaseID() []byte {
	id := make([]byte, leaseIDBytes)
	_, _ = rand.Read(id) // crypto/rand.Read never fails
	return id
}

func cloneLease(lease store.Lease) store.Lease {
	return store.Lease{ID: bytes.Clone(lease.ID), Epoch: lease.Epoch}
}

func cloneState(state store.State) store.State {
	state.LastCommitID = bytes.Clone(state.LastCommitID)
	return state
}
