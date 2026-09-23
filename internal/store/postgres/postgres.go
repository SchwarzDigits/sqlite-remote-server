// Package postgres implements store.Store in PostgreSQL. It passes the same contract as the memory store. A
// database's row holds both its version and its lease, so granting a lease, fencing and applying a commit all run
// under the same row lock.
package postgres

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/postgres/db"
)

const leaseIDBytes = 16

// partBytes is the maximum size of one block part, stored as one row. Parts of this size are stored inline, not in
// TOAST, and with fillfactor 50 the new row version of an update fits on the same page. This allows HOT updates. See
// migrations/00003_block_parts.sql.
const partBytes = 1024

// Store keeps the databases in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

var _ store.Store = (*Store)(nil)

// New returns a store that uses pool. The schema must be up to date. See Migrate.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, q: db.New(pool)}
}

// Open implements store.Store.
func (s *Store) Open(ctx context.Context, req store.OpenRequest) (store.OpenResult, error) {
	var result store.OpenResult
	err := s.inTx(ctx, func(q *db.Queries) error {
		row, err := q.LockDatabase(ctx, db.LockDatabaseParams{Subject: req.Key.Subject, DbID: req.Key.DBID})
		if errors.Is(err, pgx.ErrNoRows) {
			row, err = createDatabase(ctx, q, req)
		}
		if err != nil {
			return err
		}
		if row.Deleted {
			if row, err = reviveDatabase(ctx, q, req); err != nil {
				return err
			}
		} else if req.PageSize != 0 && uint32(row.PageSize) != req.PageSize {
			return store.BadRequest("page size %d, the database has %d", req.PageSize, row.PageSize)
		}

		if req.Resume != nil {
			if !holds(row, req.Resume.LeaseEpoch) || !bytes.Equal(row.LeaseID, req.Resume.LeaseID) {
				return store.ErrFenced
			}
			if _, err := q.ExtendLease(ctx, db.ExtendLeaseParams{
				Subject:      req.Key.Subject,
				DbID:         req.Key.DBID,
				LeaseEpoch:   row.LeaseEpoch,
				LeaseExpires: timestamp(req.Now.Add(req.TTL)),
			}); err != nil {
				return err
			}
			result = store.OpenResult{Lease: leaseOf(row), State: stateOf(row)}
			return nil
		}

		active := row.LeaseHolder != nil && row.LeaseExpires.Valid && req.Now.Before(row.LeaseExpires.Time)
		if active && !req.Takeover {
			return &store.LeaseHeldError{Since: row.LeaseGranted.Time}
		}
		id := newLeaseID()
		epoch, err := q.GrantLease(ctx, db.GrantLeaseParams{
			Subject:      req.Key.Subject,
			DbID:         req.Key.DBID,
			LeaseID:      id,
			LeaseHolder:  bytes.Clone(req.InstanceID),
			LeaseGranted: timestamp(req.Now),
			LeaseExpires: timestamp(req.Now.Add(req.TTL)),
		})
		if err != nil {
			return err
		}
		result = store.OpenResult{
			Lease:   store.Lease{ID: id, Epoch: uint64(epoch)},
			State:   stateOf(row),
			Revoked: active,
		}
		return nil
	})
	if err != nil {
		return store.OpenResult{}, err
	}
	return result, nil
}

// createDatabase creates the database if the request allows it. If another transaction created it concurrently,
// createDatabase locks and returns that row.
func createDatabase(ctx context.Context, q *db.Queries, req store.OpenRequest) (db.Database, error) {
	if !req.Create || req.Resume != nil {
		return db.Database{}, store.ErrNotFound
	}
	if !store.ValidPageSize(req.PageSize) {
		return db.Database{}, store.BadRequest("page size %d is not a power of two from %d to %d",
			req.PageSize, store.MinPageSize, store.MaxPageSize)
	}
	row, err := q.CreateDatabase(ctx, db.CreateDatabaseParams{
		Subject:  req.Key.Subject,
		DbID:     req.Key.DBID,
		PageSize: int32(req.PageSize),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// Another transaction inserted the row first. The INSERT waited for it to commit, so the row is visible now.
		return q.LockDatabase(ctx, db.LockDatabaseParams{Subject: req.Key.Subject, DbID: req.Key.DBID})
	}
	return row, err
}

// reviveDatabase creates a deleted database anew, empty. Its lease epoch and version continue, so no lease on the
// deleted database can commit to the new one. A deleted database cannot be resumed and is not found without Create.
func reviveDatabase(ctx context.Context, q *db.Queries, req store.OpenRequest) (db.Database, error) {
	switch {
	case req.Resume != nil:
		return db.Database{}, store.ErrFenced
	case !req.Create:
		return db.Database{}, store.ErrNotFound
	case !store.ValidPageSize(req.PageSize):
		return db.Database{}, store.BadRequest("page size %d is not a power of two from %d to %d",
			req.PageSize, store.MinPageSize, store.MaxPageSize)
	}
	return q.ReviveDatabase(ctx, db.ReviveDatabaseParams{
		Subject:  req.Key.Subject,
		DbID:     req.Key.DBID,
		PageSize: int32(req.PageSize),
	})
}

// Fetch implements store.Store.
func (s *Store) Fetch(ctx context.Context, key store.Key, version, first, count uint64) ([][]byte, error) {
	// Read the state and the blocks from one snapshot, so a concurrent commit cannot mix two versions.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.q.WithTx(tx)

	row, err := q.GetDatabase(ctx, db.GetDatabaseParams{Subject: key.Subject, DbID: key.DBID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if row.Deleted {
		return nil, store.ErrNotFound
	}
	if version != uint64(row.Version) {
		return nil, &store.VersionConflictError{Current: uint64(row.Version)}
	}
	if err := store.CheckRange(first, count, uint64(row.PageCount)); err != nil {
		return nil, err
	}

	written, err := q.FetchBlocks(ctx, db.FetchBlocksParams{
		Subject: key.Subject,
		DbID:    key.DBID,
		First:   int64(first),
		Beyond:  int64(first + count),
	})
	if err != nil {
		return nil, err
	}
	blocks := make([][]byte, count)
	for i := range blocks {
		// A block that was never written reads as zeros, as in a sparse file.
		blocks[i] = make([]byte, row.PageSize)
	}
	// Copy each part to its offset in the block.
	for _, part := range written {
		copy(blocks[uint64(part.Idx)-first][int(part.Part)*partBytes:], part.Data)
	}
	return blocks, nil
}

// Commit implements store.Store.
func (s *Store) Commit(ctx context.Context, req store.CommitRequest) (uint64, error) {
	var version uint64
	err := s.inTx(ctx, func(q *db.Queries) error {
		row, err := q.LockDatabase(ctx, db.LockDatabaseParams{Subject: req.Key.Subject, DbID: req.Key.DBID})
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !holds(row, req.LeaseEpoch) {
			return store.ErrFenced
		}
		expires := timestamp(req.Now.Add(req.TTL))

		if uint64(row.Version) == req.BaseVersion+1 && len(req.CommitID) > 0 &&
			bytes.Equal(row.LastCommitID, req.CommitID) {
			// Repeated commit, e.g. after a lost acknowledgement. Extend the lease and return the version without
			// applying the commit again.
			version = uint64(row.Version)
			_, err := q.ExtendLease(ctx, db.ExtendLeaseParams{
				Subject:      req.Key.Subject,
				DbID:         req.Key.DBID,
				LeaseEpoch:   row.LeaseEpoch,
				LeaseExpires: expires,
			})
			return err
		}
		if uint64(row.Version) != req.BaseVersion {
			return &store.VersionConflictError{Current: uint64(row.Version)}
		}
		if err := store.ValidateCommit(uint32(row.PageSize), req); err != nil {
			return err
		}

		if err := putBlocks(ctx, q, req); err != nil {
			return err
		}
		if err := q.DeleteBlocksFrom(ctx, db.DeleteBlocksFromParams{
			Subject: req.Key.Subject,
			DbID:    req.Key.DBID,
			Idx:     int64(req.PageCount),
		}); err != nil {
			return err
		}
		applied, err := q.ApplyCommit(ctx, db.ApplyCommitParams{
			Subject:      req.Key.Subject,
			DbID:         req.Key.DBID,
			PageCount:    int64(req.PageCount),
			LastCommitID: bytes.Clone(req.CommitID),
			LeaseExpires: expires,
		})
		if err != nil {
			return err
		}
		version = uint64(applied)
		return recordChange(ctx, q, req.Key, version, req.Blocks)
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

// putBlocks writes the block parts of a commit in one pipelined batch.
func putBlocks(ctx context.Context, q *db.Queries, req store.CommitRequest) error {
	if len(req.Blocks) == 0 {
		return nil
	}
	params := make([]db.PutBlocksParams, 0, len(req.Blocks)*((len(req.Blocks[0].Data)+partBytes-1)/partBytes))
	for _, block := range req.Blocks {
		for at := 0; at < len(block.Data); at += partBytes {
			params = append(params, db.PutBlocksParams{
				Subject: req.Key.Subject,
				DbID:    req.Key.DBID,
				Idx:     int64(block.Index),
				Part:    int16(at / partBytes),
				Data:    block.Data[at:min(at+partBytes, len(block.Data))],
			})
		}
	}
	results := q.PutBlocks(ctx, params)
	defer func() { _ = results.Close() }()
	var failed error
	results.Exec(func(_ int, err error) {
		if err != nil && failed == nil {
			failed = err
		}
	})
	return failed
}

// recordChange adds the commit's changed blocks to the change log and deletes entries older than the window. It
// runs in the commit's transaction, so the log never contains a version the database does not have.
func recordChange(ctx context.Context, q *db.Queries, key store.Key, version uint64, blocks []store.Block) error {
	touched := make([]int64, 0, len(blocks))
	for _, block := range blocks {
		touched = append(touched, int64(block.Index))
	}
	slices.Sort(touched)
	if err := q.RecordChange(ctx, db.RecordChangeParams{
		Subject: key.Subject,
		DbID:    key.DBID,
		Version: int64(version),
		Blocks:  touched,
	}); err != nil {
		return err
	}
	if version <= store.ChangeWindow {
		return nil
	}
	return q.PruneChanges(ctx, db.PruneChangesParams{
		Subject: key.Subject,
		DbID:    key.DBID,
		Version: int64(version - store.ChangeWindow),
	})
}

// Changes implements store.Store.
func (s *Store) Changes(ctx context.Context, key store.Key, fromVersion uint64) (store.ChangeSet, error) {
	var set store.ChangeSet
	err := s.inTx(ctx, func(q *db.Queries) error {
		row, err := q.GetDatabase(ctx, db.GetDatabaseParams{Subject: key.Subject, DbID: key.DBID})
		if errors.Is(err, pgx.ErrNoRows) || err == nil && row.Deleted {
			return store.ErrNotFound
		}
		if err != nil {
			return err
		}
		current := uint64(row.Version)
		if fromVersion > current {
			return store.BadRequest("version %d is ahead of %d", fromVersion, current)
		}
		set = store.ChangeSet{FromVersion: fromVersion, ToVersion: current, Complete: true}
		if fromVersion == current {
			return nil
		}

		oldest, err := q.OldestChange(ctx, db.OldestChangeParams{Subject: key.Subject, DbID: key.DBID})
		if err != nil {
			return err
		}
		// The log must contain every version after fromVersion. Otherwise the change set would be incomplete.
		if oldest == 0 || uint64(oldest) > fromVersion+1 {
			set = store.ChangeSet{FromVersion: fromVersion, ToVersion: current}
			return nil
		}
		rows, err := q.ChangedSince(ctx, db.ChangedSinceParams{
			Subject: key.Subject,
			DbID:    key.DBID,
			Version: int64(fromVersion),
		})
		if err != nil {
			return err
		}
		seen := make(map[uint64]struct{})
		for _, row := range rows {
			for _, index := range row.Blocks {
				seen[uint64(index)] = struct{}{}
			}
		}
		set.Blocks = slices.Sorted(maps.Keys(seen))
		return nil
	})
	if err != nil {
		return store.ChangeSet{}, err
	}
	return set, nil
}

// Renew implements store.Store.
func (s *Store) Renew(ctx context.Context, key store.Key, epoch uint64, now time.Time, ttl time.Duration) error {
	rows, err := s.q.ExtendLease(ctx, db.ExtendLeaseParams{
		Subject:      key.Subject,
		DbID:         key.DBID,
		LeaseEpoch:   int64(epoch),
		LeaseExpires: timestamp(now.Add(ttl)),
	})
	if err != nil {
		return err
	}
	return s.whyNoRow(ctx, key, rows)
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, key store.Key, epoch uint64) error {
	rows, err := s.q.ReleaseLease(ctx, db.ReleaseLeaseParams{
		Subject:    key.Subject,
		DbID:       key.DBID,
		LeaseEpoch: int64(epoch),
	})
	if err != nil {
		return err
	}
	return s.whyNoRow(ctx, key, rows)
}

func (s *Store) Delete(ctx context.Context, req store.DeleteRequest) (store.DeleteResult, error) {
	var result store.DeleteResult
	err := s.inTx(ctx, func(q *db.Queries) error {
		row, err := q.LockDatabase(ctx, db.LockDatabaseParams{Subject: req.Key.Subject, DbID: req.Key.DBID})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if row.Deleted {
			return nil
		}
		active := row.LeaseHolder != nil && row.LeaseExpires.Valid && req.Now.Before(row.LeaseExpires.Time)
		if active && !req.Takeover {
			return &store.LeaseHeldError{Since: row.LeaseGranted.Time}
		}
		if err := q.DeleteBlocksFrom(ctx, db.DeleteBlocksFromParams{
			Subject: req.Key.Subject,
			DbID:    req.Key.DBID,
			Idx:     0,
		}); err != nil {
			return err
		}
		if err := q.DeleteChanges(ctx, db.DeleteChangesParams{Subject: req.Key.Subject, DbID: req.Key.DBID}); err != nil {
			return err
		}
		epoch, err := q.DeleteDatabase(ctx, db.DeleteDatabaseParams{Subject: req.Key.Subject, DbID: req.Key.DBID})
		if err != nil {
			return err
		}
		result = store.DeleteResult{Epoch: uint64(epoch), Revoked: active}
		return nil
	})
	if err != nil {
		return store.DeleteResult{}, err
	}
	return result, nil
}

// whyNoRow returns the error for a lease update that matched no row: ErrNotFound if the database does not exist,
// ErrFenced otherwise.
func (s *Store) whyNoRow(ctx context.Context, key store.Key, rows int64) error {
	if rows > 0 {
		return nil
	}
	exists, err := s.q.DatabaseExists(ctx, db.DatabaseExistsParams{Subject: key.Subject, DbID: key.DBID})
	switch {
	case err != nil:
		return err
	case !exists:
		return store.ErrNotFound
	default:
		return store.ErrFenced
	}
}

// Ping implements store.Store.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// inTx runs f in a transaction and commits the transaction if f succeeds.
func (s *Store) inTx(ctx context.Context, f func(*db.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := f(s.q.WithTx(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// holds reports whether the lease is held and has the given epoch.
func holds(row db.Database, epoch uint64) bool {
	return row.LeaseHolder != nil && uint64(row.LeaseEpoch) == epoch
}

func leaseOf(row db.Database) store.Lease {
	return store.Lease{ID: row.LeaseID, Epoch: uint64(row.LeaseEpoch)}
}

func stateOf(row db.Database) store.State {
	return store.State{
		PageSize:     uint32(row.PageSize),
		PageCount:    uint64(row.PageCount),
		Version:      uint64(row.Version),
		LastCommitID: row.LastCommitID,
	}
}

func timestamp(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func newLeaseID() []byte {
	id := make([]byte, leaseIDBytes)
	_, _ = rand.Read(id) // crypto/rand.Read never fails
	return id
}
