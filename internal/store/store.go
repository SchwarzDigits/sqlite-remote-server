// Package store defines the storage of the server's databases: blocks, versions and leases. Blocks are ciphertext
// and are stored as they arrive. Every implementation must pass storetest.Run.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Page size limits of SQLite.
const (
	MinPageSize = 512
	MaxPageSize = 65536
)

// Key identifies a database by the subject that owns it and its ID within that subject.
type Key struct {
	Subject string
	DBID    string
}

// Lease is the exclusive right of one client instance to commit to a database.
type Lease struct {
	ID []byte
	// Epoch increases with every new lease. Requests with an older epoch are rejected (fencing).
	Epoch uint64
}

// State describes a database at one version.
type State struct {
	PageSize     uint32
	PageCount    uint64
	Version      uint64
	LastCommitID []byte
}

// Resume identifies a lease to continue after a reconnect.
type Resume struct {
	LeaseID    []byte
	LeaseEpoch uint64
}

// OpenRequest opens a database and takes its lease.
type OpenRequest struct {
	Key        Key
	InstanceID []byte
	// PageSize is required to create a database. For an existing database it must be 0 or match.
	PageSize uint32
	Create   bool
	// Takeover takes the lease even if another instance holds an unexpired lease.
	Takeover bool
	// Resume, if set, continues the given lease instead of granting a new one.
	Resume *Resume
	Now    time.Time
	TTL    time.Duration
}

// OpenResult holds the lease and the database state after a successful open.
type OpenResult struct {
	Lease Lease
	State State
	// Revoked is true if the open took the lease from an instance whose lease had not expired.
	Revoked bool
}

// Block is one page-sized block of the database file.
type Block struct {
	Index uint64
	Data  []byte
}

// CommitRequest applies the blocks changed by one or more SQLite transactions in one atomic step.
type CommitRequest struct {
	Key         Key
	CommitID    []byte
	LeaseEpoch  uint64
	BaseVersion uint64
	// PageCount is the file length in blocks after the commit. Blocks at or beyond it are deleted.
	PageCount uint64
	Blocks    []Block
	Now       time.Time
	TTL       time.Duration
}

// ChangeWindow is the number of recent commits for which a store keeps the indexes of the changed blocks. A client
// whose local copy is older than the window must reload the database.
//
// At about ten blocks per commit (measured), the change log takes less than 100 KB per database. The window is larger
// than a client needs: once more than about a third of the database has changed, the client reloads the whole
// database anyway.
const ChangeWindow = 1024

// ChangeSet lists the blocks changed between two versions.
type ChangeSet struct {
	FromVersion uint64
	ToVersion   uint64
	// Blocks holds the index of every block changed by a commit in between, sorted and without duplicates. It is
	// only valid if Complete is true.
	Blocks []uint64
	// Complete is false if the change log no longer reaches back to FromVersion.
	Complete bool
}

// Store keeps the databases. Implementations must be safe for concurrent use.
//
// A lease stays valid until another instance takes it or its holder releases it. Expiry only decides whether another
// instance may take the lease without Takeover. An expired lease that no other instance has taken can still commit and
// renew.
type Store interface {
	Open(ctx context.Context, req OpenRequest) (OpenResult, error)
	// Fetch returns count blocks starting at index first. version must be the current version.
	Fetch(ctx context.Context, key Key, version, first, count uint64) ([][]byte, error)
	// Commit applies a commit and returns the new version. If the request repeats the last commit (same commit
	// ID), Commit applies nothing and returns that commit's version.
	Commit(ctx context.Context, req CommitRequest) (uint64, error)
	// Changes returns the blocks changed since fromVersion. Clients use it to update a stale local copy. A
	// fromVersion newer than the current version is a bad request. If fromVersion is older than the change window,
	// the result has Complete set to false.
	Changes(ctx context.Context, key Key, fromVersion uint64) (ChangeSet, error)
	// Renew extends the lease with the given epoch.
	Renew(ctx context.Context, key Key, epoch uint64, now time.Time, ttl time.Duration) error
	// Release gives up the lease with the given epoch.
	Release(ctx context.Context, key Key, epoch uint64) error
	// Ping checks that the store is reachable. The readiness endpoint uses it.
	Ping(ctx context.Context) error
}

var (
	// ErrNotFound means the database does not exist.
	ErrNotFound = errors.New("database not found")
	// ErrFenced means the lease epoch is not the current one. Another instance took the lease, or it was released.
	ErrFenced = errors.New("lease epoch is stale")
)

// LeaseHeldError means another instance holds a lease that has not expired.
type LeaseHeldError struct {
	Since time.Time
}

func (e *LeaseHeldError) Error() string {
	return fmt.Sprintf("another instance has held the lease since %s", e.Since.UTC().Format(time.RFC3339))
}

// VersionConflictError means the request was based on a version other than the current one.
type VersionConflictError struct {
	Current uint64
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf("current version is %d", e.Current)
}

// BadRequestError means the request is invalid. Repeating it cannot succeed.
type BadRequestError struct {
	Reason string
}

func (e *BadRequestError) Error() string {
	return e.Reason
}

// BadRequest builds a BadRequestError.
func BadRequest(format string, args ...any) error {
	return &BadRequestError{Reason: fmt.Sprintf(format, args...)}
}

// ValidPageSize reports whether SQLite allows the page size.
func ValidPageSize(size uint32) bool {
	return size >= MinPageSize && size <= MaxPageSize && size&(size-1) == 0
}

// ValidateCommit checks the commit ID and the blocks of a commit against the database's page size and the new page
// count.
func ValidateCommit(pageSize uint32, req CommitRequest) error {
	if len(req.CommitID) == 0 {
		return BadRequest("commit id is empty")
	}
	seen := make(map[uint64]struct{}, len(req.Blocks))
	for _, block := range req.Blocks {
		if uint64(len(block.Data)) != uint64(pageSize) {
			return BadRequest("block %d has %d bytes, page size is %d", block.Index, len(block.Data), pageSize)
		}
		if block.Index >= req.PageCount {
			return BadRequest("block %d is beyond page count %d", block.Index, req.PageCount)
		}
		if _, dup := seen[block.Index]; dup {
			return BadRequest("block %d appears twice", block.Index)
		}
		seen[block.Index] = struct{}{}
	}
	return nil
}

// CheckRange checks that the count blocks starting at first lie within pageCount.
func CheckRange(first, count, pageCount uint64) error {
	if first > pageCount || count > pageCount-first {
		return BadRequest("blocks %d to %d are beyond page count %d", first, first+count, pageCount)
	}
	return nil
}
