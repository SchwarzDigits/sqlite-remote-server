-- All SQL queries of the PostgreSQL store. sqlc generates internal/store/postgres/db from this file.

-- LockDatabase locks the row of a database. Granting a lease, fencing and applying a commit all hold this lock, so
-- they are serialized per database.
-- name: LockDatabase :one
SELECT * FROM databases
WHERE subject = $1 AND db_id = $2
FOR UPDATE;

-- name: GetDatabase :one
SELECT * FROM databases
WHERE subject = $1 AND db_id = $2;

-- name: DatabaseExists :one
SELECT EXISTS (SELECT 1 FROM databases WHERE subject = $1 AND db_id = $2);

-- CreateDatabase inserts nothing if the row already exists. The caller then locks and reads the existing row.
-- name: CreateDatabase :one
INSERT INTO databases (subject, db_id, page_size)
VALUES ($1, $2, $3)
ON CONFLICT (subject, db_id) DO NOTHING
RETURNING *;

-- name: GrantLease :one
UPDATE databases
SET lease_epoch = lease_epoch + 1,
    lease_id = $3,
    lease_holder = $4,
    lease_granted = $5,
    lease_expires = $6
WHERE subject = $1 AND db_id = $2
RETURNING lease_epoch;

-- ExtendLease updates no row if the epoch is not current or the lease was released. The caller then returns
-- ErrFenced, or ErrNotFound if the database does not exist.
-- name: ExtendLease :execrows
UPDATE databases
SET lease_expires = $4
WHERE subject = $1 AND db_id = $2 AND lease_epoch = $3 AND lease_holder IS NOT NULL;

-- name: ReleaseLease :execrows
UPDATE databases
SET lease_holder = NULL
WHERE subject = $1 AND db_id = $2 AND lease_epoch = $3 AND lease_holder IS NOT NULL;

-- FetchBlocks returns the stored parts of the blocks in a range, ordered by block and part. A block that was never
-- written has no rows. The caller returns zeros for it.
-- name: FetchBlocks :many
SELECT idx, part, data FROM blocks
WHERE subject = sqlc.arg(subject) AND db_id = sqlc.arg(db_id)
  AND idx >= sqlc.arg(first) AND idx < sqlc.arg(beyond)
ORDER BY idx, part;

-- PutBlocks inserts or updates the parts of the blocks of one commit. pgx sends all rows in one pipelined batch.
-- An update of a part stays on the same page (HOT update), so the table does not grow. See
-- migrations/00003_block_parts.sql.
-- name: PutBlocks :batchexec
INSERT INTO blocks (subject, db_id, idx, part, data)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (subject, db_id, idx, part) DO UPDATE SET data = EXCLUDED.data;

-- name: DeleteBlocksFrom :exec
DELETE FROM blocks
WHERE subject = $1 AND db_id = $2 AND idx >= $3;

-- name: ApplyCommit :one
UPDATE databases
SET page_count = $3,
    version = version + 1,
    last_commit_id = $4,
    lease_expires = $5
WHERE subject = $1 AND db_id = $2
RETURNING version;

-- RecordChange adds the changed blocks of a commit to the change log. On a conflict it does nothing, so a repeated
-- commit cannot fail here: the entry for that version already exists with the same content.
-- name: RecordChange :exec
INSERT INTO changes (subject, db_id, version, blocks)
VALUES ($1, $2, $3, $4)
ON CONFLICT (subject, db_id, version) DO NOTHING;

-- PruneChanges deletes the change log entries up to and including the given version.
-- name: PruneChanges :exec
DELETE FROM changes
WHERE subject = $1 AND db_id = $2 AND version <= $3;

-- name: ChangedSince :many
SELECT version, blocks FROM changes
WHERE subject = $1 AND db_id = $2 AND version > $3
ORDER BY version;

-- OldestChange returns the oldest version in the change log, or 0 if the log is empty. The log is empty for a
-- database created anew after a deletion, and for one whose commits all predate the change log.
-- name: OldestChange :one
SELECT coalesce(min(version), 0)::bigint FROM changes
WHERE subject = $1 AND db_id = $2;

-- DeleteDatabase marks a database as deleted and empties its state. It increases the lease epoch and the version and
-- removes the lease, so that no earlier lease can commit again.
-- name: DeleteDatabase :one
UPDATE databases
SET deleted = true,
    page_count = 0,
    version = version + 1,
    last_commit_id = NULL,
    lease_epoch = lease_epoch + 1,
    lease_id = NULL,
    lease_holder = NULL,
    lease_granted = NULL,
    lease_expires = NULL
WHERE subject = $1 AND db_id = $2
RETURNING lease_epoch;

-- UnusedDatabases returns up to $2 databases whose lease expired before $1, oldest first. It takes no lock: the caller
-- locks each row and checks it again before deleting. No index covers lease_expires, because every commit and lease
-- renewal updates it, and an index on it would prevent HOT updates of the row. A sweep reads the whole table.
-- name: UnusedDatabases :many
SELECT subject, db_id FROM databases
WHERE NOT deleted AND lease_expires < $1
ORDER BY lease_expires
LIMIT $2;

-- name: DeleteChanges :exec
DELETE FROM changes
WHERE subject = $1 AND db_id = $2;

-- ReviveDatabase creates a deleted database anew, empty, with the given page size. Epoch and version continue.
-- name: ReviveDatabase :one
UPDATE databases
SET deleted = false,
    page_size = $3
WHERE subject = $1 AND db_id = $2
RETURNING *;

-- SyncStandbyNames returns synchronous_standby_names. It is empty if PostgreSQL does not replicate commits
-- synchronously.
-- name: SyncStandbyNames :one
SELECT current_setting('synchronous_standby_names')::text;

-- name: GetSlot :one
SELECT * FROM slots
WHERE owner = $1;

-- LockSlot locks an owner's slot. Opening a database and claiming or deleting the slot take this lock before any
-- database row, so they cannot deadlock.
-- name: LockSlot :one
SELECT * FROM slots
WHERE owner = $1
FOR UPDATE;

-- ShareSlot locks an owner's slot against a concurrent claim while a database is opened.
-- name: ShareSlot :one
SELECT * FROM slots
WHERE owner = $1
FOR SHARE;

-- InsertSlot inserts nothing if another transaction inserted the owner's slot first. The caller then locks it.
-- name: InsertSlot :one
INSERT INTO slots (owner, subject, label, claimed_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (owner) DO NOTHING
RETURNING *;

-- name: UpdateSlot :one
UPDATE slots
SET subject = $2,
    label = $3,
    claimed_at = $4
WHERE owner = $1
RETURNING *;

-- name: DeleteSlotOf :exec
DELETE FROM slots
WHERE owner = $1;

-- PurgeSubject deletes all databases of a subject completely, with their blocks and change logs (foreign keys). It
-- returns those that were not deleted before.
-- name: PurgeSubject :many
DELETE FROM databases
WHERE subject = $1
RETURNING db_id, deleted;

-- name: PurgeDatabase :exec
DELETE FROM databases
WHERE subject = $1 AND db_id = $2;

-- name: HoldsSlot :one
SELECT EXISTS (SELECT 1 FROM slots WHERE subject = $1);

-- ReleaseUnusedSlots releases the slots that were claimed before the cutoff and whose key has no databases left.
-- name: ReleaseUnusedSlots :exec
DELETE FROM slots s
WHERE s.claimed_at < $1
  AND NOT EXISTS (SELECT 1 FROM databases d WHERE d.subject = s.subject AND NOT d.deleted);
