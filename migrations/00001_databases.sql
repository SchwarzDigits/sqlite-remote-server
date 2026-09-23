-- +goose Up
-- One row per database with its page size, page count, version and lease. Lease and version are in the same row, so
-- granting a lease, fencing and committing lock the same row.
CREATE TABLE databases (
    subject        text   NOT NULL,
    db_id          text   NOT NULL,
    page_size      integer NOT NULL,
    page_count     bigint NOT NULL DEFAULT 0,
    version        bigint NOT NULL DEFAULT 0,
    last_commit_id bytea,
    -- Increases with every new lease. Requests with an older epoch are rejected. The epoch is kept after a release,
    -- so a released lease cannot be resumed.
    lease_epoch    bigint NOT NULL DEFAULT 0,
    lease_id       bytea,
    -- Instance ID of the lease holder. Null after the lease was released.
    lease_holder   bytea,
    lease_granted  timestamptz,
    lease_expires  timestamptz,
    PRIMARY KEY (subject, db_id)
);

-- Blocks of the database file, as ciphertext. A block that was never written has no row and reads as zeros, as in a
-- sparse file.
CREATE TABLE blocks (
    subject text   NOT NULL,
    db_id   text   NOT NULL,
    idx     bigint NOT NULL,
    data    bytea  NOT NULL,
    PRIMARY KEY (subject, db_id, idx),
    FOREIGN KEY (subject, db_id) REFERENCES databases (subject, db_id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE blocks;
DROP TABLE databases;
