-- +goose Up
-- Stores each block as parts of at most 1 KB, one row per part, in a table with fillfactor 50.
--
-- An update in PostgreSQL writes a new row version. If the new version fits on the same page (HOT update), the old
-- version is removed during normal page access, without VACUUM. A whole 4 KB block is stored out of line in the TOAST
-- table. Every overwrite there leaves dead chunks that only VACUUM removes, and autovacuum runs about once per
-- minute. Under sustained writes the table grew by gigabytes while holding megabytes of live data.
--
-- A 1 KB part is stored inline. With fillfactor 50 every page has room for the new version of each part. Measured
-- with the same write pattern and no VACUUM: 11 MB after 1.9 million updates, compared to 4.6 GB for whole 4 KB
-- blocks in the same minute. The cost is space, about 2.7 times the live data, which stays constant.
--
-- The migration uses only table DDL. It needs no PostgreSQL server settings, which a managed service does not let
-- users change.
CREATE TABLE block_parts (
    subject text     NOT NULL,
    db_id   text     NOT NULL,
    idx     bigint   NOT NULL,
    -- Position of the part within the block, in KB, starting at 0.
    part    smallint NOT NULL,
    data    bytea    NOT NULL,
    PRIMARY KEY (subject, db_id, idx, part),
    FOREIGN KEY (subject, db_id) REFERENCES databases (subject, db_id) ON DELETE CASCADE
) WITH (fillfactor = 50);

INSERT INTO block_parts (subject, db_id, idx, part, data)
SELECT b.subject, b.db_id, b.idx, p.part, substring(b.data FROM p.part * 1024 + 1 FOR 1024)
FROM blocks b
CROSS JOIN LATERAL generate_series(0, (length(b.data) - 1) / 1024) AS p (part);

DROP TABLE blocks;
ALTER TABLE block_parts RENAME TO blocks;
ALTER TABLE blocks RENAME CONSTRAINT block_parts_pkey TO blocks_pkey;
ALTER TABLE blocks RENAME CONSTRAINT block_parts_subject_db_id_fkey TO blocks_subject_db_id_fkey;

-- +goose Down
CREATE TABLE whole_blocks (
    subject text   NOT NULL,
    db_id   text   NOT NULL,
    idx     bigint NOT NULL,
    data    bytea  NOT NULL,
    PRIMARY KEY (subject, db_id, idx),
    FOREIGN KEY (subject, db_id) REFERENCES databases (subject, db_id) ON DELETE CASCADE
);

INSERT INTO whole_blocks (subject, db_id, idx, data)
SELECT subject, db_id, idx, string_agg(data, ''::bytea ORDER BY part)
FROM blocks
GROUP BY subject, db_id, idx;

DROP TABLE blocks;
ALTER TABLE whole_blocks RENAME TO blocks;
ALTER TABLE blocks RENAME CONSTRAINT whole_blocks_pkey TO blocks_pkey;
ALTER TABLE blocks RENAME CONSTRAINT whole_blocks_subject_db_id_fkey TO blocks_subject_db_id_fkey;
