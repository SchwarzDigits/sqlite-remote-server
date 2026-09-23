-- +goose Up
-- Change log: the indexes of the blocks changed by each commit. A client whose local copy is a few commits behind
-- removes only these blocks from the copy instead of reloading the whole database.
--
-- Only the most recent commits are kept. For an older version the server reports the change set as incomplete, and
-- the client reloads the database.
CREATE TABLE changes (
    subject text     NOT NULL,
    db_id   text     NOT NULL,
    -- Version created by the commit.
    version bigint   NOT NULL,
    blocks  bigint[] NOT NULL,
    PRIMARY KEY (subject, db_id, version),
    FOREIGN KEY (subject, db_id) REFERENCES databases (subject, db_id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE changes;
