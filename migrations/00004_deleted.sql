-- +goose Up
-- A deleted database keeps its row, so that its lease epoch and version continue if it is created again. No lease on
-- the deleted database can then commit to the new one.
ALTER TABLE databases ADD COLUMN deleted boolean NOT NULL DEFAULT false;

-- +goose Down
-- Without the column a deleted database would reappear as an empty one, so its row is removed first.
DELETE FROM databases WHERE deleted;
ALTER TABLE databases DROP COLUMN deleted;
