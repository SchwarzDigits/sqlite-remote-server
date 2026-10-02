-- +goose Up
-- One row per owner with a slot: the key (subject) that holds it, a label and when it passed to that key. Owners are
-- the `sub` of access tokens. While a key holds an owner's slot, the owner's other keys cannot open databases. See
-- store.Store.ClaimSlot.
CREATE TABLE slots (
    owner      text        PRIMARY KEY,
    subject    text        NOT NULL,
    label      text        NOT NULL,
    claimed_at timestamptz NOT NULL
);

-- For the deletion of unused databases, which checks whether a subject holds a slot.
CREATE INDEX slots_subject ON slots (subject);

-- +goose Down
DROP TABLE slots;
