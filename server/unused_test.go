package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/memory"
)

func openAt(t *testing.T, st store.Store, db string, now time.Time) {
	t.Helper()
	_, err := st.Open(context.Background(), store.OpenRequest{
		Key:        store.Key{Subject: "alice", DBID: db},
		InstanceID: []byte("instance"),
		PageSize:   4096,
		Create:     true,
		Now:        now,
		TTL:        30 * time.Second,
	})
	require.NoError(t, err)
}

func TestSweepDeletesUnusedDatabasesInBatches(t *testing.T) {
	st := memory.New()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	for i := range 2*sweepBatch + 5 {
		openAt(t, st, fmt.Sprintf("old-%d", i), now.AddDate(0, -7, 0))
	}
	openAt(t, st, "recent", now.AddDate(0, -1, 0))

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deleted, err := sweep(context.Background(), st, now.Add(-180*24*time.Hour), log)
	require.NoError(t, err)
	require.Equal(t, 2*sweepBatch+5, deleted)

	_, err = st.Open(context.Background(), store.OpenRequest{
		Key: store.Key{Subject: "alice", DBID: "old-0"}, InstanceID: []byte("other"), Now: now, TTL: time.Second,
	})
	require.ErrorIs(t, err, store.ErrNotFound)
	_, err = st.Open(context.Background(), store.OpenRequest{
		Key: store.Key{Subject: "alice", DBID: "recent"}, InstanceID: []byte("other"), Takeover: true, Now: now,
		TTL: time.Second,
	})
	require.NoError(t, err, "a database used a month ago is kept")
}

func TestDeleteUnusedSweepsAtOnceAndStops(t *testing.T) {
	st := memory.New()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	openAt(t, st, "old", now.AddDate(-1, 0, 0))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deleteUnused(ctx, st, 180*24*time.Hour, func() time.Time { return now }, log)

	keys, err := st.DeleteUnused(context.Background(), now, 10)
	require.NoError(t, err)
	require.Empty(t, keys, "the first sweep runs before the first tick")
}
