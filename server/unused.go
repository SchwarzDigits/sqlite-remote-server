package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

const (
	// sweepInterval is the time between two sweeps for unused databases.
	sweepInterval = time.Hour
	// sweepBatch is the number of databases one call to store.DeleteUnused deletes at most.
	sweepBatch = 100
)

// deleteUnused deletes the databases unused for longer than after, once right away and then every sweepInterval,
// until ctx is canceled.
func deleteUnused(ctx context.Context, st store.Store, after time.Duration, now func() time.Time, log *slog.Logger) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	var total uint64
	for {
		deleted, err := sweep(ctx, st, now().Add(-after), log)
		total += uint64(deleted)
		if deleted > 0 {
			log.Info("unused databases deleted", "count", deleted, "total_since_start", total)
		}
		if err != nil && ctx.Err() == nil {
			log.Warn("deleting unused databases failed; retrying at the next sweep", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sweep deletes every database whose lease expired before cutoff, in batches of sweepBatch, and logs each one. It
// returns the number deleted.
func sweep(ctx context.Context, st store.Store, cutoff time.Time, log *slog.Logger) (int, error) {
	deleted := 0
	for {
		keys, err := st.DeleteUnused(ctx, cutoff, sweepBatch)
		for _, k := range keys {
			log.Info("unused database deleted", "subject", k.Subject, "db", k.DBID)
		}
		deleted += len(keys)
		if err != nil || len(keys) < sweepBatch {
			return deleted, err
		}
	}
}
