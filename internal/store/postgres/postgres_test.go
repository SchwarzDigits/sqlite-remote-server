// Integration tests for the PostgreSQL store. They need a PostgreSQL database, set in
// SQLITE_REMOTE_TEST_DATABASE_URL. They are skipped without it and with -short:
//
//	docker run -d --rm -p 15432:5432 -e POSTGRES_PASSWORD=vfs -e POSTGRES_USER=vfs -e POSTGRES_DB=vfs postgres:17-alpine
//	SQLITE_REMOTE_TEST_DATABASE_URL=postgres://vfs:vfs@localhost:15432/vfs go test ./internal/store/postgres/...
package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/postgres"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/storetest"
	"github.com/SchwarzDigits/sqlite-remote-server/migrations"
)

const envTestDatabaseURL = "SQLITE_REMOTE_TEST_DATABASE_URL"

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	uri := os.Getenv(envTestDatabaseURL)
	if uri == "" {
		t.Skipf("integration test: %s is not set", envTestDatabaseURL)
	}
	pool, err := pgxpool.New(t.Context(), uri)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, postgres.Migrate(t.Context(), pool))
	return pool
}

func TestContract(t *testing.T) {
	pool := testPool(t)
	storetest.Run(t, func(*testing.T) store.Store { return postgres.New(pool) })
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	pool := testPool(t)
	require.NoError(t, postgres.Migrate(t.Context(), pool))

	// Several pods start at once. The session lock must let one of them migrate while the others wait.
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = postgres.Migrate(context.Background(), pool)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
}

func TestBlocksRoundTripAtAllPageSizes(t *testing.T) {
	// The contract uses a 512-byte page size, where a block is a single part. This test covers page sizes with
	// several parts per block.
	pool := testPool(t)
	st := postgres.New(pool)
	ctx := t.Context()
	now := time.Now()

	for _, pageSize := range []uint32{512, 1024, 4096, 65536} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			key := store.Key{Subject: "parts-" + randomHex(t), DBID: "db"}
			opened, err := st.Open(ctx, store.OpenRequest{
				Key: key, InstanceID: []byte{1}, PageSize: pageSize, Create: true, Now: now, TTL: time.Minute,
			})
			require.NoError(t, err)

			blocks := []store.Block{
				{Index: 0, Data: randomBytes(t, pageSize)},
				{Index: 1, Data: randomBytes(t, pageSize)},
				{Index: 3, Data: randomBytes(t, pageSize)},
			}
			version, err := st.Commit(ctx, store.CommitRequest{
				Key: key, CommitID: []byte("c1"), LeaseEpoch: opened.Lease.Epoch, PageCount: 4,
				Blocks: blocks, Now: now, TTL: time.Minute,
			})
			require.NoError(t, err)

			got, err := st.Fetch(ctx, key, version, 0, 4)
			require.NoError(t, err)
			require.Equal(t, blocks[0].Data, got[0])
			require.Equal(t, blocks[1].Data, got[1])
			require.Equal(t, make([]byte, pageSize), got[2], "never written, must read as zeros")
			require.Equal(t, blocks[2].Data, got[3])

			// Overwriting a block must replace all of its parts.
			again := store.Block{Index: 1, Data: randomBytes(t, pageSize)}
			version, err = st.Commit(ctx, store.CommitRequest{
				Key: key, CommitID: []byte("c2"), LeaseEpoch: opened.Lease.Epoch, BaseVersion: version, PageCount: 4,
				Blocks: []store.Block{again}, Now: now, TTL: time.Minute,
			})
			require.NoError(t, err)
			got, err = st.Fetch(ctx, key, version, 1, 1)
			require.NoError(t, err)
			require.Equal(t, again.Data, got[0])

			// Each part must be at most 1 KB, so it is stored inline.
			var parts, largest int
			require.NoError(t, pool.QueryRow(ctx,
				"SELECT count(*), max(length(data)) FROM blocks WHERE subject = $1 AND db_id = $2 AND idx = 1",
				key.Subject, key.DBID).Scan(&parts, &largest))
			require.Equal(t, max(1, int(pageSize)/1024), parts)
			require.LessOrEqual(t, largest, 1024)
		})
	}
}

func TestBlockPartsMigrationKeepsData(t *testing.T) {
	// Migration 00003 rewrites every block, so an error in it loses data. The test runs it up and down in a separate
	// schema and compares the blocks byte for byte.
	admin := testPool(t)
	ctx := t.Context()
	schema := "migrate_" + randomHex(t)
	_, err := admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	cfg, err := pgxpool.ParseConfig(os.Getenv(envTestDatabaseURL))
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	sqlDB := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = sqlDB.Close() })
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations.FS)
	require.NoError(t, err)

	// Before migration 00003: whole blocks at three page sizes.
	_, err = provider.UpTo(ctx, 2)
	require.NoError(t, err)
	written := map[string][]byte{}
	for _, pageSize := range []int{512, 4096, 65536} {
		dbID := fmt.Sprint("db", pageSize)
		_, err := pool.Exec(ctx, `INSERT INTO databases (subject, db_id, page_size, page_count, version)
			VALUES ('alice', $1, $2, 2, 1)`, dbID, pageSize)
		require.NoError(t, err)
		for idx := range 2 {
			data := randomBytes(t, uint32(pageSize))
			_, err := pool.Exec(ctx, "INSERT INTO blocks (subject, db_id, idx, data) VALUES ('alice', $1, $2, $3)",
				dbID, idx, data)
			require.NoError(t, err)
			written[fmt.Sprint(dbID, "/", idx)] = data
		}
	}

	// After migration 00003: the store must return the same bytes.
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	st := postgres.New(pool)
	for _, pageSize := range []int{512, 4096, 65536} {
		dbID := fmt.Sprint("db", pageSize)
		got, err := st.Fetch(ctx, store.Key{Subject: "alice", DBID: dbID}, 1, 0, 2)
		require.NoError(t, err)
		for idx := range 2 {
			require.Equal(t, written[fmt.Sprint(dbID, "/", idx)], got[idx], "%s block %d after the split into parts", dbID, idx)
		}
	}
	var fillfactor string
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid = 'blocks'::regclass").Scan(&fillfactor))
	require.Equal(t, "fillfactor=50", fillfactor, "the table must have fillfactor 50")

	// After migrating down: whole blocks with the same bytes.
	_, err = provider.DownTo(ctx, 2)
	require.NoError(t, err)
	for key, data := range written {
		var dbID string
		var idx int
		_, err := fmt.Sscanf(strings.Replace(key, "/", " ", 1), "%s %d", &dbID, &idx)
		require.NoError(t, err)
		var back []byte
		require.NoError(t, pool.QueryRow(ctx, "SELECT data FROM blocks WHERE subject = 'alice' AND db_id = $1 AND idx = $2",
			dbID, idx).Scan(&back))
		require.Equal(t, data, back, "%s after joining the parts", key)
	}
}

func randomBytes(t *testing.T, n uint32) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func randomHex(t *testing.T) string {
	t.Helper()
	return hex.EncodeToString(randomBytes(t, 6))
}
