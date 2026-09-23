package protocol_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/SchwarzDigits/sqlite-remote-server/internal/gen/sqlite_remote/v1"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/protocol"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/memory"
)

// counting wraps a store and counts the calls to Renew. In the PostgreSQL store each call is a write.
type counting struct {
	store.Store
	renewals atomic.Int64
}

func (s *counting) Renew(ctx context.Context, key store.Key, epoch uint64, now time.Time, ttl time.Duration) error {
	s.renewals.Add(1)
	return s.Store.Renew(ctx, key, epoch, now, ttl)
}

func withCounting(t *testing.T) (*env, *counting) {
	t.Helper()
	st := &counting{Store: memory.New()}
	return start(t, func(o *protocol.Options) { o.Store = st }), st
}

func TestPingRenewsLeaseAfterHalfTTL(t *testing.T) {
	// The lease TTL is 30 s. Open grants the lease, so pings in the first 15 s do not renew it.
	e, st := withCounting(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	a.open(testDB, true, false)

	for _, step := range []struct {
		advance  time.Duration
		renewals int64
	}{
		{5 * time.Second, 0},
		{5 * time.Second, 0},
		{5 * time.Second, 1}, // 15 s: half the TTL has passed
		{5 * time.Second, 1},
		{5 * time.Second, 1},
		{5 * time.Second, 2}, // 15 s after the last renewal
	} {
		e.clock.Advance(step.advance)
		a.call(pingFrame())
		require.Equal(t, step.renewals, st.renewals.Load())
	}
}

func TestCommitDefersPingRenewal(t *testing.T) {
	// A commit renews the lease in the same transaction, so a ping shortly after it does not renew it again.
	e, st := withCounting(t)
	a := e.connect(t, 0xa)
	a.hello("alice")
	epoch := a.open(testDB, true, false).GetLeaseEpoch()

	e.clock.Advance(14 * time.Second)
	a.call(commitFrame(epoch, 0, 1, commitIDOf(1), block(0, 0xa0)))
	e.clock.Advance(10 * time.Second) // 24 s after the open, 10 s after the commit
	a.call(pingFrame())
	require.Equal(t, int64(0), st.renewals.Load(), "the commit renewed the lease 10 s ago")
}

func TestPingingClientKeepsLease(t *testing.T) {
	// Pings renew the lease only after half the TTL. This must never cost a pinging client its lease. A second
	// instance tries to open the database just before every ping, for two minutes, which is four times the TTL.
	for _, gap := range []time.Duration{5 * time.Second, 10 * time.Second} {
		t.Run(gap.String(), func(t *testing.T) {
			e := start(t)
			a := e.connect(t, 0xa)
			a.hello("alice")
			a.open(testDB, true, false)
			b := e.connect(t, 0xb)
			b.hello("alice")

			for elapsed := time.Duration(0); elapsed < 2*time.Minute; elapsed += gap {
				e.clock.Advance(gap - 100*time.Millisecond)
				answer := b.call(openFrame(testDB, false, false, nil))
				require.Equal(t, pb.ErrorCode_ERROR_CODE_LEASE_HELD, answer.GetError().GetCode(),
					"at %s, just before a ping, the lease must still be held: %v", elapsed+gap, answer)
				e.clock.Advance(100 * time.Millisecond)
				a.call(pingFrame())
			}
		})
	}
}
