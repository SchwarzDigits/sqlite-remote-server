package protocol

import (
	"sync"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
)

// holders maps each database to the connection on this server instance that holds its lease. On a takeover the old
// holder receives a LeaseRevoked push immediately. The store fences the old holder in any case. The push only saves
// it a failed commit.
//
// The map is local to one server instance. If the takeover happens on another instance, the old holder learns about
// it when its next commit fails.
type holders struct {
	mu sync.Mutex
	m  map[store.Key]holder
}

type holder struct {
	epoch   uint64
	owner   *connection
	revoked func(newEpoch uint64)
}

func newHolders() *holders {
	return &holders{m: make(map[store.Key]holder)}
}

// set records owner as the holder of key's lease with epoch. If a different connection held an older epoch, set
// calls its revoked callback.
func (h *holders) set(key store.Key, epoch uint64, owner *connection, revoked func(newEpoch uint64)) {
	h.mu.Lock()
	previous, ok := h.m[key]
	h.m[key] = holder{epoch: epoch, owner: owner, revoked: revoked}
	h.mu.Unlock()

	if ok && previous.owner != owner && previous.epoch < epoch {
		go previous.revoked(epoch)
	}
}

// remove deletes owner as the holder of key. It does nothing if another connection holds the lease by now.
func (h *holders) remove(key store.Key, owner *connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if current, ok := h.m[key]; ok && current.owner == owner {
		delete(h.m, key)
	}
}
