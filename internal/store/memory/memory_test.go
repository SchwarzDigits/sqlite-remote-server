package memory_test

import (
	"testing"

	"github.com/SchwarzDigits/sqlite-remote-server/internal/store"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/memory"
	"github.com/SchwarzDigits/sqlite-remote-server/internal/store/storetest"
)

func TestContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store { return memory.New() })
}
