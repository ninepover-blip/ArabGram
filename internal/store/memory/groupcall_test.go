package memory_test

import (
	"testing"

	"telesrv/internal/store"
	"telesrv/internal/store/memory"
	"telesrv/internal/store/storetest"
)

func TestGroupCallStoreContract(t *testing.T) {
	var nextChannel int64 = 5000
	storetest.RunGroupCallStoreContract(t, func(t *testing.T) (store.GroupCallStore, int64) {
		nextChannel++
		return memory.NewGroupCallStore(), nextChannel
	})
}

func TestGroupCallStoreM2Contract(t *testing.T) {
	var nextChannel int64 = 6000
	storetest.RunGroupCallStoreM2Contract(t, func(t *testing.T) (store.GroupCallStore, int64) {
		nextChannel++
		return memory.NewGroupCallStore(), nextChannel
	})
}
