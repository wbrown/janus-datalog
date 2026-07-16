//go:build !(js && wasm)

package storage

import "testing"

func requireBadgerStore(t testing.TB, db *Database) *BadgerStore {
	t.Helper()
	store, ok := db.Store().(*BadgerStore)
	if !ok {
		t.Fatalf("got store %T, want *BadgerStore", db.Store())
	}
	return store
}
