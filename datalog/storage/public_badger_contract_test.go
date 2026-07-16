//go:build !(js && wasm)

package storage_test

import "github.com/wbrown/janus-datalog/datalog/storage"

var _ func(string, *storage.BinaryKeyEncoder) (*storage.BadgerStore, error) = storage.NewBadgerStore
