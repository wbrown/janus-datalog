package db

import "github.com/wbrown/janus-datalog/datalog/storage"

// SnapshotInfo re-exports storage.SnapshotInfo so db-package users can name the type
// returned by (*DB).Snapshot without importing the storage package directly. The Snapshot,
// AsOfSnapshot, Snapshots, DeleteSnapshot, and TruncateTo methods are defined on
// storage.Database (aliased as DB), so they are available on *DB automatically.
type SnapshotInfo = storage.SnapshotInfo

var (
	// ErrSnapshotExists is returned by Snapshot when the name is already taken.
	ErrSnapshotExists = storage.ErrSnapshotExists
	// ErrSnapshotNotFound is returned when a named snapshot does not exist.
	ErrSnapshotNotFound = storage.ErrSnapshotNotFound
	// ErrRollbackInProgress is returned by a write started while a TruncateTo is running.
	ErrRollbackInProgress = storage.ErrRollbackInProgress
)
