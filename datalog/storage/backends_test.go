package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestAvailableBackendsMatchesTheBuild(t *testing.T) {
	names := make([]string, 0, len(AvailableBackends()))
	for _, backend := range AvailableBackends() {
		names = append(names, backend.Name)
	}
	require.Equal(t, expectedBackendNames(), names)
}

// TestEveryAvailableBackendRoundTrips is what makes the list a claim about
// working stores rather than a list of names: every entry opens, takes a datom
// and gives it back.
func TestEveryAvailableBackendRoundTrips(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			store, err := backend.Open(t.TempDir(), &BinaryKeyEncoder{})
			require.NoError(t, err)
			defer store.Close()

			entity := datalog.NewIdentity("backend:round-trip")
			attr := datalog.NewKeyword(":backend/value")
			require.NoError(t, store.Assert([]datalog.Datom{{
				E: entity, A: attr, V: int64(7),
				Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
			}}))

			iter, err := store.Scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{entity}})
			require.NoError(t, err)
			defer iter.Close()
			require.True(t, iter.Next())
			datom, err := iter.Datom()
			require.NoError(t, err)
			require.Equal(t, int64(7), datom.V)
			require.NoError(t, iter.Error())
		})
	}
}

func TestBackendNamedResolvesEveryAvailableBackend(t *testing.T) {
	for _, backend := range AvailableBackends() {
		resolved, err := BackendNamed(backend.Name)
		require.NoError(t, err)
		require.Equal(t, backend.Name, resolved.Name)
		require.Equal(t, backend.Persistent, resolved.Persistent)
	}
}

// TestBackendNamedRejectsUnknownAndNamesWhatExists: the available set differs
// between a native binary and a wasm one, so the error has to say which build
// is answering rather than leaving the caller to guess.
func TestBackendNamedRejectsUnknownAndNamesWhatExists(t *testing.T) {
	_, err := BackendNamed("no-such-backend")
	require.ErrorContains(t, err, "no-such-backend")
	for _, backend := range AvailableBackends() {
		require.ErrorContains(t, err, backend.Name)
	}
}

func TestDefaultBackendIsAvailable(t *testing.T) {
	names := make([]string, 0, len(AvailableBackends()))
	for _, backend := range AvailableBackends() {
		names = append(names, backend.Name)
	}
	require.Contains(t, names, DefaultBackend().Name)
}

// TestInProcessBackendsIgnorePath pins what Persistent means: a backend that
// reports false keeps nothing at Path, so two stores opened on the same path do
// not share data. A test that reopens a path expecting its data back is asking
// for a persistent backend.
func TestInProcessBackendsIgnorePath(t *testing.T) {
	for _, backend := range AvailableBackends() {
		if backend.Persistent {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			path := t.TempDir()
			first, err := backend.Open(path, &BinaryKeyEncoder{})
			require.NoError(t, err)
			defer first.Close()

			entity := datalog.NewIdentity("backend:not-persisted")
			require.NoError(t, first.Assert([]datalog.Datom{{
				E: entity, A: datalog.NewKeyword(":backend/value"), V: int64(1),
				Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
			}}))

			second, err := backend.Open(path, &BinaryKeyEncoder{})
			require.NoError(t, err)
			defer second.Close()
			iter, err := second.Scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{entity}})
			require.NoError(t, err)
			defer iter.Close()
			require.False(t, iter.Next())
			require.NoError(t, iter.Error())
		})
	}
}
