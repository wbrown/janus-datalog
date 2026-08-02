package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// backendPath is where a backend's data goes: a directory for one that persists,
// nothing for one that holds its data in process and rejects a path.
func backendPath(t *testing.T, backend storage.Backend) string {
	t.Helper()
	if backend.Persistent {
		return t.TempDir()
	}
	return ""
}

// storeOfKind opens a backend directly, so a test can compare against the
// concrete store type without naming it: Badger's type does not exist in a wasm
// build, and the backends are distinct types from each other.
func storeOfKind(t *testing.T, backend storage.Backend) storage.Store {
	t.Helper()
	store, err := backend.Open(backendPath(t, backend), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestWithBackendOpensTheNamedBackend(t *testing.T) {
	for _, backend := range storage.AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			d, err := db.Open(backendPath(t, backend), db.WithBackend(backend.Name))
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })
			require.IsType(t, storeOfKind(t, backend), d.Store())

			entity := datalog.NewIdentity("backend:selected")
			tx := d.NewTransaction()
			require.NoError(t, tx.Set(entity, datalog.NewKeyword(":backend/name"), backend.Name))
			_, err = tx.Commit()
			require.NoError(t, err)

			var names []string
			require.NoError(t, d.QueryInto(&names, `[:find ?name :where [?e :backend/name ?name]]`))
			require.Equal(t, []string{backend.Name}, names)
		})
	}
}

// db.Open(path) names the build's default backend, not a specific one: native
// builds persist to Badger, wasm has no filesystem and holds the data in memory.
// The path is what the caller asks for, but which backend answers is the build's
// decision, so the wasm default takes a path it cannot honor rather than
// rejecting portable code. Only an explicitly named backend contradicts itself.
func TestDefaultBackendTakesAPathOnEveryBuild(t *testing.T) {
	d, err := db.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	require.IsType(t, storeOfKind(t, storage.DefaultBackend()), d.Store())
}

func TestNamedBackendRejectsAPathItCannotHonor(t *testing.T) {
	for _, backend := range storage.AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			if backend.Persistent {
				_, err := db.Open("", db.WithBackend(backend.Name))
				require.Error(t, err)
				require.Contains(t, err.Error(), "Path is required")
				return
			}
			_, err := db.Open(t.TempDir(), db.WithBackend(backend.Name))
			require.Error(t, err)
			require.Contains(t, err.Error(), "would be discarded")
		})
	}
}

func TestUnknownBackendNameListsWhatTheBuildHas(t *testing.T) {
	_, err := db.Open("", db.WithBackend("nonesuch"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown storage backend")
	for _, backend := range storage.AvailableBackends() {
		require.Contains(t, err.Error(), backend.Name)
	}
}

// A store answers for itself which backend it is, where its data lives and how
// its encoder is configured. Each of these options answers one of those
// questions a second time, and the store's answer is the one that would win, so
// the contradiction is rejected rather than resolved by precedence.
//
// Passing a store and a path together is no longer expressible: they are the
// same argument.
func TestStoreSourceRejectsASecondAnswer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opt     db.Option
		errText string
	}{
		{"backend", db.WithBackend("memory"), "BackendName"},
		{"compression threshold", db.WithCompressionThreshold(4096), "CompressionThreshold"},
		{"compression disabled", db.WithoutCompression(), "CompressionThreshold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := storage.NewMemoryStore(&storage.BinaryKeyEncoder{})
			t.Cleanup(func() { _ = store.Close() })
			d, err := db.Open(store, tc.opt)
			require.Error(t, err)
			require.Nil(t, d)
			require.Contains(t, err.Error(), tc.errText)
		})
	}
}

func TestCompressionThresholdConfiguresTheOpenedEncoder(t *testing.T) {
	for _, backend := range storage.AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				opts []db.Option
				want int
			}{
				{"default", nil, 512},
				{"explicit", []db.Option{db.WithCompressionThreshold(4096)}, 4096},
				{"disabled", []db.Option{db.WithoutCompression()}, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					opts := append([]db.Option{db.WithBackend(backend.Name)}, tc.opts...)
					d, err := db.Open(backendPath(t, backend), opts...)
					require.NoError(t, err)
					t.Cleanup(func() { _ = d.Close() })
					require.Equal(t, tc.want, d.Store().Encoder().CompressionThreshold)
				})
			}
		})
	}
}

// OpenMemory built its store itself and then passed CompressionThreshold to a
// constructor that takes an injected store's encoder as-is, so the option was
// accepted and discarded. It now selects the backend by name, which is the path
// that builds the encoder.
func TestOpenMemoryHonorsCompressionThreshold(t *testing.T) {
	d, err := db.OpenMemory(db.WithCompressionThreshold(4096))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	require.Equal(t, 4096, d.Store().Encoder().CompressionThreshold)
}
