//go:build !(js && wasm)

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBenchmarkDatabasePathIsWorkingDirectoryIndependent is the pin for the
// defect that made `make build-testdb` inert: the builder and the benchmarks
// that read what it built run from different working directories —
// cmd/build-testdb from the module root, the storage tests from this package —
// and both resolved the same relative string, so they named different files.
// The Makefile's guard checked one location while its build step filled the
// other, and the target could never satisfy its own check.
func TestBenchmarkDatabasePathIsWorkingDirectoryIndependent(t *testing.T) {
	packageDir, err := os.Getwd()
	require.NoError(t, err)
	root, err := moduleRoot(packageDir)
	require.NoError(t, err)

	fromPackage, err := resolveTestDataPath(BenchmarkDatabasePath)
	require.NoError(t, err)

	t.Run("from the module root", func(t *testing.T) {
		t.Chdir(root)
		fromRoot, err := resolveTestDataPath(BenchmarkDatabasePath)
		require.NoError(t, err)
		require.Equal(t, fromPackage, fromRoot,
			"the builder and the benchmarks must name one file, not one each")
	})

	// The location is not a preference: the benchmarks open it, so it is fixed
	// by them. Pin it so a future edit cannot move the database away from its
	// readers without this failing.
	require.Equal(t,
		filepath.Join(root, "datalog", "storage", "testdata", "ohlc_benchmark.db"),
		fromPackage)
}

// TestModuleRootWalksToGoMod pins the anchor the resolution rests on, including
// its loud failure: outside a module there is no correct place to put the
// database, and writing a useless copy into the current directory is worse than
// refusing.
func TestModuleRootWalksToGoMod(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example\n"), 0o644))
	nested := filepath.Join(root, "a", "b", "c")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	for _, start := range []string{root, nested} {
		found, err := moduleRoot(start)
		require.NoError(t, err)
		// The temp dir may be reached through a symlink (/var → /private/var on
		// darwin); compare resolved forms so the walk is what is under test.
		wantResolved, err := filepath.EvalSymlinks(root)
		require.NoError(t, err)
		gotResolved, err := filepath.EvalSymlinks(found)
		require.NoError(t, err)
		require.Equal(t, wantResolved, gotResolved, "walking up from %s", start)
	}

	t.Run("no module above", func(t *testing.T) {
		outside := t.TempDir()
		// t.TempDir is under the OS temp root, which has no go.mod above it.
		_, err := moduleRoot(outside)
		require.Error(t, err)
	})
}

// TestResolveTestDataPathLeavesAbsolutePathsAlone pins that a caller naming an
// explicit location keeps it — resolution anchors relative paths, it does not
// override the caller.
func TestResolveTestDataPathLeavesAbsolutePathsAlone(t *testing.T) {
	explicit := filepath.Join(t.TempDir(), "somewhere", "custom.db")
	resolved, err := resolveTestDataPath(explicit)
	require.NoError(t, err)
	require.Equal(t, explicit, resolved)
}
