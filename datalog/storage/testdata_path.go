package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// BenchmarkDatabasePath names the pre-built OHLC benchmark database, from the
// module root. Where it lives is fixed by its readers — the benchmarks in this
// package open it — not chosen by whoever builds it.
//
// Every test-data path is named from the module root and anchored by
// resolveTestDataPath, because the builder and the readers run from different
// directories: cmd/build-testdb from the module root, the package's tests from
// the package. A path relative to the current directory names two different
// files to those two callers, so a builder can fill one location while every
// reader — and the Makefile's own guard — looks at another.
const BenchmarkDatabasePath = "datalog/storage/testdata/ohlc_benchmark.db"

// Anchoring is path arithmetic and builds everywhere, unlike the builder that
// reads these paths: it opens a store, which js/wasm has no backend for. A
// module-root walk shared with that builder would inherit its constraint, and
// TestScanAcquisitionGoesThroughAReport — which walks the module under both
// targets — would fail to compile on one of them.

// moduleRoot walks up from dir to the directory holding go.mod — the anchor
// every test-data path is named from. Outside a module there is no correct
// answer, since the database is only ever read by tests in this repository, so
// this reports the failure rather than letting a caller write a copy that
// nothing will read.
func moduleRoot(dir string) (string, error) {
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf(
				"no go.mod at or above %s: test-data paths are named from the module root", dir)
		}
		dir = parent
	}
}

// resolveTestDataPath anchors a module-root-relative test-data path against the
// module root, so callers in different directories name the same file. An
// absolute path is the caller naming a location explicitly and passes through.
func resolveTestDataPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolving test-data path %s: %w", path, err)
	}
	root, err := moduleRoot(cwd)
	if err != nil {
		return "", fmt.Errorf("resolving test-data path %s: %w", path, err)
	}
	return filepath.Join(root, path), nil
}
