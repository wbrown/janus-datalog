package main

import (
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A run under -cpuprofile writes a CPU profile of the run to the named file:
// the gzipped profile runtime/pprof writes, read back by go tool pprof.
func TestCLI_ACPUProfileOfTheRunIsWrittenWhereTheFlagNames(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)
	profilePath := filepath.Join(t.TempDir(), "cpu.prof")

	cmd := exec.Command(binPath, "-db", dbPath, "-cpuprofile", profilePath,
		"-query", `[:find ?n :where [?e :person/name ?n]]`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the run ended with %v:\n%s", err, out)
	}

	f, err := os.Open(profilePath)
	if err != nil {
		t.Fatalf("the profile was not written: %v", err)
	}
	defer f.Close()
	unzipped, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("the profile is not the gzipped profile runtime/pprof writes: %v", err)
	}
	content, err := io.ReadAll(unzipped)
	if err != nil {
		t.Fatalf("the profile does not read whole: %v", err)
	}
	if len(content) == 0 {
		t.Fatal("the profile holds no samples")
	}
}

// A run under -memprofile writes an allocation profile of the run to the
// named file: the gzipped allocs profile runtime/pprof writes, read back by
// go tool pprof.
func TestCLI_AnAllocationProfileOfTheRunIsWrittenWhereTheFlagNames(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)
	profilePath := filepath.Join(t.TempDir(), "mem.prof")

	cmd := exec.Command(binPath, "-db", dbPath, "-memprofile", profilePath,
		"-query", `[:find ?n :where [?e :person/name ?n]]`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the run ended with %v:\n%s", err, out)
	}

	f, err := os.Open(profilePath)
	if err != nil {
		t.Fatalf("the profile was not written: %v", err)
	}
	defer f.Close()
	unzipped, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("the profile is not the gzipped profile runtime/pprof writes: %v", err)
	}
	content, err := io.ReadAll(unzipped)
	if err != nil {
		t.Fatalf("the profile does not read whole: %v", err)
	}
	if len(content) == 0 {
		t.Fatal("the profile holds no samples")
	}
}
