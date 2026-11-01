package main

import (
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// -cpuprofile writes the profile of the whole run whatever ends it, so a
// run whose -memprofile cannot be created or written still leaves the CPU
// profile runtime/pprof writes, gzipped and holding something.
func TestCLI_ACPUProfileIsWrittenWhenTheAllocationProfileFails(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)
	cases := []struct {
		name    string
		memPath string
	}{
		{"an allocation profile in a directory that does not exist", filepath.Join(t.TempDir(), "missing", "mem.prof")},
		{"an allocation profile on a device that takes no write", "/dev/full"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cpuPath := filepath.Join(t.TempDir(), "cpu.prof")
			out, err := exec.Command(binPath, "-cpuprofile", cpuPath, "-memprofile", c.memPath,
				"-db", dbPath, "-query", `[:find ?n :where [?p :person/name ?n]]`).CombinedOutput()
			if err == nil {
				t.Fatalf("the run was to end in a failure and succeeded:\n%s", out)
			}
			f, err := os.Open(cpuPath)
			if err != nil {
				t.Fatalf("the CPU profile was not written: %v", err)
			}
			defer f.Close()
			unzipped, err := gzip.NewReader(f)
			if err != nil {
				t.Fatalf("the CPU profile is not the gzipped profile runtime/pprof writes: %v\nthe run said:\n%s", err, out)
			}
			content, err := io.ReadAll(unzipped)
			if err != nil || len(content) == 0 {
				t.Fatalf("the CPU profile does not read whole or holds nothing: %v", err)
			}
		})
	}
}
