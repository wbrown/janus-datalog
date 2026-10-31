package main

import (
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A run under -cpuprofile or -memprofile writes the profile of the whole
// run to the named file, a run that ends in a failure included: the flag's
// usage names the whole run, and the profile of a failing query is one a
// profiler reads to see where the run spent its time. Every way a run can
// fail is a case: a query that does not parse or fails to execute, an :in
// nothing binds, a plan of a query that does not parse, -plan-only without
// -query, a database that does not exist, two transfer flags, an import of a
// file that does not exist, and statistics over a database that does not
// exist.
func TestCLI_AProfileOfARunEndingInAFailureIsWritten(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)
	missing := filepath.Join(t.TempDir(), "missing")
	cases := []struct {
		name string
		args []string
	}{
		{"a query that does not parse", []string{"-db", dbPath, "-query", `[:find ?n :where`}},
		{"a query that fails to execute", []string{"-db", dbPath, "-query", `[:find ?m :where [?e :person/name ?n] [(+ ?n 1) ?m]]`}},
		{"a query whose :in nothing binds", []string{"-db", dbPath, "-query", `[:find ?n :in $ ?age :where [?p :person/age ?age] [?p :person/name ?n]]`}},
		{"a plan of a query that does not parse", []string{"-db", dbPath, "-plan-only", "-query", `[:find ?n :where`}},
		{"-plan-only without -query", []string{"-db", dbPath, "-plan-only"}},
		{"a database that does not exist", []string{"-db", missing, "-query", `[:find ?n :where [?p :person/name ?n]]`}},
		{"two transfer flags", []string{"-db", dbPath, "-export", filepath.Join(t.TempDir(), "out.edn"), "-import", missing}},
		{"an import of a file that does not exist", []string{"-db", filepath.Join(t.TempDir(), "target.db"), "-import", missing}},
		{"statistics over a database that does not exist", []string{"-db", missing, "-stats"}},
	}
	for _, flag := range []string{"-cpuprofile", "-memprofile"} {
		for _, c := range cases {
			t.Run(flag+"/"+c.name, func(t *testing.T) {
				profilePath := filepath.Join(t.TempDir(), "run.prof")
				args := append([]string{flag, profilePath}, c.args...)
				out, err := exec.Command(binPath, args...).CombinedOutput()
				if err == nil {
					t.Fatalf("the run was to end in a failure and succeeded:\n%s", out)
				}
				f, err := os.Open(profilePath)
				if err != nil {
					t.Fatalf("the profile was not written: %v", err)
				}
				defer f.Close()
				unzipped, err := gzip.NewReader(f)
				if err != nil {
					t.Fatalf("the profile is not the gzipped profile runtime/pprof writes: %v\nthe run said:\n%s", err, out)
				}
				content, err := io.ReadAll(unzipped)
				if err != nil {
					t.Fatalf("the profile does not read whole: %v", err)
				}
				if len(content) == 0 {
					t.Fatal("the profile holds nothing")
				}
			})
		}
	}
}
