package main

import (
	"os/exec"
	"strings"
	"testing"
)

// A vector given by -in to a scalar :in entry and read at a data pattern's
// value position over the edge database. No node's name is the vector, so
// the answer is no tuple, and the process neither panics nor exits.
func TestCLI_AVectorGivenToAScalarInputAnswersNoTuple(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createEdgeDatabase(t)
	query := `[:find ?s :in $ ?n :where [?s :node/name ?n]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			out, err := exec.Command(binPath, "-db", dbPath, mode.cliFlag(), "-query", query, "-in", `["n1" "n2"]`).CombinedOutput()
			if strings.Contains(string(out), "panic") {
				t.Fatalf("the process panicked (err %v):\n%s", err, out)
			}
			if err != nil {
				t.Fatalf("the process exited with %v:\n%s", err, out)
			}
			if !strings.Contains(string(out), "Empty relation") {
				t.Errorf("want no tuple, got:\n%s", out)
			}
		})
	}
}
