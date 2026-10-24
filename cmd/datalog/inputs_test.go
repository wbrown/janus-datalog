package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestCLI_QueryDeclaringAnInputWithNoInFlagIsRefused pins that a -query whose
// :in binds a variable, run with no -in for it, is refused naming the count
// given and the count the :in binds, and never answered with the variable
// free.
func TestCLI_QueryDeclaringAnInputWithNoInFlagIsRefused(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createEdgeDatabase(t)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			cmd := exec.Command(binPath, "-db", dbPath, mode.cliFlag(),
				"-query", `[:find ?name :in $ ?start :where [?s :node/name ?start] [?s :node/name ?name]]`)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("a query binding ?start with no -in was answered:\n%s", out)
			}

			output := string(out)
			for _, want := range []string{"Execution error", "0 input relations were given", "binds 1"} {
				if !strings.Contains(output, want) {
					t.Errorf("expected %q in the refusal, got: %s", want, output)
				}
			}
			if strings.Contains(output, "tuples") {
				t.Errorf("a refused query must print no table, got: %s", output)
			}
		})
	}
}
