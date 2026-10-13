package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// createEdgeDatabase creates a database holding the chain n1 → n2 → n3 → n4
// under :edge, each node named under :node/name.
func createEdgeDatabase(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "edges.db")
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	tx := db.NewTransaction()
	names := []string{"n1", "n2", "n3", "n4"}
	for i, name := range names {
		node := datalog.NewIdentity("node/" + name)
		if err := tx.Add(node, datalog.NewKeyword(":node/name"), name); err != nil {
			t.Fatalf("Failed to add node: %v", err)
		}
		if i > 0 {
			from := datalog.NewIdentity("node/" + names[i-1])
			if err := tx.Add(from, datalog.NewKeyword(":edge"), node); err != nil {
				t.Fatalf("Failed to add edge: %v", err)
			}
		}
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	db.Close()
	return dbPath
}

const closureRules = `[[(reach ?a ?b) [?a :edge ?b]] [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`

const reachQuery = `[:find ?name :in $ % ?start :where [?s :node/name ?start] (reach ?s ?y) [?y :node/name ?name]]`

// TestCLI_RulesInput pins that the -in value at the % position is read as a
// rule set: the closure it derives reaches the table, with ?start bound
// through the -in value after it.
func TestCLI_RulesInput(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createEdgeDatabase(t)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			cmd := exec.Command(binPath, "-db", dbPath, mode.cliFlag(),
				"-query", reachQuery,
				"-in", closureRules,
				"-in", `"n2"`)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Query with rules input failed: %v\n%s", err, out)
			}

			output := string(out)
			for _, want := range []string{"n3", "n4", "_2 tuples"} {
				if !strings.Contains(output, want) {
					t.Errorf("expected %q in the closure from n2, got: %s", want, output)
				}
			}
			for _, absent := range []string{"n1", "n2"} {
				if strings.Contains(output, absent) {
					t.Errorf("%q is not reached from n2, got: %s", absent, output)
				}
			}
		})
	}
}

// TestCLI_PlanOnlyRendersInvocation pins that -plan-only reads the rules
// input and renders the invocation in the plan without executing it.
func TestCLI_PlanOnlyRendersInvocation(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createEdgeDatabase(t)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			cmd := exec.Command(binPath, "-db", dbPath, mode.cliFlag(), "-plan-only",
				"-query", reachQuery,
				"-in", closureRules,
				"-in", `"n2"`)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("-plan-only with rules input failed: %v\n%s", err, out)
			}

			output := string(out)
			for _, want := range []string{"Realized Query Plan:", "(reach ?s ?y)"} {
				if !strings.Contains(output, want) {
					t.Errorf("expected %q in -plan-only output, got: %s", want, output)
				}
			}
			if strings.Contains(output, "tuples_") {
				t.Errorf("-plan-only must not execute the query, got results: %s", output)
			}
		})
	}
}
