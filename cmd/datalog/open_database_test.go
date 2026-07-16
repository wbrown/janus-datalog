package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// createTestEDNDump exports the standard two-person test database to an EDN file.
func createTestEDNDump(t *testing.T) string {
	t.Helper()
	dbPath := createTestDatabase(t)

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ednPath := filepath.Join(t.TempDir(), "dump.edn")
	f, err := os.Create(ednPath)
	if err != nil {
		t.Fatalf("Failed to create EDN file: %v", err)
	}
	defer f.Close()

	if err := db.Export(f); err != nil {
		t.Fatalf("Failed to export: %v", err)
	}
	return ednPath
}

// createTestJDZLDump exports the standard two-person test database to a JDZL file.
func createTestJDZLDump(t *testing.T) string {
	t.Helper()
	dbPath := createTestDatabase(t)

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	jdzlPath := filepath.Join(t.TempDir(), "dump.jdzl")
	f, err := os.Create(jdzlPath)
	if err != nil {
		t.Fatalf("Failed to create JDZL file: %v", err)
	}
	defer f.Close()

	if err := db.ExportBinary(f); err != nil {
		t.Fatalf("Failed to export binary: %v", err)
	}
	return jdzlPath
}

func TestOpenDatabaseOrEDN_BadgerPath(t *testing.T) {
	dbPath := createTestDatabase(t)

	db, cleanup, err := openDatabaseOrEDN(dbPath)
	if err != nil {
		t.Fatalf("openDatabaseOrEDN(badger path) failed: %v", err)
	}
	defer cleanup()

	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [_ :person/name ?name]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 names, got %d", len(results))
	}
}

func TestOpenDatabaseOrEDN_EDNDump(t *testing.T) {
	ednPath := createTestEDNDump(t)

	db, cleanup, err := openDatabaseOrEDN(ednPath)
	if err != nil {
		t.Fatalf("openDatabaseOrEDN(edn dump) failed: %v", err)
	}
	defer cleanup()

	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [_ :person/name ?name]]`))
	if err != nil {
		t.Fatalf("Query against EDN-backed database failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 names from EDN dump, got %d", len(results))
	}
}

func TestOpenDatabaseOrEDN_JDZLDump(t *testing.T) {
	jdzlPath := createTestJDZLDump(t)

	db, cleanup, err := openDatabaseOrEDN(jdzlPath)
	if err != nil {
		t.Fatalf("openDatabaseOrEDN(jdzl dump) failed: %v", err)
	}
	defer cleanup()

	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [_ :person/name ?name]]`))
	if err != nil {
		t.Fatalf("Query against JDZL-backed database failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 names from JDZL dump, got %d", len(results))
	}
}

func TestOpenDatabaseOrEDN_MissingPath(t *testing.T) {
	for _, path := range []string{
		"/nonexistent/path/db",
		"/nonexistent/path/dump.edn",
		"/nonexistent/path/dump.jdzl",
	} {
		_, _, err := openDatabaseOrEDN(path)
		if err == nil {
			t.Errorf("openDatabaseOrEDN(%q) succeeded, want error", path)
			continue
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("openDatabaseOrEDN(%q) error = %q, want mention of 'does not exist'", path, err)
		}
	}
}

func TestCLI_QueryFromEDNDump(t *testing.T) {
	binPath := buildCLI(t)
	ednPath := createTestEDNDump(t)

	cmd := exec.Command(binPath, "-db", ednPath,
		"-query", `[:find ?name :where [_ :person/name ?name]]`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Query against EDN dump failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "Alice") || !strings.Contains(output, "Bob") {
		t.Errorf("Expected Alice and Bob in output:\n%s", output)
	}
}

func TestCLI_StatsFromEDNDump(t *testing.T) {
	binPath := buildCLI(t)
	ednPath := createTestEDNDump(t)

	cmd := exec.Command(binPath, "-db", ednPath, "-stats")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Stats against EDN dump failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "Total datoms") {
		t.Errorf("Expected stats summary in output:\n%s", output)
	}
	if !strings.Contains(output, ":person/name") {
		t.Errorf("Expected :person/name attribute row in output:\n%s", output)
	}
}

func TestCLI_ExportFromEDNDump(t *testing.T) {
	binPath := buildCLI(t)
	ednPath := createTestEDNDump(t)
	exportPath := filepath.Join(t.TempDir(), "reexport.edn")

	cmd := exec.Command(binPath, "-db", ednPath, "-export", exportPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Export from EDN dump failed: %v\n%s", err, out)
	}

	reexported, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read re-exported file: %v", err)
	}
	if !strings.Contains(string(reexported), ":person/name") {
		t.Errorf("Re-exported EDN missing expected datoms:\n%s", reexported)
	}
}

func TestCLI_QueryFromJDZLDump(t *testing.T) {
	binPath := buildCLI(t)
	jdzlPath := createTestJDZLDump(t)

	cmd := exec.Command(binPath, "-db", jdzlPath,
		"-query", `[:find ?name :where [_ :person/name ?name]]`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Query against JDZL dump failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "Alice") || !strings.Contains(output, "Bob") {
		t.Errorf("Expected Alice and Bob in output:\n%s", output)
	}
}

func TestCLI_ExportEDNFromJDZLDump(t *testing.T) {
	binPath := buildCLI(t)
	jdzlPath := createTestJDZLDump(t)
	exportPath := filepath.Join(t.TempDir(), "from-jdzl.edn")

	cmd := exec.Command(binPath, "-db", jdzlPath, "-export", exportPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Export EDN from JDZL dump failed: %v\n%s", err, out)
	}

	reexported, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read re-exported file: %v", err)
	}
	if !strings.Contains(string(reexported), ":person/name") {
		t.Errorf("EDN from JDZL missing expected datoms:\n%s", reexported)
	}
}

func TestCLI_ExportBinFromJDZLDump(t *testing.T) {
	binPath := buildCLI(t)
	jdzlPath := createTestJDZLDump(t)
	exportPath := filepath.Join(t.TempDir(), "recompressed.jdzl")

	cmd := exec.Command(binPath, "-db", jdzlPath, "-export-bin", exportPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Export-bin from JDZL dump failed: %v\n%s", err, out)
	}

	db, cleanup, err := openDatabaseOrEDN(exportPath)
	if err != nil {
		t.Fatalf("open recompressed JDZL failed: %v", err)
	}
	defer cleanup()

	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [_ :person/name ?name]]`))
	if err != nil {
		t.Fatalf("Query recompressed JDZL failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 names from recompressed JDZL, got %d", len(results))
	}
}

func TestCLI_ImportIntoEDNPathRejected(t *testing.T) {
	binPath := buildCLI(t)
	ednPath := createTestEDNDump(t)

	// Importing INTO an .edn path is nonsense (the temp database would be
	// discarded); it must be rejected, not silently accepted.
	cmd := exec.Command(binPath, "-db", filepath.Join(t.TempDir(), "target.edn"), "-import", ednPath)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Error("Expected error when importing into an .edn path")
	}
	if !strings.Contains(string(out), "database directory") {
		t.Errorf("Expected error to explain -db must be a database directory, got: %s", out)
	}
}

func TestCLI_ImportIntoJDZLPathRejected(t *testing.T) {
	binPath := buildCLI(t)
	ednPath := createTestEDNDump(t)
	jdzlPath := createTestJDZLDump(t)
	target := filepath.Join(t.TempDir(), "target.jdzl")

	for _, args := range [][]string{
		{"-db", target, "-import", ednPath},
		{"-db", target, "-import-bin", jdzlPath},
	} {
		cmd := exec.Command(binPath, args...)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Errorf("Expected error for %v", args)
		}
		if !strings.Contains(string(out), "database directory") {
			t.Errorf("Expected database-directory rejection for %v, got: %s", args, out)
		}
	}
}
