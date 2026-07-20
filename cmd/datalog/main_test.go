package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// Note: We use db.Query() directly instead of NewExecutor() + Execute()
// because Database provides a simpler API that handles parsing internally.

// buildCLI builds the CLI binary for testing
func buildCLI(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "datalog-test")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = filepath.Dir(os.Args[0])
	// Get the actual source directory
	if wd, err := os.Getwd(); err == nil {
		cmd.Dir = wd
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build CLI: %v\n%s", err, out)
	}
	return binPath
}

// createTestDatabase creates a database with test data
func createTestDatabase(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":person/age"), int64(30))

	bob := datalog.NewIdentity("bob")
	tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
	tx.Add(bob, datalog.NewKeyword(":person/age"), int64(25))

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	db.Close()

	return dbPath
}

func TestCLI_ExportFlag(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)
	exportPath := filepath.Join(t.TempDir(), "export.edn")

	cmd := exec.Command(binPath, "-db", dbPath, "-export", exportPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Export failed: %v\n%s", err, out)
	}

	// Verify output message
	if !strings.Contains(string(out), "Exported database to") {
		t.Errorf("Expected export success message, got: %s", out)
	}

	// Verify file exists and has content
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read export file: %v", err)
	}

	if len(content) == 0 {
		t.Error("Export file is empty")
	}

	// Verify it contains valid EDN vectors
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "[#identity") {
			t.Errorf("Line %d doesn't start with [#identity: %s", i+1, line)
		}
		if !strings.HasSuffix(line, "]") {
			t.Errorf("Line %d doesn't end with ]: %s", i+1, line)
		}
	}
}

func TestCLI_ImportFlag(t *testing.T) {
	binPath := buildCLI(t)

	// Create a source database with test data
	sourceDir := t.TempDir()
	sourceDBPath := filepath.Join(sourceDir, "source.db")
	sourceDB, err := storage.NewDatabase(sourceDBPath)
	if err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}

	tx := sourceDB.NewTransaction()
	entity := datalog.NewIdentity("test-entity")
	tx.Add(entity, datalog.NewKeyword(":test/value"), "hello")
	tx.Add(entity, datalog.NewKeyword(":test/count"), int64(42))
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Export the source database to EDN
	ednPath := filepath.Join(t.TempDir(), "import.edn")
	ednFile, err := os.Create(ednPath)
	if err != nil {
		t.Fatalf("Failed to create EDN file: %v", err)
	}
	if err := sourceDB.Export(ednFile); err != nil {
		t.Fatalf("Failed to export source database: %v", err)
	}
	ednFile.Close()
	sourceDB.Close()

	// Import into new database using CLI
	dbPath := filepath.Join(t.TempDir(), "imported.db")
	cmd := exec.Command(binPath, "-db", dbPath, "-import", ednPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Import failed: %v\n%s", err, out)
	}

	// Verify output message
	if !strings.Contains(string(out), "Imported") {
		t.Errorf("Expected import success message, got: %s", out)
	}

	// Verify database was created and has data
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database was not created")
	}

	// Open and verify data. The import itself doesn't plan a query, so this
	// is the only part of the test that reaches the algebra axis; the CLI's
	// own -import path doesn't accept planner options, so re-query directly
	// with each mode's executor instead of db.Query's fixed default.
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, err := storage.NewDatabase(dbPath)
			if err != nil {
				t.Fatalf("Failed to open imported database: %v", err)
			}
			defer db.Close()

			q, err := parser.ParseQuery(`[:find ?v :where [_ :test/value ?v]]`)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			result, err := executor.CollectTuples(db.NewExecutorWithOptions(mode.plannerOptions()).Execute(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result) != 1 {
				t.Errorf("Expected 1 result, got %d", len(result))
			}
		})
	}
}

func TestCLI_ExportImportRoundTrip(t *testing.T) {
	binPath := buildCLI(t)
	db1Path := createTestDatabase(t)
	exportPath := filepath.Join(t.TempDir(), "roundtrip.edn")
	db2Path := filepath.Join(t.TempDir(), "roundtrip.db")

	// Export db1
	cmd := exec.Command(binPath, "-db", db1Path, "-export", exportPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Export failed: %v\n%s", err, out)
	}

	// Import to db2
	cmd = exec.Command(binPath, "-db", db2Path, "-import", exportPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Import failed: %v\n%s", err, out)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Query both databases and compare
			db1, err := storage.NewDatabase(db1Path)
			if err != nil {
				t.Fatalf("Failed to open db1: %v", err)
			}
			defer db1.Close()

			db2, err := storage.NewDatabase(db2Path)
			if err != nil {
				t.Fatalf("Failed to open db2: %v", err)
			}
			defer db2.Close()

			q, err := parser.ParseQuery(`[:find ?name :where [_ :person/name ?name]]`)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			// Query names from both
			result1, err := executor.CollectTuples(db1.NewExecutorWithOptions(mode.plannerOptions()).Execute(q))
			if err != nil {
				t.Fatalf("Query db1 failed: %v", err)
			}

			result2, err := executor.CollectTuples(db2.NewExecutorWithOptions(mode.plannerOptions()).Execute(q))
			if err != nil {
				t.Fatalf("Query db2 failed: %v", err)
			}

			if len(result1) != len(result2) {
				t.Errorf("Result sizes differ: db1=%d, db2=%d", len(result1), len(result2))
			}
		})
	}
}

func TestCLI_ExportNonexistentDB(t *testing.T) {
	binPath := buildCLI(t)
	exportPath := filepath.Join(t.TempDir(), "export.edn")

	cmd := exec.Command(binPath, "-db", "/nonexistent/path/db", "-export", exportPath)
	out, err := cmd.CombinedOutput()

	// Should fail
	if err == nil {
		t.Error("Expected error for nonexistent database")
	}

	// Should mention database doesn't exist
	if !strings.Contains(string(out), "does not exist") {
		t.Errorf("Expected 'does not exist' error, got: %s", out)
	}
}

func TestCLI_ImportMalformedFile(t *testing.T) {
	binPath := buildCLI(t)

	// Create a valid database and export one line
	sourceDir := t.TempDir()
	sourceDBPath := filepath.Join(sourceDir, "source.db")
	sourceDB, err := storage.NewDatabase(sourceDBPath)
	if err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}

	tx := sourceDB.NewTransaction()
	entity := datalog.NewIdentity("test-entity")
	tx.Add(entity, datalog.NewKeyword(":test/ok"), int64(1))
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Export to get valid EDN line
	var validEDN strings.Builder
	if err := sourceDB.Export(&validEDN); err != nil {
		t.Fatalf("Failed to export: %v", err)
	}
	sourceDB.Close()

	// Get just the first non-txInstant line (our actual data)
	lines := strings.Split(strings.TrimSpace(validEDN.String()), "\n")
	var validLine string
	for _, line := range lines {
		if strings.Contains(line, ":test/ok") {
			validLine = line
			break
		}
	}
	if validLine == "" {
		t.Fatal("Could not find valid test line in export")
	}

	// Create malformed EDN file with valid line, then garbage, then valid line
	ednPath := filepath.Join(t.TempDir(), "malformed.edn")
	ednContent := validLine + "\nnot valid edn at all\n" + validLine + "\n"
	if err := os.WriteFile(ednPath, []byte(ednContent), 0644); err != nil {
		t.Fatalf("Failed to write EDN file: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "malformed.db")
	cmd := exec.Command(binPath, "-db", dbPath, "-import", ednPath)
	out, err := cmd.CombinedOutput()

	// Should fail
	if err == nil {
		t.Error("Expected error for malformed file")
	}

	// Should mention line number
	if !strings.Contains(string(out), "line 2") {
		t.Errorf("Expected error to mention line 2, got: %s", out)
	}
}

func TestCLI_BothFlagsError(t *testing.T) {
	binPath := buildCLI(t)

	cmd := exec.Command(binPath, "-db", "test.db", "-export", "out.edn", "-import", "in.edn")
	out, err := cmd.CombinedOutput()

	// Should fail
	if err == nil {
		t.Error("Expected error when both flags specified")
	}

	if !strings.Contains(string(out), "Specify only one of -export, -import, -export-bin, -import-bin") {
		t.Errorf("Expected mutually-exclusive transfer-mode error, got: %s", out)
	}
}

func TestCLI_BinaryFlagsMutualExclusion(t *testing.T) {
	binPath := buildCLI(t)
	cases := [][]string{
		{"-export-bin", "out.jdzl", "-import-bin", "in.jdzl"},
		{"-export", "out.edn", "-export-bin", "out.jdzl"},
		{"-import", "in.edn", "-import-bin", "in.jdzl"},
	}
	for _, args := range cases {
		cmdArgs := append([]string{"-db", "test.db"}, args...)
		cmd := exec.Command(binPath, cmdArgs...)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Errorf("expected mutual-exclusion error for %v", args)
		}
		if !strings.Contains(string(out), "Specify only one of -export, -import, -export-bin, -import-bin") {
			t.Errorf("expected transfer-mode error for %v, got: %s", args, out)
		}
	}
}

func TestCLI_StatsWithBinaryFlagError(t *testing.T) {
	binPath := buildCLI(t)
	cases := [][]string{
		{"-stats", "-export-bin", "out.jdzl"},
		{"-stats", "-import-bin", "in.jdzl"},
		{"-stats", "-export", "out.edn"},
		{"-stats", "-import", "in.edn"},
	}
	for _, args := range cases {
		cmdArgs := append([]string{"-db", "test.db"}, args...)
		cmd := exec.Command(binPath, cmdArgs...)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Errorf("expected error combining -stats with %v", args)
		}
		if !strings.Contains(string(out), "Cannot combine -stats with export/import flags") {
			t.Errorf("expected -stats guard error for %v, got: %s", args, out)
		}
	}
}

func TestCLI_ExportBinFlag(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)
	exportPath := filepath.Join(t.TempDir(), "export.jdzl")

	cmd := exec.Command(binPath, "-db", dbPath, "-export-bin", exportPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Export-bin failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Exported database to") {
		t.Errorf("Expected export success message, got: %s", out)
	}
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read binary export: %v", err)
	}
	if len(content) < 4 || string(content[0:4]) != "JDZL" {
		t.Errorf("Expected JDZL magic, got prefix %q (len=%d)", content, len(content))
	}
}

func TestCLI_ImportBinFlag(t *testing.T) {
	binPath := buildCLI(t)

	sourceDir := t.TempDir()
	sourceDBPath := filepath.Join(sourceDir, "source.db")
	sourceDB, err := storage.NewDatabase(sourceDBPath)
	if err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}
	tx := sourceDB.NewTransaction()
	entity := datalog.NewIdentity("test-entity")
	tx.Add(entity, datalog.NewKeyword(":test/value"), "hello")
	tx.Add(entity, datalog.NewKeyword(":test/count"), int64(42))
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	jdzlPath := filepath.Join(t.TempDir(), "import.jdzl")
	jdzlFile, err := os.Create(jdzlPath)
	if err != nil {
		t.Fatalf("Failed to create jdzl file: %v", err)
	}
	if err := sourceDB.ExportBinary(jdzlFile); err != nil {
		t.Fatalf("Failed to export source database: %v", err)
	}
	jdzlFile.Close()
	sourceDB.Close()

	dbPath := filepath.Join(t.TempDir(), "imported.db")
	cmd := exec.Command(binPath, "-db", dbPath, "-import-bin", jdzlPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Import-bin failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Imported") {
		t.Errorf("Expected import success message, got: %s", out)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, err := storage.NewDatabase(dbPath)
			if err != nil {
				t.Fatalf("Failed to open imported database: %v", err)
			}
			defer db.Close()

			q, err := parser.ParseQuery(`[:find ?v :where [_ :test/value ?v]]`)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			result, err := executor.CollectTuples(db.NewExecutorWithOptions(mode.plannerOptions()).Execute(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result) != 1 {
				t.Errorf("Expected 1 result, got %d", len(result))
			}
		})
	}
}

func TestCLI_ExportImportBinRoundTrip(t *testing.T) {
	binPath := buildCLI(t)
	db1Path := createTestDatabase(t)
	exportPath := filepath.Join(t.TempDir(), "roundtrip.jdzl")
	db2Path := filepath.Join(t.TempDir(), "roundtrip.db")

	cmd := exec.Command(binPath, "-db", db1Path, "-export-bin", exportPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Export-bin failed: %v\n%s", err, out)
	}
	cmd = exec.Command(binPath, "-db", db2Path, "-import-bin", exportPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Import-bin failed: %v\n%s", err, out)
	}

	db1, err := storage.NewDatabase(db1Path)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	defer db1.Close()
	db2, err := storage.NewDatabase(db2Path)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	var edn1, edn2 bytes.Buffer
	if err := db1.Export(&edn1); err != nil {
		t.Fatalf("export db1 edn: %v", err)
	}
	if err := db2.Export(&edn2); err != nil {
		t.Fatalf("export db2 edn: %v", err)
	}
	if edn1.String() != edn2.String() {
		t.Error("binary CLI round-trip produced different EDN dumps")
	}
}

func TestCLI_ImportBinNonexistentFile(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := filepath.Join(t.TempDir(), "test.db")
	cmd := exec.Command(binPath, "-db", dbPath, "-import-bin", "/nonexistent/file.jdzl")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Error("Expected error for nonexistent import-bin file")
	}
	if !strings.Contains(string(out), "does not exist") {
		t.Errorf("Expected 'does not exist' error, got: %s", out)
	}
}

func TestCLI_QueryWithScalarInput(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)

	// Query with scalar input: find person with age 30
	cmd := exec.Command(binPath, "-db", dbPath,
		"-query", `[:find ?name :in $ ?age :where [?p :person/age ?age] [?p :person/name ?name]]`,
		"-in", "30")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Query with scalar input failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "Alice") {
		t.Errorf("Expected Alice (age 30), got: %s", output)
	}
	if strings.Contains(output, "Bob") {
		t.Errorf("Should not contain Bob (age 25), got: %s", output)
	}
}

func TestCLI_QueryWithCollectionInput(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)

	// Query with collection input: find people with age 25 or 30
	cmd := exec.Command(binPath, "-db", dbPath,
		"-query", `[:find ?name :in $ [?age ...] :where [?p :person/age ?age] [?p :person/name ?name]]`,
		"-in", "[25 30]")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Query with collection input failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "Alice") {
		t.Errorf("Expected Alice, got: %s", output)
	}
	if !strings.Contains(output, "Bob") {
		t.Errorf("Expected Bob, got: %s", output)
	}
}

func TestCLI_QueryWithStringInput(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)

	// Query with string scalar input
	cmd := exec.Command(binPath, "-db", dbPath,
		"-query", `[:find ?age :in $ ?name :where [?p :person/name ?name] [?p :person/age ?age]]`,
		"-in", `"Bob"`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Query with string input failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "25") {
		t.Errorf("Expected 25 (Bob's age), got: %s", output)
	}
}

func TestCLI_QueryWithMultipleInputs(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)

	// Query with two scalar inputs
	cmd := exec.Command(binPath, "-db", dbPath,
		"-query", `[:find ?name :in $ ?min-age ?max-age :where [?p :person/age ?age] [?p :person/name ?name] [(>= ?age ?min-age)] [(<= ?age ?max-age)]]`,
		"-in", "26", "-in", "35")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Query with multiple inputs failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "Alice") {
		t.Errorf("Expected Alice (age 30), got: %s", output)
	}
	if strings.Contains(output, "Bob") {
		t.Errorf("Should not contain Bob (age 25), got: %s", output)
	}
}

func TestCLI_ImportNonexistentFile(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := filepath.Join(t.TempDir(), "test.db")

	cmd := exec.Command(binPath, "-db", dbPath, "-import", "/nonexistent/file.edn")
	out, err := cmd.CombinedOutput()

	// Should fail
	if err == nil {
		t.Error("Expected error for nonexistent import file")
	}

	// Should mention file doesn't exist
	if !strings.Contains(string(out), "does not exist") {
		t.Errorf("Expected 'does not exist' error, got: %s", out)
	}
}

// TestCLI_BareInvocationDoesNotWrite is the regression test for demo-mode
// removal: invoking the CLI with no mode flags must never commit data.
// (Demo mode used to auto-populate an empty database with sample datoms.)
func TestCLI_BareInvocationDoesNotWrite(t *testing.T) {
	binPath := buildCLI(t)

	// Create an EMPTY database (directory exists, no datoms).
	dbPath := filepath.Join(t.TempDir(), "empty.db")
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	db.Close()

	cmd := exec.Command(binPath, "-db", dbPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Bare invocation failed: %v\n%s", err, out)
	}

	// Should print the usage hint, not load demo data.
	if strings.Contains(string(out), "demo") || strings.Contains(string(out), "Demo") {
		t.Errorf("Bare invocation mentions demo mode:\n%s", out)
	}
	if !strings.Contains(string(out), "-query") {
		t.Errorf("Expected usage hint mentioning -query, got:\n%s", out)
	}

	// The database must still be empty.
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, err := storage.NewDatabase(dbPath)
			if err != nil {
				t.Fatalf("Failed to reopen database: %v", err)
			}
			defer db.Close()

			q, err := parser.ParseQuery(`[:find ?e :where [?e _ _]]`)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			results, err := executor.CollectTuples(db.NewExecutorWithOptions(mode.plannerOptions()).Execute(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != 0 {
				t.Errorf("Bare invocation wrote %d entities into an empty database", len(results))
			}
		})
	}
}
