//go:build example

// export_import.go demonstrates database backup and restore using
// the EDN export/import format.
//
// Run from the repository root:
//
//	go run -tags example examples/export_import.go
package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "export-import-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== 1. Create and populate a database ===")
	d1, err := db.Open(tmpDir + "/source.db")
	if err != nil {
		log.Fatal(err)
	}

	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	carol := datalog.NewIdentity("person:carol")
	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	city := datalog.NewKeyword(":person/city")

	tx := d1.NewTransaction()
	tx.Add(alice, name, "Alice")
	tx.Add(alice, age, int64(30))
	tx.Add(alice, city, "San Francisco")
	tx.Add(bob, name, "Bob")
	tx.Add(bob, age, int64(25))
	tx.Add(bob, city, "New York")
	tx.Add(carol, name, "Carol")
	tx.Add(carol, age, int64(35))
	tx.Add(carol, city, "Chicago")
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Created database with 3 people")

	fmt.Println("\n=== 2. Export to EDN format ===")
	// Export writes one datom per line in EDN format.
	var buf bytes.Buffer
	if err := d1.Export(&buf); err != nil {
		log.Fatal(err)
	}
	exportedData := buf.String()
	lines := strings.Split(strings.TrimSpace(exportedData), "\n")
	fmt.Printf("  Exported %d datoms (%d bytes)\n", len(lines), len(exportedData))

	fmt.Println("\n  First 5 lines of export:")
	for i, line := range lines {
		if i >= 5 {
			fmt.Printf("  ...\n")
			break
		}
		// Truncate long lines for display
		if len(line) > 100 {
			line = line[:97] + "..."
		}
		fmt.Printf("  %s\n", line)
	}

	fmt.Println("\n=== 3. Import into a new database ===")
	d2, err := db.Open(tmpDir + "/restored.db")
	if err != nil {
		log.Fatal(err)
	}

	if err := d2.Import(bytes.NewReader(buf.Bytes())); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Imported into fresh database")

	fmt.Println("\n=== 4. Verify round-trip ===")
	// Query the restored database to confirm all data survived.
	type PersonResult struct {
		Name string `datalog:"?name"`
		Age  int64  `datalog:"?age"`
		City string `datalog:"?city"`
	}
	var results []PersonResult
	err = d2.QueryInto(&results,
		`[:find ?name ?age ?city
		  :where [?e :person/name ?name]
		         [?e :person/age ?age]
		         [?e :person/city ?city]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Restored %d people:\n", len(results))
	for _, r := range results {
		fmt.Printf("    %s (age %d) in %s\n", r.Name, r.Age, r.City)
	}

	// Verify specific values match
	aliceName, ok, _ := d2.GetString(alice, name)
	if ok && aliceName == "Alice" {
		fmt.Println("  Verification: Alice's name matches")
	}

	fmt.Println("\n=== 5. Compressed export ===")
	// ExportCompressed uses #lzj tagged literals for large string values.
	// For small values like these, it's identical to normal export.
	var compBuf bytes.Buffer
	if err := d1.ExportCompressed(&compBuf); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Compressed export: %d bytes (normal: %d bytes)\n",
		compBuf.Len(), buf.Len())

	d1.Close()
	d2.Close()
}
