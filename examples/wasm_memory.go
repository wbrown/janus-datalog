//go:build example

// wasm_memory.go demonstrates the portable memory backend used under js/wasm.
// Browser hosts persist by Export/Import snapshots rather than a filesystem path.
//
// Run from the repository root:
//
//	go run -tags example examples/wasm_memory.go
package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

func main() {
	fmt.Println("=== 1. Open an in-memory database ===")
	database, err := db.OpenMemory(db.WithReplicaID(1))
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	entity := datalog.NewIdentity("item:portable")
	name := datalog.NewKeyword(":item/name")
	tx := database.NewTransaction()
	if err := tx.Set(entity, name, "wasm-ready"); err != nil {
		log.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  wrote :item/name")

	fmt.Println("\n=== 2. Query ===")
	var names []string
	if err := database.QueryInto(
		&names,
		`[:find ?name :where [?entity :item/name ?name]]`,
	); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  names=%v\n", names)

	fmt.Println("\n=== 3. Export snapshot for host persistence ===")
	var snapshot bytes.Buffer
	if err := database.Export(&snapshot); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  exported %d bytes\n", snapshot.Len())

	fmt.Println("\n=== 4. Import into a fresh memory database ===")
	restored, err := db.OpenMemory(db.WithReplicaID(1))
	if err != nil {
		log.Fatal(err)
	}
	defer restored.Close()
	if err := restored.Import(bytes.NewReader(snapshot.Bytes())); err != nil {
		log.Fatal(err)
	}
	names = nil
	if err := restored.QueryInto(
		&names,
		`[:find ?name :where [?entity :item/name ?name]]`,
	); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  restored names=%v\n", names)
}
