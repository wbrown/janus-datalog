//go:build example

// annotations.go demonstrates query observability: tracing query
// execution with WithVerbose and custom annotation handlers.
//
// Run from the repository root:
//
//	go run -tags example examples/annotations.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/db"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "annotations-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== 1. WithVerbose: trace to stdout ===")
	// The simplest way to see what the query engine is doing.
	// WithVerbose prints a human-readable trace for every query.
	d1, err := db.Open(tmpDir+"/verbose.db", db.WithVerbose())
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d1.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Println("  Running query with verbose tracing:")
	var names []string
	err = d1.QueryInto(&names,
		`[:find ?name
		  :where [?e :person/name ?name]
		         [?e :person/city "San Francisco"]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n  Result: %d people in SF\n", len(names))
	d1.Close()

	fmt.Println("\n=== 2. Custom handler: collect events ===")
	// WithAnnotationHandler gives you programmatic access to every
	// event the query engine emits. Events have Name, Latency, and Data.
	var events []annotations.Event
	handler := func(event annotations.Event) {
		events = append(events, event)
	}

	d2, err := db.Open(tmpDir+"/handler.db", db.WithAnnotationHandler(handler))
	if err != nil {
		log.Fatal(err)
	}

	f, err = os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d2.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	// Clear events from import, then run a query
	events = nil

	err = d2.QueryInto(&names,
		`[:find ?name
		  :where [?e :person/name ?name]
		         [?e :person/department ?dept]
		         [?dept :dept/name "Engineering"]]`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Query returned %d engineers\n", len(names))
	fmt.Printf("  Captured %d annotation events:\n", len(events))
	for _, e := range events {
		latencyStr := ""
		if e.Latency > 0 {
			latencyStr = fmt.Sprintf(" (%v)", e.Latency)
		}
		fmt.Printf("    %s%s\n", e.Name, latencyStr)
	}

	fmt.Println("\n=== 3. Event details ===")
	// Events carry a Data map with operation-specific metrics.
	fmt.Println("  Events with data:")
	for _, e := range events {
		if len(e.Data) > 0 {
			fmt.Printf("    %s:\n", e.Name)
			for k, v := range e.Data {
				fmt.Printf("      %s = %v\n", k, v)
			}
		}
	}

	fmt.Println("\n=== 4. WithVerboseCallback: custom string output ===")
	// WithVerboseCallback receives pre-formatted strings.
	// Useful for logging frameworks that expect string input.
	var logLines []string
	d3, err := db.Open(tmpDir+"/callback.db",
		db.WithVerboseCallback(func(line string) {
			logLines = append(logLines, line)
		}))
	if err != nil {
		log.Fatal(err)
	}

	f, err = os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d3.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	logLines = nil // clear import lines
	d3.QueryInto(&names, `[:find ?name :where [?e :person/name ?name]]`)

	fmt.Printf("  Captured %d log lines:\n", len(logLines))
	for i, line := range logLines {
		if i >= 5 {
			fmt.Printf("    ... and %d more\n", len(logLines)-5)
			break
		}
		fmt.Printf("    %s\n", line)
	}

	d2.Close()
	d3.Close()
}
