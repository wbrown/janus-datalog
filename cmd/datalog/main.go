package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	var dbPath string
	var interactive bool
	var help bool
	var verbose bool
	var queryStr string
	var enableDecorrelation bool

	flag.StringVar(&dbPath, "db", "", "database path")
	flag.BoolVar(&interactive, "i", false, "interactive mode")
	flag.BoolVar(&help, "h", false, "show help")
	flag.BoolVar(&verbose, "verbose", false, "verbose mode (show query annotations)")
	flag.StringVar(&queryStr, "query", "", "run a single query and exit")
	flag.BoolVar(&enableDecorrelation, "decorrelate", true, "enable subquery decorrelation optimization (default: true)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [database_path]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "A Datalog query engine with persistent storage.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s                    # Run demo with default database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s mydata.db          # Run demo with specific database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i                 # Interactive mode with default database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i mydata.db      # Interactive mode with specific database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -db /path/to/db   # Using -db flag\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -verbose           # Verbose mode with query annotations\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -verbose -i        # Interactive mode with annotations\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -query '[:find ?x :where [?x :person/name _]]'  # Run single query\n", os.Args[0])
	}
	flag.Parse()

	if help {
		flag.Usage()
		os.Exit(0)
	}

	// Check for positional argument
	if dbPath == "" && flag.NArg() > 0 {
		dbPath = flag.Arg(0)
	}

	// Default to datalog.db if no path specified
	if dbPath == "" {
		dbPath = "datalog.db"
	}

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Database does not exist: %s", dbPath)
	}

	// Open database
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create annotation handler if verbose mode
	var handler annotations.Handler
	if verbose {
		formatter := annotations.NewOutputFormatter(os.Stderr)
		handler = annotations.Handler(formatter.Handle)
	}

	if queryStr != "" {
		// Run single query mode
		runSingleQuery(db, handler, queryStr, enableDecorrelation)
	} else if interactive {
		runInteractive(db, handler, enableDecorrelation)
	} else {
		// Check if database is empty before running demo
		if isDatabaseEmpty(db) {
			fmt.Println("Database is empty, loading demo data...")
			runDemo(db, handler, enableDecorrelation)
		} else {
			fmt.Println("Database contains data. Use -i for interactive mode or -query to run a query.")
		}
	}
}

func runDemo(db *storage.Database, handler annotations.Handler, enableDecorrelation bool) {
	fmt.Println("=== Janus Datalog Demo ===")

	// Create a transaction
	tx := db.NewTransaction()

	// Add some test data
	fmt.Println("\nAdding test data...")

	// Add people
	alice, _ := tx.AddMap(map[string]interface{}{
		":person/name": "Alice",
		":person/age":  int64(30),
		":person/city": "New York",
	})

	bob, _ := tx.AddMap(map[string]interface{}{
		":person/name": "Bob",
		":person/age":  int64(25),
		":person/city": "Boston",
	})

	charlie, _ := tx.AddMap(map[string]interface{}{
		":person/name": "Charlie",
		":person/age":  int64(35),
		":person/city": "New York",
	})

	// Add friendships
	tx.Add(alice, datalog.NewKeyword(":person/friend"), bob)
	tx.Add(alice, datalog.NewKeyword(":person/friend"), charlie)
	tx.Add(bob, datalog.NewKeyword(":person/friend"), charlie)

	// Commit transaction
	txID, err := tx.Commit()
	if err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}
	fmt.Printf("Committed transaction %d\n", txID)

	// Run some queries
	fmt.Println("\n=== Running Queries ===")

	queries := []string{
		// Find all people
		`[:find ?name ?age
		  :where [?p :person/name ?name]
		         [?p :person/age ?age]]`,

		// Find people in New York
		`[:find ?name
		  :where [?p :person/name ?name]
		         [?p :person/city "New York"]]`,

		// Find Alice's friends
		`[:find ?friend-name
		  :where [?alice :person/name "Alice"]
		         [?alice :person/friend ?friend]
		         [?friend :person/name ?friend-name]]`,

		// Find people over 25
		`[:find ?name ?age
		  :where [?p :person/name ?name]
		         [?p :person/age ?age]
		         [(> ?age 25)]]`,

		// Calculate age in 5 years
		`[:find ?name ?age ?future-age
		  :where [?p :person/name ?name]
		         [?p :person/age ?age]
		         [(+ ?age 5) ?future-age]]`,
	}

	// Create executor with optimizations
	opts := storage.DefaultPlannerOptions()
	opts.EnableSubqueryDecorrelation = enableDecorrelation
	exec := db.NewExecutorWithOptions(opts)

	for _, queryStr := range queries {
		fmt.Printf("\nQuery: %s\n", queryStr)

		// Parse query
		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		// Execute query
		var result executor.Relation
		if handler != nil {
			ctx := executor.NewContext(handler)
			result, err = exec.ExecuteWithContext(ctx, q)
		} else {
			result, err = exec.Execute(q)
		}
		if err != nil {
			fmt.Printf("Execution error: %v\n", err)
			continue
		}

		// Display results as markdown table
		fmt.Println(result.Table())
	}
}

func runInteractive(db *storage.Database, handler annotations.Handler, enableDecorrelation bool) {
	fmt.Println("=== Janus Datalog Interactive Mode ===")
	fmt.Println("Commands:")
	fmt.Println("  .help    - Show help")
	fmt.Println("  .exit    - Exit")
	fmt.Println("  .add     - Start adding data")
	fmt.Println("  [:find ...] - Run a query")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	opts := storage.DefaultPlannerOptions()
	opts.EnableSubqueryDecorrelation = enableDecorrelation
	exec := db.NewExecutorWithOptions(opts)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		switch {
		case line == ".exit":
			return

		case line == ".help":
			fmt.Println("Enter Datalog queries or commands")

		case line == ".add":
			addInteractiveData(db, scanner)

		case strings.HasPrefix(line, "[:find"):
			// Collect multi-line query
			query := line
			for !strings.HasSuffix(line, "]") {
				fmt.Print("  ")
				if !scanner.Scan() {
					return
				}
				line = scanner.Text()
				query += "\n" + line
			}

			// Parse and execute
			q, err := parser.ParseQuery(query)
			if err != nil {
				fmt.Printf("Parse error: %v\n", err)
				continue
			}

			var result executor.Relation
			if handler != nil {
				ctx := executor.NewContext(handler)
				result, err = exec.ExecuteWithContext(ctx, q)
			} else {
				result, err = exec.Execute(q)
			}
			if err != nil {
				fmt.Printf("Execution error: %v\n", err)
				continue
			}

			// Display results as markdown table
			fmt.Println(result.Table())

		default:
			fmt.Println("Unknown command. Use .help for help.")
		}
	}
}

func addInteractiveData(db *storage.Database, scanner *bufio.Scanner) {
	fmt.Println("Adding data (empty line to finish):")

	tx := db.NewTransaction()
	count := 0

	for {
		fmt.Print("  entity attribute value> ")
		if !scanner.Scan() {
			tx.Rollback()
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			break
		}

		parts := strings.Fields(line)
		if len(parts) != 3 {
			fmt.Println("Expected: <entity> <attribute> <value>")
			continue
		}

		e := datalog.NewIdentity(parts[0])
		a := datalog.NewKeyword(parts[1])
		v := parseValue(parts[2])

		if err := tx.Add(e, a, v); err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		count++
	}

	if count > 0 {
		txID, err := tx.Commit()
		if err != nil {
			fmt.Printf("Commit failed: %v\n", err)
		} else {
			fmt.Printf("Committed %d datoms in transaction %d\n", count, txID)
		}
	} else {
		tx.Rollback()
		fmt.Println("No data added")
	}
}

func parseValue(s string) interface{} {
	// Try to parse as number
	if n, err := fmt.Sscanf(s, "%d", new(int64)); err == nil && n == 1 {
		var val int64
		fmt.Sscanf(s, "%d", &val)
		return val
	}

	// Try to parse as float
	if n, err := fmt.Sscanf(s, "%f", new(float64)); err == nil && n == 1 {
		var val float64
		fmt.Sscanf(s, "%f", &val)
		return val
	}

	// Remove quotes if present
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}

	// Default to string
	return s
}

// isDatabaseEmpty checks if the database contains any data
func isDatabaseEmpty(db *storage.Database) bool {
	// Try a simple query to see if there's any data
	query := `[:find ?e :where [?e _ _]]`

	exec := executor.NewExecutor(db.Matcher())
	q, err := parser.ParseQuery(query)
	if err != nil {
		return true // Assume empty on error
	}

	result, err := exec.Execute(q)
	if err != nil {
		return true // Assume empty on error
	}

	return result.Size() == 0
}

// runSingleQuery executes a single query and exits
func runSingleQuery(db *storage.Database, handler annotations.Handler, queryStr string, enableDecorrelation bool) {
	// Parse query
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse error: %v\n", err)
		os.Exit(1)
	}

	// Print the formatted query
	fmt.Printf("Query:\n%s\n\n", q.String())

	// Create executor with optimizations
	opts := storage.DefaultPlannerOptions()
	opts.EnableSubqueryDecorrelation = enableDecorrelation
	exec := db.NewExecutorWithOptions(opts)

	// Execute query with timing
	start := time.Now()
	var result executor.Relation
	if handler != nil {
		ctx := executor.NewContext(handler)
		result, err = exec.ExecuteWithContext(ctx, q)
	} else {
		result, err = exec.Execute(q)
	}
	elapsed := time.Since(start)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Execution error: %v\n", err)
		os.Exit(1)
	}

	// Display results as markdown table with timing
	table := result.Table()
	// Replace the row count line with row count + timing
	lines := strings.Split(table, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "_") && strings.HasSuffix(lines[i], "rows_") {
			// Extract row count
			rowLine := lines[i]
			rowLine = strings.TrimSuffix(rowLine, "_")
			lines[i] = rowLine + fmt.Sprintf(" (%.3fms)_", float64(elapsed.Microseconds())/1000.0)
			break
		}
	}
	fmt.Print(strings.Join(lines, "\n"))
}
