package storage

import (
	"fmt"
	"os"
	"testing"
)

// TestMain runs before all tests and ensures test database exists
func TestMain(m *testing.M) {
	// Check if test database exists
	dbPath := "testdata/ohlc_benchmark.db"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		// Database doesn't exist - build it automatically
		fmt.Println("📦 Test database not found, building it now...")
		fmt.Println("   (This is a one-time setup, will be cached)")
		fmt.Println()

		config := DefaultOHLCConfig()
		db, err := BuildTestDatabase(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to build test database: %v\n", err)
			fmt.Fprintf(os.Stderr, "   You can build it manually with:\n")
			fmt.Fprintf(os.Stderr, "   go run cmd/build-testdb/main.go\n")
			os.Exit(1)
		}
		db.Close()

		fmt.Println()
		fmt.Println("✅ Test database built successfully!")
		fmt.Println()
	}

	// Run tests
	code := m.Run()

	// Optional: Clean up test database after tests
	// Uncomment if you want automatic cleanup:
	// os.RemoveAll(dbPath)

	os.Exit(code)
}
