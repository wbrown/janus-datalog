package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	configType := flag.String("config", "default", "Config type: default, medium, or large")
	flag.Parse()

	var config storage.TestDataConfig
	switch *configType {
	case "default":
		config = storage.DefaultOHLCConfig()
	case "medium":
		config = storage.MediumOHLCConfig()
	case "large":
		config = storage.LargeOHLCConfig()
	default:
		fmt.Fprintf(os.Stderr, "Unknown config type: %s (use 'default', 'medium', or 'large')\n", *configType)
		os.Exit(1)
	}

	fmt.Printf("Building test database: %s\n", config.OutputPath)
	fmt.Printf("  Symbols: %d\n", config.NumSymbols)
	fmt.Printf("  Days: %d\n", config.NumDays)
	fmt.Printf("  Bars/day: %d\n", config.BarsPerDay)
	fmt.Printf("  Total bars: %d\n", config.NumSymbols*config.NumDays*config.BarsPerDay)
	fmt.Println()

	db, err := storage.BuildTestDatabase(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := storage.TestDatabaseStats(db); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get stats: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ Done! Use this database with:")
	fmt.Println("   go test -bench=BenchmarkPrebuiltDatabase ./datalog/storage")
}
