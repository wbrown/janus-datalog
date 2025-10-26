package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestDataConfig specifies what kind of test database to build
type TestDataConfig struct {
	NumSymbols      int       // Number of stock symbols
	NumDays         int       // Number of days of data
	BarsPerDay      int       // Number of bars per day (1=daily, 24=hourly, 390=minute)
	OutputPath      string    // Where to store the database
	AttributePrefix string    // e.g., "price/" or "trade/"
	StartDate       time.Time // Start date for data generation
}

// DefaultOHLCConfig returns a small realistic OHLC dataset for profiling
// Size: ~10 symbols × 30 days × 24 hours = 7,200 bars = 50,400 datoms (~8 MB)
func DefaultOHLCConfig() TestDataConfig {
	return TestDataConfig{
		NumSymbols:      10, // 10 stock symbols
		NumDays:         30, // 30 days of data
		BarsPerDay:      24, // Hourly bars
		OutputPath:      "testdata/ohlc_benchmark.db",
		AttributePrefix: "price/",
		StartDate:       time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
	}
}

// MediumOHLCConfig returns a medium-sized dataset for profiling
// Size: ~50 symbols × 30 days × 24 hours = 36,000 bars = 252,000 datoms (~40 MB)
func MediumOHLCConfig() TestDataConfig {
	return TestDataConfig{
		NumSymbols:      50, // 50 stock symbols
		NumDays:         30, // 30 days of data
		BarsPerDay:      24, // Hourly bars
		OutputPath:      "testdata/ohlc_medium.db",
		AttributePrefix: "price/",
		StartDate:       time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
	}
}

// LargeOHLCConfig returns a large dataset for stress testing
func LargeOHLCConfig() TestDataConfig {
	return TestDataConfig{
		NumSymbols:      500, // 500 stock symbols
		NumDays:         365, // 1 year of data
		BarsPerDay:      390, // Minute bars (6.5 hour trading day)
		OutputPath:      "testdata/ohlc_large.db",
		AttributePrefix: "price/",
		StartDate:       time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC),
	}
}

// BuildTestDatabase creates a pre-populated BadgerDB for benchmarking
func BuildTestDatabase(config TestDataConfig) (*Database, error) {
	// Remove existing database
	if err := os.RemoveAll(config.OutputPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove existing db: %w", err)
	}

	// Create directory if needed
	dir := filepath.Dir(config.OutputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create database
	db, err := NewDatabase(config.OutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Generate test data
	datoms := generateOHLCData(config)

	// Write datoms in batches to avoid slow commits
	// BadgerDB commits are slow for large transactions because it updates 5 indices
	batchSize := 5000
	fmt.Printf("Writing %d datoms to %s in batches of %d...\n", len(datoms), config.OutputPath, batchSize)

	for batchStart := 0; batchStart < len(datoms); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(datoms) {
			batchEnd = len(datoms)
		}

		tx := db.NewTransaction()

		for i := batchStart; i < batchEnd; i++ {
			datom := datoms[i]
			if err := tx.Add(datom.E, datom.A, datom.V); err != nil {
				tx.Rollback()
				db.Close()
				return nil, fmt.Errorf("failed to add datom %d: %w", i, err)
			}
		}

		// Commit batch
		_, err := tx.Commit()
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to commit batch %d-%d: %w", batchStart, batchEnd, err)
		}

		// Progress indicator
		fmt.Printf("  Written %d/%d datoms (%.1f%%)\n", batchEnd, len(datoms),
			float64(batchEnd)/float64(len(datoms))*100)
	}

	fmt.Printf("✅ Database created: %s\n", config.OutputPath)
	fmt.Printf("   Total datoms: %d\n", len(datoms))
	fmt.Printf("   Symbols: %d, Days: %d, Bars/day: %d\n",
		config.NumSymbols, config.NumDays, config.BarsPerDay)

	return db, nil
}

// generateOHLCData creates realistic OHLC bar data
func generateOHLCData(config TestDataConfig) []datalog.Datom {
	totalBars := config.NumSymbols * config.NumDays * config.BarsPerDay
	datoms := make([]datalog.Datom, 0, totalBars*6) // 6 attributes per bar

	barIdx := 0
	baseTime := config.StartDate

	for symbolIdx := 0; symbolIdx < config.NumSymbols; symbolIdx++ {
		symbol := fmt.Sprintf("TICK%04d", symbolIdx)
		symbolEntity := datalog.NewKeyword(fmt.Sprintf("%ssymbol", config.AttributePrefix))
		symbolIdentity := datalog.NewIdentity(symbol)

		for day := 0; day < config.NumDays; day++ {
			for bar := 0; bar < config.BarsPerDay; bar++ {
				barIdx++

				// Create entity for this bar
				barID := fmt.Sprintf("bar%d", barIdx)
				barEntity := datalog.NewIdentity(barID)

				// Calculate time for this bar
				var barTime time.Time
				if config.BarsPerDay == 1 {
					// Daily bars: one per day at midnight
					barTime = baseTime.AddDate(0, 0, day)
				} else if config.BarsPerDay == 24 {
					// Hourly bars: 24 per day
					barTime = baseTime.AddDate(0, 0, day).Add(time.Duration(bar) * time.Hour)
				} else {
					// Minute bars: evenly spaced throughout the day
					minutesPerBar := (24 * 60) / config.BarsPerDay
					barTime = baseTime.AddDate(0, 0, day).Add(time.Duration(bar*minutesPerBar) * time.Minute)
				}

				// Generate OHLC values (simple random walk)
				basePrice := 100.0 + float64(symbolIdx)*10.0
				dayOffset := float64(day) * 0.1
				barOffset := float64(bar) * 0.01
				open := basePrice + dayOffset + barOffset
				high := open + 2.0
				low := open - 1.5
				close := open + 0.5

				// Create datoms for this bar
				datoms = append(datoms,
					// Link bar to symbol
					datalog.Datom{
						E:  barEntity,
						A:  symbolEntity,
						V:  symbolIdentity,
						Tx: 1,
					},
					// Time
					datalog.Datom{
						E:  barEntity,
						A:  datalog.NewKeyword(fmt.Sprintf("%stime", config.AttributePrefix)),
						V:  barTime,
						Tx: 1,
					},
					// Minute of day
					datalog.Datom{
						E:  barEntity,
						A:  datalog.NewKeyword(fmt.Sprintf("%sminute-of-day", config.AttributePrefix)),
						V:  int64(bar * (24 * 60 / config.BarsPerDay)),
						Tx: 1,
					},
					// OHLC values
					datalog.Datom{
						E:  barEntity,
						A:  datalog.NewKeyword(fmt.Sprintf("%sopen", config.AttributePrefix)),
						V:  open,
						Tx: 1,
					},
					datalog.Datom{
						E:  barEntity,
						A:  datalog.NewKeyword(fmt.Sprintf("%shigh", config.AttributePrefix)),
						V:  high,
						Tx: 1,
					},
					datalog.Datom{
						E:  barEntity,
						A:  datalog.NewKeyword(fmt.Sprintf("%slow", config.AttributePrefix)),
						V:  low,
						Tx: 1,
					},
					datalog.Datom{
						E:  barEntity,
						A:  datalog.NewKeyword(fmt.Sprintf("%sclose", config.AttributePrefix)),
						V:  close,
						Tx: 1,
					},
				)
			}
		}
	}

	return datoms
}

// OpenTestDatabase opens a pre-built test database
func OpenTestDatabase(path string) (*Database, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("test database not found: %s (run BuildTestDatabase first)", path)
	}

	db, err := NewDatabase(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open test database: %w", err)
	}

	return db, nil
}

// TestDatabaseStats prints statistics about the test database
func TestDatabaseStats(db *Database) error {
	// Get database path and file size
	info, err := os.Stat(db.store.db.Opts().Dir)
	if err != nil {
		return fmt.Errorf("failed to stat database: %w", err)
	}

	fmt.Printf("Database Statistics:\n")
	fmt.Printf("  Path: %s\n", db.store.db.Opts().Dir)
	fmt.Printf("  Size on disk: %.2f MB\n", float64(info.Size())/1024/1024)

	return nil
}
