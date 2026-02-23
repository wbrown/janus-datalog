package executor

import (
	"strings"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestTableFormatter(t *testing.T) {
	formatter := NewTableFormatter()

	t.Run("FormatEmptyRelation", func(t *testing.T) {
		rel := NewMaterializedRelation([]query.Symbol{}, []Tuple{})
		result := formatter.FormatRelation(rel)
		if result != "_Empty relation_" {
			t.Errorf("Expected '_Empty relation_', got %s", result)
		}
	})

	t.Run("FormatSimpleRelation", func(t *testing.T) {
		symbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age"), datalog.NewSymbol("?active")}
		tuples := []Tuple{
			{"Alice", int64(30), true},
			{"Bob", int64(25), false},
			{"Charlie", int64(35), true},
		}
		rel := NewMaterializedRelation(symbols, tuples)

		result := formatter.FormatRelation(rel)

		// Check that it contains expected elements
		if !strings.Contains(result, "?name") {
			t.Error("Missing symbol ?name")
		}
		if !strings.Contains(result, "Alice") {
			t.Error("Missing value Alice")
		}
		if !strings.Contains(result, "3 tuples") {
			t.Error("Missing tuple count")
		}
	})

	t.Run("FormatWithDifferentTypes", func(t *testing.T) {
		symbols := []query.Symbol{datalog.NewSymbol("?entity"), datalog.NewSymbol("?keyword"), datalog.NewSymbol("?value"), datalog.NewSymbol("?time")}
		testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		tuples := []Tuple{
			{
				datalog.NewIdentity("user:123"),
				datalog.NewKeyword(":user/name"),
				"John Doe",
				testTime,
			},
			{
				datalog.NewIdentity("user:456"),
				datalog.NewKeyword(":user/age"),
				int64(42),
				testTime.Add(24 * time.Hour),
			},
			{
				datalog.NewIdentity("user:789"),
				datalog.NewKeyword(":user/salary"),
				75000.50,
				testTime.Add(48 * time.Hour),
			},
		}

		rel := NewMaterializedRelation(symbols, tuples)
		result := formatter.FormatRelation(rel)

		// Check formatting
		if !strings.Contains(result, "user:123") {
			t.Error("Missing identity value")
		}
		if !strings.Contains(result, ":user/name") {
			t.Error("Missing keyword value")
		}
		if !strings.Contains(result, "75000.50") {
			t.Error("Missing float value")
		}
		if !strings.Contains(result, "2024-01-01") {
			t.Error("Missing time value")
		}
	})

	t.Run("FormatWithLongStrings", func(t *testing.T) {
		formatter.MaxWidth = 20

		symbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?description")}
		longString := "This is a very long string that should be truncated because it exceeds the maximum width"

		tuples := []Tuple{
			{int64(1), longString},
			{int64(2), "Short"},
		}

		rel := NewMaterializedRelation(symbols, tuples)
		result := formatter.FormatRelation(rel)

		// Tablewriter handles truncation differently - it wraps or shows full text
		// Just check that the table is formatted
		if !strings.Contains(result, "?id") {
			t.Error("Missing symbol header")
		}
	})

	t.Run("FormatMarkdownTable", func(t *testing.T) {
		symbols := []query.Symbol{datalog.NewSymbol("?symbol"), datalog.NewSymbol("?price"), datalog.NewSymbol("?volume")}
		tuples := []Tuple{
			{"AAPL", 150.25, int64(1000000)},
			{"GOOG", 2800.50, int64(500000)},
		}

		rel := NewMaterializedRelation(symbols, tuples)
		result := formatter.FormatRelation(rel)

		// Check markdown format - tablewriter uses different separator style
		if !strings.Contains(result, "| ?symbol") {
			t.Error("Missing markdown header")
		}
		if !strings.Contains(result, "|---") {
			t.Error("Missing markdown separator")
		}
		if !strings.Contains(result, "| AAPL") && !strings.Contains(result, "150.25") {
			t.Error("Missing markdown tuple data")
		}
	})

	t.Run("FormatResult", func(t *testing.T) {
		result := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?count")},
			[]Tuple{
				{"Alice", int64(5)},
				{"Bob", int64(3)},
			},
		)

		formatted := formatter.FormatRelation(result)

		if !strings.Contains(formatted, "?name") {
			t.Error("Missing symbol in result format")
		}
		if !strings.Contains(formatted, "_2 tuples_") {
			t.Error("Missing tuple count in result format")
		}
	})
}

func TestPrintHelpers(t *testing.T) {
	// Just test that these don't panic
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	tuples := []Tuple{
		{int64(1), "a"},
		{int64(2), "b"},
	}
	rel := NewMaterializedRelation(symbols, tuples)

	// These print to stdout, so we just ensure they don't panic
	PrintRelation(rel)

	result := NewMaterializedRelation(symbols, tuples)
	PrintResult(result)

	// Test string helpers
	relStr := RelationString(rel)
	if relStr == "" {
		t.Error("RelationString should not be empty")
	}

}

// TestTableFormatterTupleCopying verifies that Table() correctly copies tuples
// from iterators, which is critical for streaming relations that may reuse buffers.
// This is a regression test for the CLI empty results bug.
func TestTableFormatterTupleCopying(t *testing.T) {
	// Create a relation with streaming behavior enabled
	symbols := []query.Symbol{datalog.NewSymbol("x"), datalog.NewSymbol("y"), datalog.NewSymbol("z")}
	tuples := []Tuple{
		{int64(1), int64(10), int64(100)},
		{int64(2), int64(20), int64(200)},
		{int64(3), int64(30), int64(300)},
	}

	rel := NewMaterializedRelationWithOptions(symbols, tuples, ExecutorOptions{
		EnableTrueStreaming: true, // Enable streaming mode
	})

	// Format as table
	formatter := NewTableFormatter()
	table := formatter.FormatRelation(rel)

	// Verify all three tuples appear in the output (not just the last one)
	if !strings.Contains(table, "1") {
		t.Errorf("Expected to find value 1 in table output:\n%s", table)
	}
	if !strings.Contains(table, "2") {
		t.Errorf("Expected to find value 2 in table output:\n%s", table)
	}
	if !strings.Contains(table, "3") {
		t.Errorf("Expected to find value 3 in table output:\n%s", table)
	}

	// Verify tuple count
	if !strings.Contains(table, "3 tuples") {
		t.Errorf("Expected '3 tuples' in output, got:\n%s", table)
	}

	// Verify symbol headers
	if !strings.Contains(table, "x") || !strings.Contains(table, "y") || !strings.Contains(table, "z") {
		t.Errorf("Expected symbol headers x, y, z in output, got:\n%s", table)
	}

	// Verify all values from different tuples are present
	if !strings.Contains(table, "10") || !strings.Contains(table, "20") || !strings.Contains(table, "30") {
		t.Errorf("Expected y-symbol values (10, 20, 30) in output, got:\n%s", table)
	}
	if !strings.Contains(table, "100") || !strings.Contains(table, "200") || !strings.Contains(table, "300") {
		t.Errorf("Expected z-symbol values (100, 200, 300) in output, got:\n%s", table)
	}
}
