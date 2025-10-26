package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TableFormatter provides utilities for formatting Relations as tables
type TableFormatter struct {
	// MaxWidth is the maximum width for a column
	MaxWidth int
	// TruncateString is the string to append when truncating
	TruncateString string
}

// NewTableFormatter creates a new table formatter with default settings
func NewTableFormatter() *TableFormatter {
	return &TableFormatter{
		MaxWidth:       50,
		TruncateString: "...",
	}
}

// FormatRelation formats a Relation as a markdown table
func (tf *TableFormatter) FormatRelation(rel Relation) string {
	if rel == nil || rel.IsEmpty() {
		return "_Empty relation_"
	}

	// Collect all tuples
	var tuples []Tuple
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		tuples = append(tuples, tupleCopy)
	}

	columns := rel.Columns()
	return tf.formatTable(columns, tuples)
}

// formatTable formats columns and tuples as a markdown table
func (tf *TableFormatter) formatTable(columns []query.Symbol, tuples []Tuple) string {
	if len(tuples) == 0 {
		return fmt.Sprintf("_Columns: %v_\n\n_No rows_", columns)
	}

	tableString := &strings.Builder{}

	// Create alignment array with all columns using AlignNone for simple separators
	alignment := make([]tw.Align, len(columns))
	for i := range alignment {
		alignment[i] = tw.AlignNone
	}

	table := tablewriter.NewTable(tableString,
		tablewriter.WithRenderer(renderer.NewMarkdown()),
		tablewriter.WithAlignment(alignment),
		tablewriter.WithHeaderAutoFormat(tw.Off),
	)

	// Set headers
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = string(col)
	}
	table.Header(headers)

	// Append rows
	for _, tuple := range tuples {
		row := make([]string, len(tuple))
		for j, val := range tuple {
			row[j] = tf.formatValue(val)
		}
		table.Append(row)
	}

	// Render the table
	table.Render()

	// Add row count
	tableString.WriteString(fmt.Sprintf("\n_%d rows_\n", len(tuples)))

	return tableString.String()
}

// formatValue converts a value to a string representation
func (tf *TableFormatter) formatValue(val interface{}) string {
	if val == nil {
		return "nil"
	}

	switch v := val.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.2f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	case datalog.Identity:
		// Show the original string for readability
		return v.String()
	case datalog.Keyword:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Quick helper functions for debugging

// PrintRelation prints a relation to stdout
func PrintRelation(rel Relation) {
	formatter := NewTableFormatter()
	fmt.Println(formatter.FormatRelation(rel))
}

// PrintResult prints a result to stdout
func PrintResult(result Relation) {
	PrintRelation(result)
}

// RelationString returns a string representation of a relation
func RelationString(rel Relation) string {
	formatter := NewTableFormatter()
	return formatter.FormatRelation(rel)
}
