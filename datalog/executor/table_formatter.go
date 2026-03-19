package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/mattn/go-runewidth"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TableFormatter provides utilities for formatting Relations as tables
type TableFormatter struct {
	// MaxWidth is the maximum width for a symbol
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
	collectTuplesInto(&tuples, rel)

	symbols := rel.Symbols()
	return tf.formatTable(symbols, tuples)
}

// formatTable formats symbols and tuples as a markdown table
func (tf *TableFormatter) formatTable(symbols []query.Symbol, tuples []Tuple) string {
	if len(tuples) == 0 {
		return fmt.Sprintf("_Symbols: %v_\n\n_No tuples_", symbols)
	}

	tableString := &strings.Builder{}

	// Create alignment array with all symbols using AlignNone for simple separators
	alignment := make([]tw.Align, len(symbols))
	for i := range alignment {
		alignment[i] = tw.AlignNone
	}

	table := tablewriter.NewTable(tableString,
		tablewriter.WithRenderer(renderer.NewMarkdown()),
		tablewriter.WithAlignment(alignment),
		tablewriter.WithHeaderAutoFormat(tw.Off),
	)

	// Set headers
	headers := make([]string, len(symbols))
	for i, sym := range symbols {
		headers[i] = sym.String()
	}
	table.Header(headers)

	// Append tuples
	for _, tuple := range tuples {
		cells := make([]string, len(tuple))
		for j, val := range tuple {
			cells[j] = tf.formatValue(val)
		}
		table.Append(cells)
	}

	// Render the table
	table.Render()

	// Add tuple count
	tableString.WriteString(fmt.Sprintf("\n_%d tuples_\n", len(tuples)))

	return tableString.String()
}

// formatValue converts a value to a string representation
func (tf *TableFormatter) formatValue(val interface{}) string {
	var s string
	if val == nil {
		s = "nil"
	} else {
		switch v := val.(type) {
		case string:
			s = v
		case int:
			s = fmt.Sprintf("%d", v)
		case int64:
			s = fmt.Sprintf("%d", v)
		case float64:
			s = fmt.Sprintf("%.2f", v)
		case bool:
			s = fmt.Sprintf("%t", v)
		case time.Time:
			s = v.Format("2006-01-02 15:04:05")
		case datalog.Identity:
			// Show the original string for readability
			s = v.String()
		case datalog.Keyword:
			s = v.String()
		case *uint64:
			s = fmt.Sprintf("%d", *v)
		case *int64:
			s = fmt.Sprintf("%d", *v)
		default:
			s = fmt.Sprintf("%v", v)
		}
	}

	return tf.truncate(s)
}

// truncate shortens a string to MaxWidth display symbols, appending TruncateString if truncated
func (tf *TableFormatter) truncate(s string) string {
	if tf.MaxWidth <= 0 || runewidth.StringWidth(s) <= tf.MaxWidth {
		return s
	}
	return runewidth.Truncate(s, tf.MaxWidth, tf.TruncateString)
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
