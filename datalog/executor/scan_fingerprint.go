package executor

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanFingerprint computes a deterministic key for a DataPattern based on
// which positions are bound vs. variable and the constant values in bound
// positions. Two patterns with the same fingerprint will produce identical
// scan results from storage — enabling scan sharing.
//
// Variable names are excluded: [?t :task/root ?s] and [?x :task/root ?y]
// produce the same fingerprint because they scan the same datoms.
//
// Only applicable to unbound scans (no bindings from prior joins).
func ScanFingerprint(pattern *query.DataPattern) string {
	var b strings.Builder

	// Source
	src := pattern.Source
	if src == nil {
		src = datalog.SymDollar
	}
	b.WriteString(src.String())
	b.WriteByte('|')

	// Each element: VAR for variables, value string for constants
	for i, elem := range pattern.Elements {
		if i > 0 {
			b.WriteByte('|')
		}
		switch e := elem.(type) {
		case query.Variable:
			b.WriteString("VAR")
		case query.Constant:
			fmt.Fprintf(&b, "%v", e.Value)
		case query.Blank:
			b.WriteString("_")
		default:
			fmt.Fprintf(&b, "%v", e)
		}
	}

	return b.String()
}

// ScanQueryFingerprint extends the logical pattern fingerprint with physical
// ordering and limit requirements. Variable names remain canonicalized by
// pattern position so equivalent renamed scans can still share.
func ScanQueryFingerprint(q *query.Query, pattern *query.DataPattern) string {
	var b strings.Builder
	b.WriteString(ScanFingerprint(pattern))
	positions := make(map[query.Symbol]int)
	for i, element := range pattern.Elements {
		if variable, ok := element.(query.Variable); ok {
			positions[variable.Name] = i
		}
	}
	b.WriteString("|order:")
	for _, clause := range q.OrderBy {
		position, ok := positions[clause.Variable]
		if !ok {
			fmt.Fprintf(&b, "external:%s", clause.Variable)
		} else {
			fmt.Fprintf(&b, "var%d", position)
		}
		fmt.Fprintf(&b, ":%s;", clause.Direction)
	}
	b.WriteString("|limit:")
	if q.Limit == nil {
		b.WriteString("none")
	} else {
		fmt.Fprintf(&b, "%d", *q.Limit)
	}
	return b.String()
}
