package executor

import (
	"encoding/hex"
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
	writeFingerprintText(&b, "source", src.String())

	// Every component is typed and length-delimited. Human formatting is not
	// injective ("1" vs int64(1), sentinels, and embedded delimiters).
	for _, elem := range pattern.Elements {
		switch e := elem.(type) {
		case query.Variable:
			writeFingerprintText(&b, "variable", "")
		case query.Constant:
			valueType := datalog.Type(e.Value)
			data := datalog.ValueBytes(e.Value)
			writeFingerprintText(
				&b,
				fmt.Sprintf("constant-%d", valueType),
				hex.EncodeToString(data),
			)
		case query.Blank:
			writeFingerprintText(&b, "blank", "")
		default:
			writeFingerprintText(&b, fmt.Sprintf("%T", e), fmt.Sprint(e))
		}
	}

	return b.String()
}

func writeFingerprintText(b *strings.Builder, kind, value string) {
	fmt.Fprintf(b, "%d:%s:%d:%s;", len(kind), kind, len(value), value)
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
		fmt.Fprintf(&b, ":%t;", clause.Descending)
	}
	b.WriteString("|limit:")
	if q.Limit == nil {
		b.WriteString("none")
	} else {
		fmt.Fprintf(&b, "%d", *q.Limit)
	}
	return b.String()
}
