package executor

import (
	"encoding/hex"
	"fmt"
	"strconv"
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
		case query.VectorConstant:
			// Elements carry their own type tag and bytes, as Constant does. The
			// rendered form is not injective: FormatValueEDN prints int64(1) and
			// float64(1) both as "1", and ValuesEqual holds those distinct, so [1]
			// and [1.0] shared one key. The count is part of the fingerprint
			// because a vector contributes a variable number of entries, so
			// without it the boundary to the element after it is not recoverable.
			writeFingerprintText(&b, "vector", strconv.Itoa(len(e.Values)))
			for _, value := range e.Values {
				writeFingerprintText(
					&b,
					fmt.Sprintf("element-%d", datalog.Type(value)),
					hex.EncodeToString(datalog.ValueBytes(value)),
				)
			}
		default:
			// PatternElement has four implementations and all are handled above. A
			// new one must be fingerprinted from its values rather than rendered:
			// this string is ScanSharingMatcher's cache key, so two patterns that
			// collide here are served each other's tuples.
			panic(fmt.Sprintf("ScanFingerprint: unhandled pattern element %T", elem))
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
	// Length-delimited like the pattern components, not raw text. An external
	// sort key writes a symbol's name, and datalog.NewSymbol accepts any string —
	// so relying on a lexed symbol being unable to contain the separator would
	// make injectivity a property of the parser rather than of this encoding.
	writeFingerprintText(&b, "order", strconv.Itoa(len(q.OrderBy)))
	for _, clause := range q.OrderBy {
		if position, ok := positions[clause.Variable]; ok {
			writeFingerprintText(&b, "order-var", strconv.Itoa(position))
		} else {
			writeFingerprintText(&b, "order-external", clause.Variable.String())
		}
		writeFingerprintText(&b, "order-descending", strconv.FormatBool(clause.Descending))
	}
	if q.Limit == nil {
		writeFingerprintText(&b, "limit", "none")
	} else {
		writeFingerprintText(&b, "limit", strconv.Itoa(*q.Limit))
	}
	return b.String()
}
