package executor

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanFingerprint is the identity of an unbound scan: its source; each
// pattern position, holding a variable canonicalized to the position, a
// blank, a constant with its value, or a vector literal with its elements;
// the order-by keys, a pattern variable canonicalized to its position and a
// symbol the pattern does not bind kept as itself; and the limit. Two
// fingerprints are equal when a scan under one answers the other, so
// [?t :task/root ?s] and [?x :task/root ?y] share a scan, and a scan
// materialized under :limit 5 or sorted one way serves no consumer wanting
// every tuple or another order.
//
// The components are typed, hashed for bucketing and compared one by one on
// a hash match, as TupleKeyMap keys a tuple. A vector literal's elements are
// the pattern's own slice. Only an unbound scan has a fingerprint: a bound
// scan is specialized to its binding values.
type ScanFingerprint struct {
	hash     uint64
	source   datalog.Symbol
	elements [query.PatternPositions]fingerprintElement
	count    int
	order    []fingerprintOrder
	limited  bool
	limit    int
}

// fingerprintElementKind is what a pattern position holds.
type fingerprintElementKind uint8

const (
	fingerprintVariable fingerprintElementKind = iota
	fingerprintBlank
	fingerprintConstant
	fingerprintVector
)

type fingerprintElement struct {
	kind   fingerprintElementKind
	value  interface{}   // fingerprintConstant
	values []interface{} // fingerprintVector
}

// fingerprintOrderKind is what an order-by key names: a pattern position,
// or a symbol the pattern does not bind.
type fingerprintOrderKind uint8

const (
	fingerprintOrderPosition fingerprintOrderKind = iota
	fingerprintOrderExternal
)

type fingerprintOrder struct {
	kind       fingerprintOrderKind
	position   int          // fingerprintOrderPosition
	external   query.Symbol // fingerprintOrderExternal
	descending bool
}

// FingerprintScan computes the fingerprint of the unbound scan of pattern
// under q's order-by and limit. The hash is an FNV-1a fold over the
// components in order, each kind tag and value folded in turn.
func FingerprintScan(q *query.Query, pattern *query.DataPattern) ScanFingerprint {
	const prime = 1099511628211
	fp := ScanFingerprint{source: pattern.Source, count: len(pattern.Elements)}
	if fp.source == nil {
		fp.source = datalog.SymDollar
	}
	hash := uint64(14695981039346656037)
	hash ^= datalog.HashValue(fp.source)
	hash *= prime
	for i, element := range pattern.Elements {
		var e fingerprintElement
		switch element := element.(type) {
		case query.Variable:
			e.kind = fingerprintVariable
		case query.Blank:
			e.kind = fingerprintBlank
		case query.Constant:
			e.kind = fingerprintConstant
			e.value = element.Value
		case query.VectorConstant:
			e.kind = fingerprintVector
			e.values = element.Values
		default:
			// PatternElement has four implementations and all are handled above. A
			// new one must be fingerprinted from its values: the fingerprint is
			// ScanSharingMatcher's registry key, so two patterns that collide here
			// are served each other's tuples.
			panic(fmt.Sprintf("FingerprintScan: unhandled pattern element %T", element))
		}
		fp.elements[i] = e
		hash ^= uint64(e.kind)
		hash *= prime
		switch e.kind {
		case fingerprintConstant:
			hash ^= datalog.HashValue(e.value)
			hash *= prime
		case fingerprintVector:
			hash ^= uint64(len(e.values))
			hash *= prime
			for _, value := range e.values {
				hash ^= datalog.HashValue(value)
				hash *= prime
			}
		}
	}
	if len(q.OrderBy) > 0 {
		fp.order = make([]fingerprintOrder, len(q.OrderBy))
	}
	hash ^= uint64(len(q.OrderBy))
	hash *= prime
	for i, clause := range q.OrderBy {
		o := fingerprintOrder{kind: fingerprintOrderExternal, external: clause.Variable, descending: clause.Descending}
		for position, element := range pattern.Elements {
			if variable, ok := element.(query.Variable); ok && variable.Name == clause.Variable {
				o = fingerprintOrder{kind: fingerprintOrderPosition, position: position, descending: clause.Descending}
				break
			}
		}
		fp.order[i] = o
		hash ^= uint64(o.kind)
		hash *= prime
		switch o.kind {
		case fingerprintOrderPosition:
			hash ^= uint64(o.position)
		case fingerprintOrderExternal:
			hash ^= datalog.HashValue(o.external)
		}
		hash *= prime
		if o.descending {
			hash ^= 1
		}
		hash *= prime
	}
	if q.Limit != nil {
		fp.limited = true
		fp.limit = *q.Limit
		hash ^= 1
		hash *= prime
		hash ^= uint64(fp.limit)
		hash *= prime
	} else {
		hash *= prime
	}
	fp.hash = hash
	return fp
}

// Equal reports whether a scan under f answers other.
func (f ScanFingerprint) Equal(other ScanFingerprint) bool {
	if f.hash != other.hash || f.source != other.source || f.count != other.count ||
		len(f.order) != len(other.order) || f.limited != other.limited || f.limit != other.limit {
		return false
	}
	for i := 0; i < f.count; i++ {
		if !f.elements[i].equal(other.elements[i]) {
			return false
		}
	}
	for i := range f.order {
		if f.order[i] != other.order[i] {
			return false
		}
	}
	return true
}

func (e fingerprintElement) equal(other fingerprintElement) bool {
	if e.kind != other.kind {
		return false
	}
	switch e.kind {
	case fingerprintConstant:
		return datalog.ValuesEqual(e.value, other.value)
	case fingerprintVector:
		if len(e.values) != len(other.values) {
			return false
		}
		for i := range e.values {
			if !datalog.ValuesEqual(e.values[i], other.values[i]) {
				return false
			}
		}
	}
	return true
}

// String renders the fingerprint for a reader of the scan-sharing
// annotations: every component typed and length-delimited. It is never the
// registry's key.
func (f ScanFingerprint) String() string {
	var b strings.Builder
	writeFingerprintText(&b, "source", f.source.String())
	for i := 0; i < f.count; i++ {
		e := f.elements[i]
		switch e.kind {
		case fingerprintVariable:
			writeFingerprintText(&b, "variable", "")
		case fingerprintBlank:
			writeFingerprintText(&b, "blank", "")
		case fingerprintConstant:
			writeFingerprintText(&b,
				fmt.Sprintf("constant-%d", datalog.Type(e.value)),
				hex.EncodeToString(datalog.ValueBytes(e.value)))
		case fingerprintVector:
			writeFingerprintText(&b, "vector", strconv.Itoa(len(e.values)))
			for _, value := range e.values {
				writeFingerprintText(&b,
					fmt.Sprintf("element-%d", datalog.Type(value)),
					hex.EncodeToString(datalog.ValueBytes(value)))
			}
		}
	}
	writeFingerprintText(&b, "order", strconv.Itoa(len(f.order)))
	for _, o := range f.order {
		switch o.kind {
		case fingerprintOrderPosition:
			writeFingerprintText(&b, "order-var", strconv.Itoa(o.position))
		case fingerprintOrderExternal:
			writeFingerprintText(&b, "order-external", o.external.String())
		}
		writeFingerprintText(&b, "order-descending", strconv.FormatBool(o.descending))
	}
	if f.limited {
		writeFingerprintText(&b, "limit", strconv.Itoa(f.limit))
	} else {
		writeFingerprintText(&b, "limit", "none")
	}
	return b.String()
}

func writeFingerprintText(b *strings.Builder, kind, value string) {
	fmt.Fprintf(b, "%d:%s:%d:%s;", len(kind), kind, len(value), value)
}
