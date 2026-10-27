package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// fingerprintOf is the fingerprint of pattern as a query of that one pattern
// alone: no order-by, no limit.
func fingerprintOf(pattern *query.DataPattern) ScanFingerprint {
	return FingerprintScan(query.PatternQuery(pattern), pattern)
}

// TestScanFingerprint_SamePatternDifferentVars verifies that two patterns
// with the same constants but different variable names produce equal
// fingerprints. This is the core invariant for scan sharing.
func TestScanFingerprint_SamePatternDifferentVars(t *testing.T) {
	p1 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}
	p2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?x")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?y")},
		},
	}

	assert.True(t, fingerprintOf(p1).Equal(fingerprintOf(p2)),
		"[?t :task/root ?s] and [?x :task/root ?y] should have equal fingerprints")
}

// TestScanFingerprint_VectorElementTypes pins that a vector literal is
// fingerprinted from its elements' types and values, not from its rendered
// form. FormatValueEDN prints int64(1) and float64(1) both as "1" — FormatFloat
// drops a zero fraction — while ValuesEqual holds the two distinct, so a
// rendered form gives [1] and [1.0] one fingerprint. The fingerprint is
// ScanSharingMatcher's registry key, so that collision serves one pattern the
// other pattern's tuples.
func TestScanFingerprint_VectorElementTypes(t *testing.T) {
	vectorPattern := func(values ...interface{}) *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: datalog.NewKeyword(":m/samples")},
				query.VectorConstant{Values: values},
			},
		}
	}
	scalarPattern := func(value interface{}) *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: datalog.NewKeyword(":m/samples")},
				query.Constant{Value: value},
			},
		}
	}

	assert.False(t,
		fingerprintOf(vectorPattern(int64(1))).Equal(fingerprintOf(vectorPattern(float64(1)))),
		"[1] and [1.0] are distinct values and must not share a scan")
	assert.False(t,
		fingerprintOf(vectorPattern(int64(1), int64(2))).Equal(fingerprintOf(vectorPattern(int64(1)))),
		"vectors of different length must not share a scan")
	assert.False(t,
		fingerprintOf(vectorPattern(int64(12))).Equal(fingerprintOf(vectorPattern(int64(1), int64(2)))),
		"[12] and [1 2] must not share a scan")
	assert.False(t,
		fingerprintOf(vectorPattern(int64(1))).Equal(fingerprintOf(scalarPattern(int64(1)))),
		"a one-element vector and the bare element must not share a scan")
	assert.True(t,
		fingerprintOf(vectorPattern(int64(1), "a")).Equal(fingerprintOf(vectorPattern(int64(1), "a"))),
		"identical vector literals must share a scan")
}

// TestScanFingerprint_DifferentAttribute verifies that patterns with
// different attribute constants produce different fingerprints.
func TestScanFingerprint_DifferentAttribute(t *testing.T) {
	p1 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}
	p2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/status")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	assert.False(t, fingerprintOf(p1).Equal(fingerprintOf(p2)),
		":task/root and :task/status should have different fingerprints")
}

// TestScanFingerprint_BoundValue verifies that a bound V produces a different
// fingerprint than an unbound V for the same attribute.
func TestScanFingerprint_BoundValue(t *testing.T) {
	pBound := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/status")},
			query.Constant{Value: datalog.NewKeyword(":status/complete")},
		},
	}
	pUnbound := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?t")},
			query.Constant{Value: datalog.NewKeyword(":task/status")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	assert.False(t, fingerprintOf(pBound).Equal(fingerprintOf(pUnbound)),
		"bound V (:status/complete) and unbound V (?s) should differ")
}

// TestScanFingerprint_ConstantEntity verifies that a bound E produces a
// different fingerprint than an unbound E.
func TestScanFingerprint_ConstantEntity(t *testing.T) {
	pBound := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: datalog.NewIdentity("entity:1")},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	pUnbound := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	assert.False(t, fingerprintOf(pBound).Equal(fingerprintOf(pUnbound)),
		"bound E (entity:1) and unbound E (?e) should differ")
}

// TestScanFingerprint_DifferentSources verifies that patterns on different
// sources produce different fingerprints.
func TestScanFingerprint_DifferentSources(t *testing.T) {
	p1 := &query.DataPattern{
		Source: datalog.NewSymbol("$src1"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	p2 := &query.DataPattern{
		Source: datalog.NewSymbol("$src2"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	assert.False(t, fingerprintOf(p1).Equal(fingerprintOf(p2)),
		"$src1 and $src2 should have different fingerprints")
}

// TestScanFingerprint_Deterministic verifies that the same pattern produces
// an equal fingerprint on repeated calls.
func TestScanFingerprint_Deterministic(t *testing.T) {
	p := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	assert.True(t, fingerprintOf(p).Equal(fingerprintOf(p)), "same pattern should always produce an equal fingerprint")
}

func TestScanQueryFingerprintIncludesPhysicalRequirements(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: value},
	}}
	limitOne, limitTwo := 1, 2
	base := &query.Query{Where: []query.Clause{pattern}}
	ordered := &query.Query{
		Where:   []query.Clause{pattern},
		OrderBy: []query.OrderByClause{{Variable: entity, Descending: false}},
	}
	limitedOne := &query.Query{Where: []query.Clause{pattern}, Limit: &limitOne}
	limitedTwo := &query.Query{Where: []query.Clause{pattern}, Limit: &limitTwo}

	assert.False(t, FingerprintScan(base, pattern).Equal(FingerprintScan(ordered, pattern)))
	assert.False(t, FingerprintScan(limitedOne, pattern).Equal(FingerprintScan(limitedTwo, pattern)))
}

func TestScanQueryFingerprintCanonicalizesRenamedOrderVariables(t *testing.T) {
	leftEntity := datalog.NewSymbol("?left-entity")
	rightEntity := datalog.NewSymbol("?right-entity")
	left := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: leftEntity},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: datalog.NewSymbol("?left-value")},
	}}
	right := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: rightEntity},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: datalog.NewSymbol("?right-value")},
	}}
	leftQuery := &query.Query{
		Where:   []query.Clause{left},
		OrderBy: []query.OrderByClause{{Variable: leftEntity, Descending: true}},
	}
	rightQuery := &query.Query{
		Where:   []query.Clause{right},
		OrderBy: []query.OrderByClause{{Variable: rightEntity, Descending: true}},
	}

	assert.True(t, FingerprintScan(leftQuery, left).Equal(FingerprintScan(rightQuery, right)))
}

func TestScanFingerprintTypedConstantsAndSentinelsDoNotCollide(t *testing.T) {
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	testCases := []struct {
		name  string
		left  *query.DataPattern
		right *query.DataPattern
	}{
		{
			name: "variable versus string sentinel",
			left: pattern(
				query.Variable{Name: datalog.NewSymbol("?entity")},
				query.Constant{Value: datalog.NewKeyword(":item/value")},
				query.Constant{Value: "VAR"},
			),
			right: pattern(
				query.Constant{Value: "VAR"},
				query.Constant{Value: datalog.NewKeyword(":item/value")},
				query.Variable{Name: datalog.NewSymbol("?value")},
			),
		},
		{
			name: "blank versus string sentinel",
			left: pattern(
				query.Blank{},
				query.Constant{Value: datalog.NewKeyword(":item/value")},
				query.Constant{Value: "_"},
			),
			right: pattern(
				query.Constant{Value: "_"},
				query.Constant{Value: datalog.NewKeyword(":item/value")},
				query.Blank{},
			),
		},
		{
			name: "integer versus string",
			left: pattern(
				query.Variable{Name: datalog.NewSymbol("?entity")},
				query.Constant{Value: datalog.NewKeyword(":item/value")},
				query.Constant{Value: int64(1)},
			),
			right: pattern(
				query.Variable{Name: datalog.NewSymbol("?entity")},
				query.Constant{Value: datalog.NewKeyword(":item/value")},
				query.Constant{Value: "1"},
			),
		},
		{
			name: "delimiter placement",
			left: pattern(
				query.Constant{Value: "a|b"},
				query.Constant{Value: "c"},
			),
			right: pattern(
				query.Constant{Value: "a"},
				query.Constant{Value: "b|c"},
			),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.False(t, fingerprintOf(testCase.left).Equal(fingerprintOf(testCase.right)))
		})
	}
}

// fingerprintSink keeps a fingerprint built for a measurement from being
// discarded before it is built.
var fingerprintSink ScanFingerprint

// A fingerprint keys the registry on every unbound match, whether or not a
// handler reads its rendering, so the fingerprint of the scan every query
// pays, a constant attribute between two variables with no order-by and no
// limit, is built without allocating.
func TestFingerprintScanAllocatesNothingForAnUnboundPattern(t *testing.T) {
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
	q := &query.Query{Where: []query.Clause{pattern}}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		fingerprintSink = FingerprintScan(q, pattern)
	}))
}

// BenchmarkFingerprintScan measures what one fingerprint costs for the
// unbound scan every query pays: a constant attribute between two variables,
// no order-by and no limit.
func BenchmarkFingerprintScan(b *testing.B) {
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(":item/value")},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
	q := &query.Query{Where: []query.Clause{pattern}}
	b.ReportAllocs()
	for b.Loop() {
		fingerprintSink = FingerprintScan(q, pattern)
	}
}
