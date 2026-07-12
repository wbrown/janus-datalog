package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestScanFingerprint_SamePatternDifferentVars verifies that two patterns
// with the same constants but different variable names produce identical
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

	assert.Equal(t, ScanFingerprint(p1), ScanFingerprint(p2),
		"[?t :task/root ?s] and [?x :task/root ?y] should have same fingerprint")
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

	assert.NotEqual(t, ScanFingerprint(p1), ScanFingerprint(p2),
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

	assert.NotEqual(t, ScanFingerprint(pBound), ScanFingerprint(pUnbound),
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

	assert.NotEqual(t, ScanFingerprint(pBound), ScanFingerprint(pUnbound),
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

	assert.NotEqual(t, ScanFingerprint(p1), ScanFingerprint(p2),
		"$src1 and $src2 should have different fingerprints")
}

// TestScanFingerprint_Deterministic verifies that the same pattern produces
// the same fingerprint on repeated calls.
func TestScanFingerprint_Deterministic(t *testing.T) {
	p := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":task/root")},
			query.Variable{Name: datalog.NewSymbol("?s")},
		},
	}

	fp1 := ScanFingerprint(p)
	fp2 := ScanFingerprint(p)
	assert.Equal(t, fp1, fp2, "same pattern should always produce same fingerprint")
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
		OrderBy: []query.OrderByClause{{Variable: entity, Direction: query.OrderAsc}},
	}
	limitedOne := &query.Query{Where: []query.Clause{pattern}, Limit: &limitOne}
	limitedTwo := &query.Query{Where: []query.Clause{pattern}, Limit: &limitTwo}

	assert.NotEqual(t, ScanQueryFingerprint(base, pattern), ScanQueryFingerprint(ordered, pattern))
	assert.NotEqual(t, ScanQueryFingerprint(limitedOne, pattern), ScanQueryFingerprint(limitedTwo, pattern))
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
		OrderBy: []query.OrderByClause{{Variable: leftEntity, Direction: query.OrderDesc}},
	}
	rightQuery := &query.Query{
		Where:   []query.Clause{right},
		OrderBy: []query.OrderByClause{{Variable: rightEntity, Direction: query.OrderDesc}},
	}

	assert.Equal(t,
		ScanQueryFingerprint(leftQuery, left),
		ScanQueryFingerprint(rightQuery, right),
	)
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
			assert.NotEqual(t, ScanFingerprint(testCase.left), ScanFingerprint(testCase.right))
		})
	}
}
