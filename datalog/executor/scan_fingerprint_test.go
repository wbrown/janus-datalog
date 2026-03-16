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
