package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Identity values never match string constants. Strings become entities only by
// boundary construction (NewIdentity, #identity literals), never by
// comparison-time coercion: neither the seed string that produced an identity
// nor its L85 text is equal to the identity itself. Cross-type comparison
// within the value domain is an ordinary typed non-match.

func TestMatchesConstantRejectsStringForIdentity(t *testing.T) {
	id := datalog.NewIdentity("user:alice")
	other := datalog.NewIdentity("user:bob")

	if !matchesConstant(id, id) {
		t.Error("identity must match itself")
	}
	if matchesConstant(id, other) {
		t.Error("distinct identities must not match")
	}
	if matchesConstant(id, "user:alice") {
		t.Error("identity matched its seed string; comparison-time string coercion must not exist")
	}
	if matchesConstant(id, id.L85()) {
		t.Error("identity matched its L85 text; comparison-time string coercion must not exist")
	}
}

// TestMatchesConstantRejectsStringForKeyword pins the same rule for
// keywords: a keyword matches only a keyword (interned pointer equality).
// Strings become keywords by boundary construction (NewKeyword, :literal in
// query text), never by comparison-time coercion.
func TestMatchesConstantRejectsStringForKeyword(t *testing.T) {
	kw := datalog.NewKeyword(":status/active")
	other := datalog.NewKeyword(":status/done")

	if !matchesConstant(kw, kw) {
		t.Error("keyword must match itself")
	}
	if matchesConstant(kw, other) {
		t.Error("distinct keywords must not match")
	}
	if matchesConstant(kw, ":status/active") {
		t.Error("keyword matched its text; comparison-time string coercion must not exist")
	}
}

func TestValuesEqualRejectsStringForIdentity(t *testing.T) {
	id := datalog.NewIdentity("user:alice")
	other := datalog.NewIdentity("user:bob")

	if !datalog.ValuesEqual(id, id) {
		t.Error("identity must equal itself")
	}
	if datalog.ValuesEqual(id, other) {
		t.Error("distinct identities must not be equal")
	}
	if datalog.ValuesEqual(id, "user:alice") {
		t.Error("identity equaled its seed string; comparison-time string coercion must not exist")
	}
	if datalog.ValuesEqual(id, id.L85()) {
		t.Error("identity equaled its L85 text; comparison-time string coercion must not exist")
	}
}
