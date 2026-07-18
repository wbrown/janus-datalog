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

func TestValuesEqualRejectsStringForIdentity(t *testing.T) {
	id := datalog.NewIdentity("user:alice")
	other := datalog.NewIdentity("user:bob")

	if !valuesEqual(id, id) {
		t.Error("identity must equal itself")
	}
	if valuesEqual(id, other) {
		t.Error("distinct identities must not be equal")
	}
	if valuesEqual(id, "user:alice") {
		t.Error("identity equaled its seed string; comparison-time string coercion must not exist")
	}
	if valuesEqual(id, id.L85()) {
		t.Error("identity equaled its L85 text; comparison-time string coercion must not exist")
	}
}
