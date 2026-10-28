package qb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// RuleSet refuses a built rule whose body holds a constant outside the value
// domain, a ground or a pattern value, naming the rule and the value's type,
// or the value itself for an unsigned integer above int64's range, as Build
// refuses the same constant in a query's :where; it never panics. An
// unsigned integer within the range is the integer it equals and is accepted.
func TestRuleSet_RefusesAConstantOutsideTheDomain(t *testing.T) {
	e, x := NewVar("e"), NewVar("x")
	attr := Kw(":t/a")
	cases := []struct {
		name  string
		rule  *RuleBuilder
		names string
	}{
		{"a float32 ground", DefineRule("r", x).Where(Pat(e, attr, Blank()), Ground(float32(2)).As(x)), "float32"},
		{"an unsigned integer above int64's range as a ground", DefineRule("r", x).Where(Pat(e, attr, Blank()), Ground(uint64(math.MaxInt64)+1).As(x)), "9223372036854775808"},
		{"a float32 pattern value", DefineRule("r", e).Where(Pat(e, attr, float32(2))), "float32"},
		{"a vector ground holding a float32", DefineRule("r", x).Where(Pat(e, attr, Blank()), Ground([]interface{}{int64(1), float32(2)}).As(x)), "float32"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error
			require.NotPanics(t, func() { _, err = RuleSet(c.rule) })
			require.ErrorContains(t, err, "rule r", "the refusal names the rule")
			require.ErrorContains(t, err, c.names, "the refusal names the constant")
		})
	}

	t.Run("an unsigned integer within the range is accepted", func(t *testing.T) {
		_, err := RuleSet(DefineRule("r", x).Where(Pat(e, attr, Blank()), Ground(uint64(5)).As(x)))
		require.NoError(t, err)
	})
}
