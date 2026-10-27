package qb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// An attribute keyword wider than storage holds is refused at Build with
// the width rule's one message, as a pattern's constant attribute and as a
// get-else's attribute.
func TestBuild_RefusesAnAttributeWiderThanStorageHolds(t *testing.T) {
	e, v, w := NewVar("e"), NewVar("v"), NewVar("w")
	text := ":t/" + strings.Repeat("a", datalog.MaxAttributeBytes-len(":t/"))
	fits, wide := Kw(text), Kw(text+"z")
	names := "exceeds the 32-byte storage limit"

	cases := []struct {
		name  string
		shape interface{}
	}{
		{"a pattern's constant attribute", Pat(e, wide, w)},
		{"a get-else's attribute", GetElse(e, wide, "none").As(w)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Query().Find(v).Where(Pat(e, fits, v), c.shape).Build()
			require.ErrorContains(t, err, names)
		})
	}

	t.Run("an attribute at the limit is accepted", func(t *testing.T) {
		_, err := Query().Find(v).Where(Pat(e, fits, v)).Build()
		require.NoError(t, err)
	})
}
