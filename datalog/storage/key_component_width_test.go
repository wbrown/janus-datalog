package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestComponentKeySizeEnumeratesTheTaxonomy pins that every key component has a
// decided width and an undeclared one fails loudly.
//
// The distinction that matters is between componentV — which genuinely has no
// fixed width, and whose "not fixed" answer is what makes a V-bound range a
// prefix range needing a membership test — and a component nobody has
// classified. A shared "not fixed" answer for both would let a fifth component
// added later read as variable-width, and widthBehind would silently mis-measure
// every member key behind it.
func TestComponentKeySizeEnumeratesTheTaxonomy(t *testing.T) {
	for _, tc := range []struct {
		component keyComponent
		width     int
	}{
		{componentE, entitySize},
		{componentA, attrSize},
		{componentTx, txSize},
	} {
		t.Run(tc.component.String(), func(t *testing.T) {
			width, fixed := componentKeySize(tc.component)
			require.True(t, fixed, "%v is a fixed-width component", tc.component)
			require.Equal(t, tc.width, width)
		})
	}

	t.Run("V", func(t *testing.T) {
		_, fixed := componentKeySize(componentV)
		require.False(t, fixed,
			"a value is as long as it is; V must report no fixed width")
	})

	t.Run("unclassified", func(t *testing.T) {
		require.Panics(t, func() { componentKeySize(componentTx + 1) },
			"a component with no declared width must fail loudly, not share V's answer")
	})
}

// TestRunMembershipRejectsAnAbsentKey pins that the membership test answers the
// same way for a key that is not there, whichever kind of run is asking. The
// two arms disagreed once: the exact arm returned true before the emptiness
// check, so "no key" was a member of every exact run.
func TestRunMembershipRejectsAnAbsentKey(t *testing.T) {
	require.False(t, runMembership{exact: true}.holds(nil),
		"an absent key is not a member of an exact run")
	require.False(t, runMembership{size: 32}.holds(nil),
		"an absent key is not a member of a length-bounded run")
}
