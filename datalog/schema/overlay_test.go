package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Overlay composes two schemas into one: every definition of top, and each
// definition of base whose ident top does not define.
func TestOverlay(t *testing.T) {
	base := NewSchema().
		Add(&AttributeDefinition{Ident: kw(":a/x"), ValueType: TypeString, Doc: "base"}).
		Add(&AttributeDefinition{Ident: kw(":a/y"), ValueType: TypeLong, Cardinality: CardinalityMany})
	top := NewSchema().
		Add(&AttributeDefinition{Ident: kw(":a/x"), ValueType: TypeLong, Doc: "top"}).
		Add(&AttributeDefinition{Ident: kw(":a/z"), ValueType: TypeRef})

	t.Run("top wins per ident and base supplies the rest", func(t *testing.T) {
		got := Overlay(base, top)
		require.Equal(t, 3, got.Count())

		x := got.GetAttribute(kw(":a/x"))
		require.NotNil(t, x)
		require.Equal(t, TypeLong, x.ValueType)
		require.Equal(t, "top", x.Doc)

		y := got.GetAttribute(kw(":a/y"))
		require.NotNil(t, y)
		require.Equal(t, CardinalityMany, y.Cardinality)

		require.NotNil(t, got.GetAttribute(kw(":a/z")))
	})

	t.Run("the result is a schema of its own", func(t *testing.T) {
		got := Overlay(base, top)
		got.Add(&AttributeDefinition{Ident: kw(":a/w"), ValueType: TypeBoolean})
		require.Nil(t, base.GetAttribute(kw(":a/w")))
		require.Nil(t, top.GetAttribute(kw(":a/w")))
	})

	t.Run("a nil base contributes nothing", func(t *testing.T) {
		got := Overlay((*Schema)(nil), top)
		require.Equal(t, 2, got.Count())
		require.NotNil(t, got.GetAttribute(kw(":a/x")))
		require.NotNil(t, got.GetAttribute(kw(":a/z")))
	})

	t.Run("a nil top is the base", func(t *testing.T) {
		got := Overlay(base, (*Schema)(nil))
		require.Equal(t, 2, got.Count())
		require.Equal(t, "base", got.GetAttribute(kw(":a/x")).Doc)
	})
}
