package schema

// Overlay composes two schemas into a third: every definition of top, and each
// definition of base whose ident top does not define. The result is a schema of
// its own, so adding to it changes neither argument. Either argument may be a
// nil *Schema, which contributes nothing.
func Overlay(base, top SchemaProvider) *Schema {
	out := NewSchema()
	for _, def := range base.Attributes() {
		out.Add(def)
	}
	for _, def := range top.Attributes() {
		out.Add(def)
	}
	return out
}
