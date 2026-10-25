package storage

import "testing"

// A data pattern naming one variable at two positions unifies them, as a
// symbol written twice in any binding form does: [?x :t/ref ?x] selects the
// datoms whose entity and value agree, under both optimizer paths. a refers
// to itself and c to itself; b refers to c.
func TestPattern_AVariableNamedTwiceSelectsAgreeingDatoms(t *testing.T) {
	a, b, c := named("a"), named("b"), named("c")
	facts := []fact{{a, ":t/ref", a}, {b, ":t/ref", c}, {c, ":t/ref", c}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			everyOrder(t, d, names, "?x", []string{`[?x :t/ref ?x]`}, []string{"a", "c"})
			everyOrder(t, d, names, "?x", []string{`[?x :t/ref ?x]`, `[?x :t/ref _]`}, []string{"a", "c"})
			everyOrder(t, d, names, "?x ?y", []string{`[?y :t/ref ?x]`, `[?x :t/ref ?x]`}, []string{"a a", "c b", "c c"})
		})
	}
}
