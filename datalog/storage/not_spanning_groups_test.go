package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Reproducers for BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP: a not
// clause whose variables span disjoint relation groups must not anti-join
// each group separately — that wipes the group carrying only the correlate
// with a partially-bound body, and turns the entity-side anti-join
// existential ("has any value") instead of correlated. The correct execution
// joins the spanning groups (the clause's correlation is the connector,
// exactly as a bridging predicate's theta-join), anti-joins once on the full
// key set, and replaces the spanning groups with the filtered join.

// notSpanningFixture writes three "thing" entities: one flagged with the
// probe's value ("hot"), one flagged differently ("cold"), one unflagged.
// The probe entity carries the value the NOT must correlate against.
func notSpanningFixture(t *testing.T, d *Database) (flagged, unflagged, otherFlag datalog.Identity) {
	t.Helper()
	probe := datalog.NewIdentity("probe:1")
	flagged = datalog.NewIdentity("entity:flagged")
	unflagged = datalog.NewIdentity("entity:unflagged")
	otherFlag = datalog.NewIdentity("entity:other-flag")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(probe, datalog.NewKeyword(":probe/flag"), "hot"))
	for _, e := range []datalog.Identity{flagged, unflagged, otherFlag} {
		require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"))
	}
	require.NoError(t, tx.Add(flagged, datalog.NewKeyword(":entity/flag"), "hot"))
	require.NoError(t, tx.Add(otherFlag, datalog.NewKeyword(":entity/flag"), "cold"))
	_, err := tx.Commit()
	require.NoError(t, err)
	return flagged, unflagged, otherFlag
}

// TestNotSpanningDisjointGroups pins the bare-not shape at top level: ?outer
// is WHERE-bound in its own group, and the NOT must exclude exactly the
// entities flagged with the outer value — not every entity with any flag.
func TestNotSpanningDisjointGroups(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :where
			                      [_ :probe/flag ?outer]
			                      [?e :entity/kind "thing"]
			                      (not [?e :entity/flag ?outer])]`)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"the NOT must correlate ?outer across groups: only the hot-flagged entity is excluded")
		})
	}
}

// TestNotJoinSpanningDisjointGroups pins the explicit-header form of the same
// shape: not-join declares both variables, whose bindings live in two
// disjoint groups — the declared header selects and bridges them (the
// original defect errored per-group instead).
func TestNotJoinSpanningDisjointGroups(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :where
			                      [_ :probe/flag ?outer]
			                      [?e :entity/kind "thing"]
			                      (not-join [?e ?outer]
			                        [?e :entity/flag ?outer])]`)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"a not-join whose declared header spans disjoint groups must bridge them")
		})
	}
}

// TestNotBodyConsumesInBoundParameter pins the environment leg of the class:
// the correlate reaches the query through :in. Pattern-consumed, it rides
// the bound-input relation as a symbol in its own group; the NOT's bridging
// must pull that group in exactly as for a WHERE-bound correlate.
func TestNotBodyConsumesInBoundParameter(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not [?e :entity/flag ?flag])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"an :in-bound correlate consumed by a NOT body must correlate, not turn existential")
		})
	}
}

// The tests below pin the header rule of
// docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.2 on a not-join reading a
// query input. The binding domain of a not-join body is its header alone,
// the variables the enclosing scope binds under the names the header lists,
// and the environment is not in it: the body reads an input only when the
// header names it, and a variable the body binds that shares an input's
// name, with the header not naming it, is the body's own. A plain not's
// domain is the header the text infers, which names an input its body
// reads.

// notSpanningExpected is the correct anti-join answer on notSpanningFixture
// with the probe value "hot": the hot-flagged entity is excluded, the
// cold-flagged and unflagged entities remain.
func notSpanningExpected(unflagged, otherFlag datalog.Identity) map[string]bool {
	return map[string]bool{
		unflagged.String(): true,
		otherFlag.String(): true,
	}
}

// TestNotJoinDeclaredEnvHeaderPatternProvided pins shape 1: the header
// declares the :in-bound symbol and the body pattern provides it. Plainly
// valid Datomic; the declaration matches trivially against the environment's
// single tuple.
func TestNotJoinDeclaredEnvHeaderPatternProvided(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not-join [?e ?flag]
			                        [?e :entity/flag ?flag])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, notSpanningExpected(unflagged, otherFlag), got,
				"a not-join header may declare an :in-bound symbol; it must correlate, not error")
		})
	}
}

// TestNotJoinDeclaredEnvHeaderPredicateConsumed pins shape 2: the header
// declares the :in-bound symbol and the body consumes it only through a
// predicate. The header declaration makes the correlation interface
// explicit; the environment supplies the value.
func TestNotJoinDeclaredEnvHeaderPredicateConsumed(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not-join [?e ?flag]
			                        [?e :entity/flag ?f]
			                        [(= ?f ?flag)])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, notSpanningExpected(unflagged, otherFlag), got,
				"a not-join body may consume a declared :in-bound symbol through a predicate")
		})
	}
}

// TestNotBodyPredicateConsumesInBoundParameter pins shape 6, the bare-not
// sibling of TestNotBodyConsumesInBoundParameter: the body consumes the
// :in-bound symbol only through a predicate. Plain not infers the
// correlation interface, so no header is involved.
func TestNotBodyPredicateConsumesInBoundParameter(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not [?e :entity/flag ?f]
			                           [(= ?f ?flag)])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, notSpanningExpected(unflagged, otherFlag), got,
				"a NOT body may consume an :in-bound symbol through a predicate alone")
		})
	}
}

// TestHeaderlessNotReadingOnlyAnInputDecidesTheScope pins the header rule of
// docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.2 on a plain not that
// reads a query input and nothing else of the scope: its inferred header is
// the input alone, the body runs once over the environment's tuple, and the
// body's answer decides the whole scope, in every written order. With a flag
// some thing carries the body has a result and every group is emptied; with
// a flag no thing carries the body has none and every thing remains.
func TestHeaderlessNotReadingOnlyAnInputDecidesTheScope(t *testing.T) {
	clauses := []string{`[?e :entity/kind "thing"]`, `(not [?x :entity/flag ?flag])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			flagged, unflagged, otherFlag := notSpanningFixture(t, d)
			for _, order := range orderings(len(clauses)) {
				ordered := make([]string, len(order))
				for i, at := range order {
					ordered[i] = clauses[at]
				}
				query := "[:find ?e :in $ ?flag :where " + strings.Join(ordered, " ") + "]"

				rel, err := d.Query(query, "hot")
				require.NoError(t, err, query)
				require.Empty(t, collectEntityResults(t, rel),
					"some thing carries the flag, so the body has a result and the scope is emptied: %s", query)

				rel, err = d.Query(query, "warm")
				require.NoError(t, err, query)
				require.Equal(t, map[string]bool{
					flagged.String():   true,
					unflagged.String(): true,
					otherFlag.String(): true,
				}, collectEntityResults(t, rel),
					"no thing carries the flag, so the body has no result and every thing remains: %s", query)
			}
		})
	}
}

// TestOneFormWhereOfAnEnvironmentOnlyNotDecidesTheScope pins the same rule
// on a :where of the not alone: its header is the input, its run the
// environment's tuple and its subject the unit relation
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.2), so the find's
// input is answered when no thing carries the flag and nothing is answered
// when one does.
func TestOneFormWhereOfAnEnvironmentOnlyNotDecidesTheScope(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			flagged, _, _ := notSpanningFixture(t, d)
			query := `[:find ?e
			           :in $ ?flag ?e
			           :where (not [?x :entity/flag ?flag])]`

			rel, err := d.Query(query, "hot", flagged)
			require.NoError(t, err)
			require.Empty(t, collectEntityResults(t, rel),
				"some thing carries the flag, so the body has a result and the scope is emptied")

			rel, err = d.Query(query, "warm", flagged)
			require.NoError(t, err)
			require.Equal(t, map[string]bool{flagged.String(): true}, collectEntityResults(t, rel),
				"no thing carries the flag, so the body has no result and the input is answered")
		})
	}
}

// TestNotJoinBodySeesOnlyItsHeader pins a not-join body variable sharing the
// name of an input the header does not name as the body's own: the body
// excludes every flagged entity, whatever the input holds.
func TestNotJoinBodySeesOnlyItsHeader(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, _ := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not-join [?e]
			                        [?e :entity/flag ?flag])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{unflagged.String(): true}, got,
				"a body variable the header does not name is the body's own, so every flagged entity is excluded")
		})
	}
}

// TestNotJoinOmittedHeaderPredicateConsumptionRejected pins shape 4 under
// the header rule (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4): a
// variable a not-join body reads and neither binds nor names in its header
// is refused at preparation naming the form and the variable, a query input
// sharing the name included, on both modes.
func TestNotJoinOmittedHeaderPredicateConsumptionRejected(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :in $ ?flag
			                    :where
			                    [?e :entity/kind "thing"]
			                    (not-join [?e]
			                      [?e :entity/flag ?f]
			                      [(= ?f ?flag)])]`,
				"hot")
			require.Error(t, err,
				"a not-join body reading a variable outside its header is refused, environment or not")
			require.Contains(t, err.Error(), "(not-join [?e]")
			require.Contains(t, err.Error(), "?flag")
		})
	}
}

// TestNotJoinDeclaredEnvSymbolUnusedByBodyErrors pins the over-declaration
// rule for environment symbols: a not-join header variable the body neither
// binds nor reads is refused at preparation naming the form and the
// variable, on both modes. The shape's likely origin is a body pattern
// edited into a literal with the header left behind; silent tolerance would
// hide that authoring error.
func TestNotJoinDeclaredEnvSymbolUnusedByBodyErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :in $ ?flag
			                    :where
			                    [?e :entity/kind "thing"]
			                    (not-join [?e ?flag]
			                      [?e :entity/flag "hot"])]`,
				"hot")
			require.Error(t, err,
				"a declared header symbol the body never mentions must reject, not silently drop")
			require.Contains(t, err.Error(), "(not-join [?e ?flag]")
			require.Contains(t, err.Error(), "?flag")
		})
	}
}

// TestNotJoinDeclaredOuterSymbolUnusedByBodyErrors pins the same rule for a
// WHERE-bound header symbol — the sibling shape: both modes must reject it,
// not let the executor key the anti-join on the unused symbol vacuously.
func TestNotJoinDeclaredOuterSymbolUnusedByBodyErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :where
			                    [_ :probe/flag ?outer]
			                    [?e :entity/kind "thing"]
			                    (not-join [?e ?outer]
			                      [?e :entity/flag "hot"])]`)
			require.Error(t, err,
				"a declared header symbol the body never mentions must reject on both modes")
			require.Contains(t, err.Error(), "(not-join [?e ?outer]")
			require.Contains(t, err.Error(), "?outer")
		})
	}
}

// TestNotJoinHeaderSymbolBoundNowhereErrors pins the refusal of a not-join
// whose header names a variable nothing in the enclosing scope binds
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4): the planner refuses it
// on both paths, naming the variable.
func TestNotJoinHeaderSymbolBoundNowhereErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :where
			                    [?e :entity/kind "thing"]
			                    (not-join [?e ?nowhere]
			                      [?e :entity/flag ?nowhere])]`)
			require.Error(t, err,
				"a not-join header variable nothing binds is refused")
			require.Contains(t, err.Error(), "?nowhere")
		})
	}
}

// TestNotBodyPredicateConsumesUnboundSymbolErrors is the bare-not negative
// control: a body predicate consuming a symbol bound nowhere is still an
// error, not silently existential.
func TestNotBodyPredicateConsumesUnboundSymbolErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :where
			                    [?e :entity/kind "thing"]
			                    (not [?e :entity/flag ?f]
			                         [(= ?f ?nowhere)])]`)
			require.Error(t, err,
				"a NOT body predicate consuming a symbol bound nowhere must stay an error")
			require.Contains(t, err.Error(), "?nowhere")
		})
	}
}

// collectEntityFlagResults collects two-symbol (?e ?flag) tuples into a set
// keyed "identity|flag".
func collectEntityFlagResults(t *testing.T, rel executor.Relation) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	iter := rel.Iterator()
	defer iter.Close()
	for iter.Next() {
		tuple := iter.Tuple()
		require.Len(t, tuple, 2)
		id, ok := tuple[0].(datalog.Identity)
		require.True(t, ok, "?e must be an Identity, got %T", tuple[0])
		flag, ok := tuple[1].(string)
		require.True(t, ok, "?flag must be a string, got %T", tuple[1])
		got[id.String()+"|"+flag] = true
	}
	require.NoError(t, iter.Error())
	return got
}

// TestNotJoinDeclaredEnvHeaderCollectionInput pins the iterated-input leg of
// shape 1: ?flag arrives as a collection input, which binds as a data
// relation. The result must be the union of per-value anti-joins — a fix
// that only works for a single scalar fails the cold-value tuples.
func TestNotJoinDeclaredEnvHeaderCollectionInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			flagged, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e ?flag
			                      :in $ [?flag ...]
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not-join [?e ?flag]
			                        [?e :entity/flag ?flag])]`,
				[]interface{}{"hot", "cold"})
			require.NoError(t, err)
			got := collectEntityFlagResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String() + "|hot":  true,
				otherFlag.String() + "|hot":  true,
				unflagged.String() + "|cold": true,
				flagged.String() + "|cold":   true,
			}, got,
				"a collection-input header symbol anti-joins per value")
		})
	}
}

// TestNotJoinScalarEnvWithCollectionDataInput pins the env-projection leg:
// an unrelated collection input makes the bound-input relation multi-tuple,
// and the environment must still be the single-tuple projection of the
// scalar symbols. Only "thing" matches the fixture, so the answer is the
// scalar shape's answer.
func TestNotJoinScalarEnvWithCollectionDataInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag [?kind ...]
			                      :where
			                      [?e :entity/kind ?kind]
			                      (not-join [?e ?flag]
			                        [?e :entity/flag ?flag])]`,
				"hot", []interface{}{"thing", "other"})
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, notSpanningExpected(unflagged, otherFlag), got,
				"a multi-tuple bound-input relation must not perturb the scalar environment")
		})
	}
}

// TestNotJoinScalarEnvBesideARelationInput pins the environment beside a
// relation input: the scalar ?flag is the environment the not-join's header
// reads, and [[?kind] ...] binds two kinds as a group the plan runs once
// over. The "other" kind matches no entity; the answer is the "thing"
// entities.
func TestNotJoinScalarEnvBesideARelationInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag [[?kind] ...]
			                      :where
			                      [?e :entity/kind ?kind]
			                      (not-join [?e ?flag]
			                        [?e :entity/flag ?flag])]`,
				"hot", [][]interface{}{{"thing"}, {"other"}})
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, notSpanningExpected(unflagged, otherFlag), got,
				"the environment carries the scalar correlate into the not-join beside a relation input")
		})
	}
}
