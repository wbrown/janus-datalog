package storage

import (
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
			d := createOptimizerModeDB(t, mode, nil)
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
			d := createOptimizerModeDB(t, mode, nil)
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
			d := createOptimizerModeDB(t, mode, nil)
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

// The tests below pin the ruling of NOTJOIN_HEADER_ENV_BINDING_DERIVATION
// (docs/wip): an :in-bound environment symbol is an ordinary bound variable
// in every not/not-join binding domain — declaring it in a not-join header is
// valid (Datomic compatibility), and a body consuming it only through a
// predicate is valid with the header declaring it. The binding domain at
// every home is outer-relation symbols ∪ environment.

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
			d := createOptimizerModeDB(t, mode, nil)
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
			d := createOptimizerModeDB(t, mode, nil)
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
			d := createOptimizerModeDB(t, mode, nil)
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

// TestNotJoinOmittedHeaderBodyProvidesInBoundParameter pins shape 3: the
// header omits the :in-bound symbol, the body pattern provides it, and the
// executor's environment bridging correlates it. This shape is unaffected
// by the header-env-binding ruling — a header may omit a symbol the body
// itself binds.
func TestNotJoinOmittedHeaderBodyProvidesInBoundParameter(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not-join [?e]
			                        [?e :entity/flag ?flag])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, notSpanningExpected(unflagged, otherFlag), got,
				"an omitted-header not-join with a body-provided :in-bound symbol keeps working")
		})
	}
}

// TestNotJoinOmittedHeaderPredicateConsumptionRejected pins shape 4 and the
// header-declaration language rule (query/clause_scope.go): a body consuming
// an outer requirement only through a predicate must declare it in the
// header. This holds for environment symbols too — shape 2 is the valid
// spelling — and the rejection is mode-independent (it happens at parse).
func TestNotJoinOmittedHeaderPredicateConsumptionRejected(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
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
				"a not-join header must declare predicate-only outer requirements, environment or not")
			require.Contains(t, err.Error(), "must declare every outer requirement")
		})
	}
}

// TestNotJoinDeclaredEnvSymbolUnusedByBodyErrors pins the over-declaration
// rule for environment symbols: a header symbol the body neither produces
// nor consumes declares a correlation that does not exist — rejected on
// both modes, mirroring the or-join family's rule that every declared
// header symbol must be bound by the branches. The shape's likely origin
// is a body pattern edited into a literal with the header left behind;
// silent tolerance would hide that authoring error.
func TestNotJoinDeclaredEnvSymbolUnusedByBodyErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
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
			require.Contains(t, err.Error(), "neither produced nor consumed")
		})
	}
}

// TestNotJoinDeclaredOuterSymbolUnusedByBodyErrors pins the same rule for a
// WHERE-bound header symbol — the sibling shape: both modes must reject it,
// not let the executor key the anti-join on the unused symbol vacuously.
func TestNotJoinDeclaredOuterSymbolUnusedByBodyErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :where
			                    [_ :probe/flag ?outer]
			                    [?e :entity/kind "thing"]
			                    (not-join [?e ?outer]
			                      [?e :entity/flag "hot"])]`)
			require.Error(t, err,
				"a declared header symbol the body never mentions must reject on both modes")
			require.Contains(t, err.Error(), "neither produced nor consumed")
		})
	}
}

// TestNotJoinHeaderSymbolBoundNowhereErrors is the negative control for the
// widened binding domain: a header symbol bound neither by the outer
// relation nor by :in is still an error, not a vacuous match.
func TestNotJoinHeaderSymbolBoundNowhereErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
			notSpanningFixture(t, d)

			_, err := d.Query(`[:find ?e
			                    :where
			                    [?e :entity/kind "thing"]
			                    (not-join [?e ?nowhere]
			                      [?e :entity/flag ?nowhere])]`)
			// The rejection layer differs by mode (algebra compile names the
			// symbol; the baseline planner rejects at phasing) — the pinned
			// behavior is that rejection survives the widened binding domain.
			require.Error(t, err,
				"a not-join header symbol bound nowhere must stay an error under the widened domain")
		})
	}
}

// TestNotBodyPredicateConsumesUnboundSymbolErrors is the bare-not negative
// control: a body predicate consuming a symbol bound nowhere is still an
// error, not silently existential.
func TestNotBodyPredicateConsumesUnboundSymbolErrors(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
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
			d := createOptimizerModeDB(t, mode, nil)
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
			d := createOptimizerModeDB(t, mode, nil)
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

// TestNotJoinScalarEnvWithRelationInputIteration pins the per-Run
// environment path: a relation input iterates the query per tuple, and each
// Run's environment carries the scalar ?flag alongside the Run's ?kind. The
// "other" Run matches nothing; the union is the "thing" Run's tuples.
func TestNotJoinScalarEnvWithRelationInputIteration(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)
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
				"per-Run environments must carry the scalar correlate into the not-join")
		})
	}
}
