package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Reproducers for BUG_ORJOIN_IN_BOUND_CORRELATE_TREATED_AS_BRANCH_LOCAL:
// an or-join branch external bound only by :in must correlate with the input
// binding exactly as a clause-bound external does. The documented contract
// (skill or-join section) keeps branch-specific correlates out of the header;
// under the branch-locals rule an :in-bound external misclassifies as a fresh
// per-branch local and the or-join silently over-matches. :in parameters are
// environment — the query's formal parameters, visible in every clause scope
// including or-branch bodies — so both planner modes must correlate them.

func collectEntityResults(t *testing.T, rel executor.Relation) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	iter := rel.Iterator()
	defer iter.Close()
	for iter.Next() {
		tuple := iter.Tuple()
		require.Len(t, tuple, 1)
		id, ok := tuple[0].(datalog.Identity)
		require.True(t, ok, "?e must be an Identity, got %T", tuple[0])
		got[id.String()] = true
	}
	require.NoError(t, iter.Error())
	return got
}

// TestOrJoinInBoundCorrelate pins the both-branches shape: ?ref is bound only
// by :in, the header stays narrow per the documented contract, and exactly
// the entity whose name equals the input may match.
func TestOrJoinInBoundCorrelate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")
			coded := datalog.NewIdentity("entity:coded")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed, coded} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			require.NoError(t, tx.Add(coded, datalog.NewKeyword(":entity/code"), "code-1"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?region ?ref
			                      :where
			                      [?e :entity/region ?region]
			                      (or-join [?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"an :in-bound ?ref must correlate into the branches, not degenerate to has-any-name/has-any-code")
		})
	}
}

// TestOrJoinInBoundMixedCorrelates pins the mixed-correlate admission shape —
// the documented example with no wide-header rewrite: each branch binds only
// one of the two :in-bound correlates, so the query is only expressible if
// :in-bound externals correlate.
func TestOrJoinInBoundMixedCorrelates(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			site := datalog.NewIdentity("site:this")
			region := datalog.NewIdentity("region:this")
			otherSite := datalog.NewIdentity("site:other")
			otherRegion := datalog.NewIdentity("region:other")

			inSite := datalog.NewIdentity("entity:in-site")
			inRegion := datalog.NewIdentity("entity:in-region")
			foreignSite := datalog.NewIdentity("entity:foreign-site")
			foreignRegion := datalog.NewIdentity("entity:foreign-region")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{inSite, inRegion, foreignSite, foreignRegion} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"))
			}
			require.NoError(t, tx.Add(inSite, datalog.NewKeyword(":entity/site"), site))
			require.NoError(t, tx.Add(inRegion, datalog.NewKeyword(":entity/region"), region))
			require.NoError(t, tx.Add(foreignSite, datalog.NewKeyword(":entity/site"), otherSite))
			require.NoError(t, tx.Add(foreignRegion, datalog.NewKeyword(":entity/region"), otherRegion))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?site ?region
			                      :where
			                      [?e :entity/kind "thing"]
			                      (or-join [?e]
			                        [?e :entity/site ?site]
			                        [?e :entity/region ?region])]`,
				site, region)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{inSite.String(): true, inRegion.String(): true}, got,
				"foreign-site/foreign-region entities must not be admitted; each :in-bound correlate quantifies its branch")
		})
	}
}

// TestOrJoinClauseBoundCorrelate is the green control: the identical shape
// with ?ref bound by a where clause instead of :in. This behavior is correct
// at head and must stay correct while the :in-bound classification is fixed.
func TestOrJoinClauseBoundCorrelate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			probe := datalog.NewIdentity("entity:probe")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(probe, datalog.NewKeyword(":probe/ref"), "alpha"))
			for _, e := range []datalog.Identity{named, otherNamed} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?region
			                      :where
			                      [_ :probe/ref ?ref]
			                      [?e :entity/region ?region]
			                      (or-join [?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				region)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"clause-bound correlates work at head and must keep working")
		})
	}
}

// TestOrJoinInBoundMixedCorrelatesAggregate pins the aggregate find over the
// mixed-correlate shape: the residual all-environment group ({?site ?region})
// must absorb at the find boundary instead of tripping the disjoint-groups
// aggregation error.
func TestOrJoinInBoundMixedCorrelatesAggregate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			site := datalog.NewIdentity("site:this")
			region := datalog.NewIdentity("region:this")
			otherSite := datalog.NewIdentity("site:other")

			inSite := datalog.NewIdentity("entity:in-site")
			inRegion := datalog.NewIdentity("entity:in-region")
			foreignSite := datalog.NewIdentity("entity:foreign-site")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{inSite, inRegion, foreignSite} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"))
			}
			require.NoError(t, tx.Add(inSite, datalog.NewKeyword(":entity/site"), site))
			require.NoError(t, tx.Add(inRegion, datalog.NewKeyword(":entity/region"), region))
			require.NoError(t, tx.Add(foreignSite, datalog.NewKeyword(":entity/site"), otherSite))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find (count ?e)
			                      :in $ ?site ?region
			                      :where
			                      [?e :entity/kind "thing"]
			                      (or-join [?e]
			                        [?e :entity/site ?site]
			                        [?e :entity/region ?region])]`,
				site, region)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			require.True(t, iter.Next(), "aggregate must produce one tuple")
			require.Equal(t, int64(2), iter.Tuple()[0],
				"count over the mixed-correlate shape must absorb the residual environment group")
			require.NoError(t, iter.Error())
		})
	}
}

// TestOrJoinTupleInputEnvironment pins tuple-input parameters as environment:
// a TupleInput's jointly-bound symbols must correlate into branches exactly
// as scalars do.
func TestOrJoinTupleInputEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")
			coded := datalog.NewIdentity("entity:coded")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed, coded} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			require.NoError(t, tx.Add(coded, datalog.NewKeyword(":entity/code"), "code-1"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ [[?region ?ref]]
			                      :where
			                      [?e :entity/region ?region]
			                      (or-join [?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				[]interface{}{region, "alpha"})
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"tuple-input parameters are environment and must correlate into branches")
		})
	}
}

// TestOrJoinBranchPredicateConsumesEnvironment pins environment consumption
// through a branch predicate (the non-cacheable branch arm).
func TestOrJoinBranchPredicateConsumesEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?region ?ref
			                      :where
			                      [?e :entity/region ?region]
			                      (or-join [?e]
			                        (and [?e :entity/name ?n] [(= ?n ?ref)])
			                        [?e :entity/code ?ref])]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"a branch predicate consuming an :in parameter must see the environment")
		})
	}
}

// TestOrDefaultJoinBranchConsumesEnvironment pins the short-circuit route's
// non-cacheable arm: an or-default-join branch consuming an :in parameter
// must evaluate under the environment, per outer tuple.
func TestOrDefaultJoinBranchConsumesEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e ?flag
			                      :in $ ?region ?ref
			                      :where
			                      [?e :entity/region ?region]
			                      (or-default-join [[?e] ?flag]
			                        (and [?e :entity/name ?ref] [(ground true) ?flag])
			                        [(ground false) ?flag])]`,
				region, "alpha")
			require.NoError(t, err)
			got := map[string]bool{}
			iter := rel.Iterator()
			defer iter.Close()
			for iter.Next() {
				tuple := iter.Tuple()
				require.Len(t, tuple, 2)
				id, ok := tuple[0].(datalog.Identity)
				require.True(t, ok)
				got[id.String()] = tuple[1].(bool)
			}
			require.NoError(t, iter.Error())
			require.Equal(t, map[string]bool{named.String(): true, otherNamed.String(): false}, got,
				"the fallback decision must be quantified by the environment value, not has-any-name")
		})
	}
}

// TestOrDefaultJoinCacheableBranchConsumesEnvironment pins the short-circuit
// route's cacheable arm: a DataPattern-only branch consuming an :in parameter
// evaluates once, narrowed by the outer join keys AND the environment.
func TestOrDefaultJoinCacheableBranchConsumesEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/code"), "code-1"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/code"), "code-2"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e ?v
			                      :in $ ?region ?ref
			                      :where
			                      [?e :entity/region ?region]
			                      (or-default-join [[?e] ?v]
			                        (and [?e :entity/name ?ref] [?e :entity/code ?v])
			                        [(ground "none") ?v])]`,
				region, "alpha")
			require.NoError(t, err)
			got := map[string]string{}
			iter := rel.Iterator()
			defer iter.Close()
			for iter.Next() {
				tuple := iter.Tuple()
				require.Len(t, tuple, 2)
				id, ok := tuple[0].(datalog.Identity)
				require.True(t, ok)
				got[id.String()] = tuple[1].(string)
			}
			require.NoError(t, iter.Error())
			require.Equal(t, map[string]string{named.String(): "code-1", otherNamed.String(): "none"}, got,
				"the cacheable branch's single evaluation must be environment-narrowed")
		})
	}
}

// TestOrJoinWideHeaderInBound is a regression pin: the wide-header rewrite of
// the both-branches shape works today and must keep working.
func TestOrJoinWideHeaderInBound(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")
			coded := datalog.NewIdentity("entity:coded")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed, coded} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			require.NoError(t, tx.Add(coded, datalog.NewKeyword(":entity/code"), "code-1"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?region ?ref
			                      :where
			                      [?e :entity/region ?region]
			                      (or-join [?e ?ref]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"the wide-header form works today and must keep working")
		})
	}
}

// TestOrJoinEnvironmentUnderRelationInputIteration pins the per-tuple
// iteration path (preparedIteration.Run): the environment must bind per Run,
// alongside the iterated relation input.
func TestOrJoinEnvironmentUnderRelationInputIteration(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			regionA := datalog.NewIdentity("region:a")
			regionB := datalog.NewIdentity("region:b")
			namedA := datalog.NewIdentity("entity:named-a")
			otherA := datalog.NewIdentity("entity:other-a")
			namedB := datalog.NewIdentity("entity:named-b")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(namedA, datalog.NewKeyword(":entity/region"), regionA))
			require.NoError(t, tx.Add(otherA, datalog.NewKeyword(":entity/region"), regionA))
			require.NoError(t, tx.Add(namedB, datalog.NewKeyword(":entity/region"), regionB))
			require.NoError(t, tx.Add(namedA, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherA, datalog.NewKeyword(":entity/name"), "beta"))
			require.NoError(t, tx.Add(namedB, datalog.NewKeyword(":entity/name"), "alpha"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?ref [[?r] ...]
			                      :where
			                      [?e :entity/region ?r]
			                      (or-join [?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				"alpha", [][]interface{}{{regionA}, {regionB}})
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{namedA.String(): true, namedB.String(): true}, got,
				"the environment must bind on every per-tuple Run of the iterated plan")
		})
	}
}

// TestOrJoinBranchNotConsumesWhereBoundExternal pins the externals gap: a
// WHERE-bound outer symbol consumed only by a bare (not ...) inside a branch,
// outside the header, must correlate. The canonical visibility derivation
// delivers the binding into the branch, and the branch's anti-join bridges
// the groups its keys span — see
// BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP.
func TestOrJoinBranchNotConsumesWhereBoundExternal(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			probe := datalog.NewIdentity("probe:1")
			flagged := datalog.NewIdentity("entity:flagged")
			unflagged := datalog.NewIdentity("entity:unflagged")
			otherFlag := datalog.NewIdentity("entity:other-flag")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(probe, datalog.NewKeyword(":probe/flag"), "hot"))
			for _, e := range []datalog.Identity{flagged, unflagged, otherFlag} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"))
			}
			require.NoError(t, tx.Add(flagged, datalog.NewKeyword(":entity/flag"), "hot"))
			require.NoError(t, tx.Add(flagged, datalog.NewKeyword(":entity/special"), true))
			require.NoError(t, tx.Add(otherFlag, datalog.NewKeyword(":entity/flag"), "cold"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// ?outer = "hot" (WHERE-bound). Branch 1 admits things NOT
			// flagged ?outer: unflagged (no flag) and otherFlag ("cold").
			// Under the existential-NOT defect, otherFlag is wrongly
			// excluded (it has *a* flag) — branch 2 (special only) does not
			// re-admit it, so the errors cannot cancel.
			rel, err := d.Query(`[:find ?e
			                      :where
			                      [_ :probe/flag ?outer]
			                      (or-join [?e]
			                        (and [?e :entity/kind "thing"] (not [?e :entity/flag ?outer]))
			                        [?e :entity/special true])]`)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				flagged.String():   true,
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"a WHERE-bound external consumed by a branch NOT must correlate, not turn existential")
		})
	}
}

// TestOrBranchNotConsumesExternalInSeparateGroup pins outer-group selection
// for the inference forms: a plain-or branch whose only outer correlate is
// NOT-consumed must pull that correlate's group into the outer relation.
func TestOrBranchNotConsumesExternalInSeparateGroup(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			probe := datalog.NewIdentity("probe:1")
			flagged := datalog.NewIdentity("entity:flagged")
			unflagged := datalog.NewIdentity("entity:unflagged")
			otherFlag := datalog.NewIdentity("entity:other-flag")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(probe, datalog.NewKeyword(":probe/flag"), "hot"))
			for _, e := range []datalog.Identity{flagged, unflagged, otherFlag} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"))
			}
			require.NoError(t, tx.Add(flagged, datalog.NewKeyword(":entity/flag"), "hot"))
			require.NoError(t, tx.Add(flagged, datalog.NewKeyword(":entity/special"), true))
			require.NoError(t, tx.Add(otherFlag, datalog.NewKeyword(":entity/flag"), "cold"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Branch 2 (special only) cannot re-admit what branch 1's
			// existential-NOT defect wrongly excludes — no cancellation.
			rel, err := d.Query(`[:find ?e
			                      :where
			                      [_ :probe/flag ?outer]
			                      [?e :entity/kind "thing"]
			                      (or (and [?e :entity/kind "thing"] (not [?e :entity/flag ?outer]))
			                          [?e :entity/special true])]`)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				flagged.String():   true,
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"a NOT-only outer correlate must select its group into the plain-or outer relation")
		})
	}
}

// TestOrJoinInBoundCorrelateInSubquery pins the environment's scope boundary:
// the or-join lives inside a subquery whose own :in binds the correlate, so
// the inner query's environment — not the outer's — must reach the branches.
func TestOrJoinInBoundCorrelateInSubquery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, nil)

			region := datalog.NewIdentity("region:1")
			named := datalog.NewIdentity("entity:named")
			otherNamed := datalog.NewIdentity("entity:other-named")
			coded := datalog.NewIdentity("entity:coded")

			tx := d.NewTransaction()
			for _, e := range []datalog.Identity{named, otherNamed, coded} {
				require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/region"), region))
			}
			require.NoError(t, tx.Add(named, datalog.NewKeyword(":entity/name"), "alpha"))
			require.NoError(t, tx.Add(otherNamed, datalog.NewKeyword(":entity/name"), "beta"))
			require.NoError(t, tx.Add(coded, datalog.NewKeyword(":entity/code"), "code-1"))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?region ?ref
			                      :where
			                      [(q [:find ?e2
			                           :in $ ?g ?r
			                           :where
			                           [?e2 :entity/region ?g]
			                           (or-join [?e2]
			                             [?e2 :entity/name ?r]
			                             [?e2 :entity/code ?r])]
			                          $ ?region ?ref) [[?e] ...]]]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"the subquery's own :in environment must correlate inside its or-join branches")
		})
	}
}
