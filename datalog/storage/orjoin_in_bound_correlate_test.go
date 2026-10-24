package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// An or-join branch sees the required run of its header and nothing else of
// the enclosing scope (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.2). A
// query input a branch reads is named in the required run, whether a scalar,
// a tuple input's symbol, or a subquery's own input; a variable a branch
// binds that shares an input's name, with the header not naming it, is the
// branch's own. The tests below pin each shape on both planner paths.

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

// TestOrJoinInBoundCorrelate pins the both-branches shape: the input ?ref,
// named in the required run, reaches both branches, and exactly the entity
// whose name or code equals it matches.
func TestOrJoinInBoundCorrelate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-join [[?ref] ?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"the input the required run names reaches both branches")
		})
	}
}

// TestOrJoinInBoundMixedCorrelates pins the mixed shape: each branch reads
// one of the two inputs the required run names, so each input quantifies
// its own branch.
func TestOrJoinInBoundMixedCorrelates(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-join [[?site ?region] ?e]
			                        [?e :entity/site ?site]
			                        [?e :entity/region ?region])]`,
				site, region)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{inSite.String(): true, inRegion.String(): true}, got,
				"each input the required run names quantifies its own branch")
		})
	}
}

// TestOrJoinClauseBoundCorrelate pins the both-branches shape with ?ref
// bound by a pattern of the enclosing scope: named in the required run, it
// reaches both branches as an input does.
func TestOrJoinClauseBoundCorrelate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-join [[?ref] ?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				region)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"a variable a pattern binds and the required run names reaches both branches")
		})
	}
}

// TestOrJoinInBoundMixedCorrelatesAggregate pins the aggregate find over the
// mixed shape: one tuple, the count of the entities the branches admit.
func TestOrJoinInBoundMixedCorrelatesAggregate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-join [[?site ?region] ?e]
			                        [?e :entity/site ?site]
			                        [?e :entity/region ?region])]`,
				site, region)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			require.True(t, iter.Next(), "aggregate must produce one tuple")
			require.Equal(t, int64(2), iter.Tuple()[0],
				"the count is over the entities the branches admit")
			require.NoError(t, iter.Error())
		})
	}
}

// TestOrJoinTupleInputEnvironment pins a tuple input's symbol as an input
// like a scalar: named in the required run, ?ref reaches both branches.
func TestOrJoinTupleInputEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      :in $ [?region ?ref]
			                      :where
			                      [?e :entity/region ?region]
			                      (or-join [[?ref] ?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				[]interface{}{region, "alpha"})
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"a tuple input's symbol the required run names reaches both branches")
		})
	}
}

// TestOrJoinBranchPredicateReadsAnInputThroughTheHeader: a branch predicate
// reads an input through the or-join's header, which names it in the
// required run.
func TestOrJoinBranchPredicateReadsAnInputThroughTheHeader(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-join [[?ref] ?e]
			                        (and [?e :entity/name ?n] [(= ?n ?ref)])
			                        [?e :entity/code ?ref])]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"a branch predicate reads the input its header's required run names")
		})
	}
}

// TestOrDefaultJoinBranchConsumesEnvironment pins a fallback whose first
// branch reads the input ?ref through the required run: the decision is
// taken per entity and input, true for the entity named alpha and false for
// the other.
func TestOrDefaultJoinBranchConsumesEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-default-join [[?e ?ref] ?flag]
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
				"the fallback is decided per entity and the input the required run names")
		})
	}
}

// TestOrDefaultJoinCacheableBranchConsumesEnvironment pins a fallback whose
// first branch is two patterns, one reading the input ?ref through the
// required run: the entity named alpha gets its code, the other the default.
func TestOrDefaultJoinCacheableBranchConsumesEnvironment(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-default-join [[?e ?ref] ?v]
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
				"the fallback is decided per entity and the input the required run names")
		})
	}
}

// TestOrJoinWideHeaderInBound is a regression pin: the wide-header rewrite of
// the both-branches shape works today and must keep working.
func TestOrJoinWideHeaderInBound(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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

// TestOrJoinEnvironmentBesideARelationInput pins the scalar input ?ref,
// named in the required run, reaching the branches when a relation input
// binds ?r as a group of two regions the plan runs once over.
func TestOrJoinEnvironmentBesideARelationInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                      (or-join [[?ref] ?e]
			                        [?e :entity/name ?ref]
			                        [?e :entity/code ?ref])]`,
				"alpha", [][]interface{}{{regionA}, {regionB}})
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{namedA.String(): true, namedB.String(): true}, got,
				"the input the required run names reaches the branches beside a relation input")
		})
	}
}

// TestOrJoinBranchNotReadsAnExternalThroughTheHeader: a WHERE-bound outer
// symbol reaches a bare (not ...) inside a branch only through the or-join's
// header. Outside the header it is local to the branch, so the not excludes
// every entity carrying the attribute. Named in the required run it
// correlates: the binding reaches the branch, and the branch's anti-join
// bridges the groups its keys span — see
// BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP.
func TestOrJoinBranchNotReadsAnExternalThroughTheHeader(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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

			// Under the header [?e], the branch's ?outer is local: branch 1
			// admits things with no flag at all, unflagged alone, and branch
			// 2 (special only) admits flagged.
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
			}, got,
				"a branch variable outside the header is local, so the branch NOT excludes every flagged thing")

			// Under [[?outer] ?e], ?outer = "hot" (WHERE-bound) reaches the
			// branch: branch 1 admits things NOT flagged "hot", unflagged and
			// otherFlag ("cold"), and branch 2 admits flagged.
			rel, err = d.Query(`[:find ?e
			                     :where
			                     [_ :probe/flag ?outer]
			                     (or-join [[?outer] ?e]
			                       (and [?e :entity/kind "thing"] (not [?e :entity/flag ?outer]))
			                       [?e :entity/special true])]`)
			require.NoError(t, err)
			got = collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				flagged.String():   true,
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"a WHERE-bound external the required run names correlates into the branch NOT")
		})
	}
}

// TestOrBranchNotConsumesExternalInSeparateGroup pins outer-group selection
// for the inference forms: a plain-or branch whose only outer correlate is
// NOT-consumed must pull that correlate's group into the outer relation.
func TestOrBranchNotConsumesExternalInSeparateGroup(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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

// TestOrJoinInBoundCorrelateInSubquery pins an or-join inside a subquery
// reading the subquery's own input ?r through its required run.
func TestOrJoinInBoundCorrelateInSubquery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
			                           (or-join [[?r] ?e2]
			                             [?e2 :entity/name ?r]
			                             [?e2 :entity/code ?r])]
			                          $ ?region ?ref) [[?e] ...]]]`,
				region, "alpha")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{named.String(): true}, got,
				"the subquery's input its or-join's required run names reaches the branches")
		})
	}
}
