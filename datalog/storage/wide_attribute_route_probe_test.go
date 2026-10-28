package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A keyword wider than storage holds, bound at the attribute position, names
// no stored attribute on any binding-driven route: each route is forced in
// turn and the answer is the tuples whose attribute is a stored one alone.
func TestWideAttributeBindingProbeEveryRoute(t *testing.T) {
	short := ":t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	wideText := short + "z"
	if len(short) != datalog.MaxAttributeBytes {
		t.Fatalf("short is %d bytes", len(short))
	}
	shortKw := datalog.NewKeyword(short)
	wide := datalog.NewKeyword(wideText)
	name := datalog.NewKeyword(":t/name")
	alice := datalog.NewIdentity("probe-alice")
	bob := datalog.NewIdentity("probe-bob")

	eSym := datalog.NewSymbol("?e")
	aSym := datalog.NewSymbol("?a")
	vSym := datalog.NewSymbol("?v")
	open := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: eSym}, query.Variable{Name: aSym}, query.Variable{Name: vSym},
	}}
	onAlice := &query.DataPattern{Elements: []query.PatternElement{
		query.Constant{Value: alice}, query.Variable{Name: aSym}, query.Variable{Name: vSym},
	}}

	ea := func(tuples ...executor.Tuple) executor.Relation {
		return executor.NewMaterializedRelation([]query.Symbol{eSym, aSym}, tuples)
	}
	aOnly := func(tuples ...executor.Tuple) executor.Relation {
		return executor.NewMaterializedRelation([]query.Symbol{aSym}, tuples)
	}

	hash, merge := HashJoinScan, MergeJoin
	type route struct {
		name      string
		cache     bool
		force     *JoinStrategy
		perBind   bool
		pattern   *query.DataPattern
		bindings  func() executor.Relation
		want      []string // values answered
		wantRoute string
	}
	routes := []route{
		{"cache, A fixed", true, nil, false, open, func() executor.Relation { return ea(executor.Tuple{alice, wide}) }, nil, ""},
		{"hash join, E and A bound, one tuple", false, nil, false, open, func() executor.Relation { return ea(executor.Tuple{alice, wide}) }, nil, string(annotations.ScanHashJoin)},
		{"hash join, E and A bound, extent scan", false, nil, false, open, func() executor.Relation {
			tuples := []executor.Tuple{{alice, wide}, {alice, name}}
			for i := 0; i < 1100; i++ {
				tuples = append(tuples, executor.Tuple{datalog.NewIdentity(fmt.Sprintf("probe-other-%d", i)), wide})
			}
			tuples = append(tuples, executor.Tuple{bob, wide})
			return ea(tuples...)
		}, []string{"alice"}, string(annotations.ScanHashJoin)},
		{"cache, A varying", true, nil, false, open, func() executor.Relation {
			return ea(executor.Tuple{alice, wide}, executor.Tuple{alice, name}, executor.Tuple{bob, wide})
		}, []string{"alice"}, "cache"},
		{"per-key seek, E and A bound", false, &hash, false, open, func() executor.Relation {
			return ea(executor.Tuple{alice, wide}, executor.Tuple{bob, wide}, executor.Tuple{alice, name})
		}, []string{"alice"}, string(annotations.ScanPerKeySeek)},
		{"merge join, E and A bound", false, &merge, false, open, func() executor.Relation {
			return ea(executor.Tuple{alice, wide}, executor.Tuple{bob, wide}, executor.Tuple{alice, name})
		}, []string{"alice"}, string(annotations.ScanMergeJoin)},
		{"hash join, A alone, one key", false, &hash, false, open, func() executor.Relation { return aOnly(executor.Tuple{wide}) }, nil, string(annotations.ScanHashJoin)},
		{"merge join, A alone, one key", false, &merge, false, open, func() executor.Relation { return aOnly(executor.Tuple{wide}) }, nil, string(annotations.ScanMergeJoin)},
		{"per-key seek, A alone", false, &hash, false, open, func() executor.Relation {
			return aOnly(executor.Tuple{wide}, executor.Tuple{name})
		}, []string{"alice", "bob"}, string(annotations.ScanPerKeySeek)},
		{"hash join, E written, A bound", false, &hash, false, onAlice, func() executor.Relation { return aOnly(executor.Tuple{wide}) }, nil, string(annotations.ScanHashJoin)},
		{"cache on, E written, A bound", true, nil, false, onAlice, func() executor.Relation { return aOnly(executor.Tuple{wide}) }, nil, ""},
		{"per-binding, E and A bound", false, nil, true, open, func() executor.Relation {
			return ea(executor.Tuple{alice, wide}, executor.Tuple{bob, wide}, executor.Tuple{alice, name})
		}, []string{"alice"}, string(annotations.ScanPerBinding)},
		{"per-binding, E written, A bound", false, nil, true, onAlice, func() executor.Relation { return aOnly(executor.Tuple{wide}) }, nil, string(annotations.ScanPerBinding)},
	}

	for _, mode := range optimizerModes {
		for _, r := range routes {
			t.Run(mode.name+"/"+r.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: !r.cache})
				tx := db.NewTransaction()
				tx.Add(alice, name, "alice")
				tx.Add(alice, shortKw, "alice-short")
				tx.Add(bob, name, "bob")
				tx.Add(bob, shortKw, "bob-short")
				if _, err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				m := db.Matcher().(*PatternMatcher)
				var seen []string
				m.handler = func(e annotations.Event) {
					switch e.Name {
					case annotations.StorageScanComplete:
						if s, ok := e.Data[annotations.KeyStrategy]; ok {
							seen = append(seen, toString(s))
						}
					case annotations.StorageResolveComplete:
						seen = append(seen, "cache")
					}
				}
				if r.force != nil {
					m.ForceJoinStrategy(r.force)
				}
				var rel executor.Relation
				var err error
				if r.perBind {
					rel, err = m.matchWithoutIteratorReuse(r.pattern, r.bindings(), r.pattern.Symbols(), nil)
				} else {
					rel, err = m.MatchWithConstraints(query.PatternQuery(r.pattern), executor.Relations{r.bindings()}, nil)
				}
				tuples, err := executor.CollectTuples(rel, err)
				if err != nil {
					t.Fatalf("match: %v", err)
				}
				vIdx := query.SymbolIndex(r.pattern.Symbols(), vSym)
				var got []string
				for _, tuple := range tuples {
					got = append(got, toString(tuple[vIdx]))
				}
				sort.Strings(got)
				t.Logf("routes seen %v; answered %v", seen, got)
				if r.wantRoute != "" {
					found := false
					for _, s := range seen {
						if s == r.wantRoute {
							found = true
						}
					}
					if !found {
						t.Errorf("route %s not taken; seen %v", r.wantRoute, seen)
					}
				}
				if len(got) != len(r.want) {
					t.Fatalf("answered %v, want %v", got, r.want)
				}
				for i := range got {
					if got[i] != r.want[i] {
						t.Fatalf("answered %v, want %v", got, r.want)
					}
				}
			})
		}
	}
}

func toString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case annotations.ScanStrategy:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}
